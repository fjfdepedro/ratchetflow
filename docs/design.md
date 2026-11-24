# Design and guarantees

This is what ratchetflow promises when things go wrong, and why it is built the way it is. The
README says how to use it; this page says what you can rely on.

The short version: **a crash costs the step in flight, never a finished one.** A finished step
never runs again. The step that was running when the process died runs again, as a new attempt.
External effects inside that step may happen twice, so make them safe to repeat.

## The model: replay over stored steps

A workflow is a plain Python function. Each `ctx.step(name, fn, ...)` call is a unit of work
whose output is stored in SQLite as JSON once it finishes. To resume a run, the engine calls
the function again from the top. Every step that already finished returns its stored output
without running, so execution fast-forwards to the first step that has not finished.

This is the replay model used by durable-execution systems, reduced to one table of steps. It
was chosen over the alternatives for a few reasons:

- **The workflow stays plain code.** No DSL, graph or state machine. Loops, `if`s and helper
  functions work as usual.
- **No server.** The state is one file. A cron job or a systemd timer running `ratchet tick` is
  the whole scheduler.
- **Long waits cost nothing.** A run waiting three days for a person is a row in a table, not a
  sleeping process.

The price is the usual one for replay: **the workflow body must make the same calls in the same
order every time it runs.** Anything that could differ between replays (the clock, random
numbers, a file on disk, a network call) goes inside a step, or through `ctx.now(name)` and
`ctx.random(name)`, which are read once and stored.

## What is stored

Three tables, in [`store.py`](../src/ratchetflow/store.py):

| Table | One row per | Holds |
|---|---|---|
| `runs` | run | status, input, output, lease, wait condition, budget |
| `steps` | step of a run | status, attempts, output, scratch, cost, timing |
| `events` | signal sent to a run | name, payload, the step that consumed it |

A step is identified by its position in the run (`seq`) and its name, and both must match on
replay. A step's **scratch** is a small JSON dict it can write to disk at any moment through
`ctx.save_scratch(...)`. The agent adapters keep their session id there, so a retry can resume
the conversation instead of starting over.

## A step, and what a crash at each point costs

`ctx.step` goes through these points, in order. The table says what happens if the process dies
right after each one.

| The process dies after... | On resume | Repeated |
|---|---|---|
| the step row is created | the step starts | nothing |
| the attempt is counted | a new attempt starts | nothing |
| `fn` started, mid-way | a new attempt starts, with the scratch so far | the work since the last scratch write |
| `fn` returned, before the output is stored | a new attempt starts | the whole step |
| the output is stored | the step returns its output | nothing |

Three things follow from this:

- **The attempt is counted before `fn` runs.** A step that kills its own process (out of
  memory, a segfault, a child that takes the worker down) uses up its `retries` and fails. It
  cannot loop forever.
- **The window between "`fn` returned" and "stored" is real, though small.** A step that
  publishes, pays or pushes should check first whether it already did, or carry an idempotency
  key in its scratch.
- **Scratch writes are the step's own checkpoints.** The finer the step writes them, the less
  a crash repeats. The Claude adapter writes the session id the moment the CLI announces it, so
  a crash after that costs the turn in flight, not the session.

How a step ends:

| `fn` raises | The step | Attempts |
|---|---|---|
| nothing | is stored as done | used |
| `Retryable` or any exception | is retried with exponential backoff, until `retries` runs out | used |
| `Fatal` | fails at once | used |
| `Pause(until)` | waits until that time; the run is parked | given back |
| `NeedsHuman(event, ...)` | waits for that signal; the run is parked | given back |

A step that failed, when the workflow caught `StepFailed`, raises `StepFailed` again on every
replay. It does not run again. `ratchet retry <run>` gives a failed run's failed steps all
their attempts back.

## One worker per run: leases and fencing

Several processes may run `tick` against the same database at once, for example a timer and a
person at a terminal. A **lease** makes sure that only one of them runs a given run at a time:

- To run a run, a process sets `owner` and `lease_until` on its row. This only succeeds if the
  run has no live lease, or the lease is already its own. It happens in one `UPDATE`, inside a
  write transaction.
- While the run executes, a heartbeat thread renews the lease every `heartbeat_s` (60 s by
  default), for `lease_ttl_s` more (300 s by default).
- If the worker dies, nothing renews the lease. Once it expires, the next `tick` sees a
  `pending` or `running` run with an expired lease and takes it over.

A lease alone is not enough. A worker that freezes for longer than the TTL (a suspended laptop,
a stopped container) wakes up believing it still owns the run. That is why **every step write
is fenced**: the `UPDATE` only applies while `runs.owner` is still this worker. The first write
that finds another owner raises internally, and the old worker stops executing that run.
Finishing the run, as completed, waiting, failed or cancelled, is fenced the same way.

Two writes are deliberately not fenced: **a step's cost and its busy time**. They describe what
already happened. Money a stale worker spent was spent, and it counts towards the budget.

What fencing cannot stop: a stale worker that is inside `fn` when it loses the lease runs `fn`
to its end. Its external effects happen, even though its result will not be stored. This is the
same window as a crash, and the advice is the same: make external effects safe to repeat.

## Waiting

A run that waits releases its lease and stores **why** it is waiting:

- `wake_at`, a time, for `ctx.sleep`, for `Pause`, and for the timeout of `ctx.wait_for_event`;
- `wait_event`, a signal name, for `ctx.wait_for_event`, for `NeedsHuman` and for a budget
  that ran out.

`tick` resumes exactly the runs that are due: the time has come, an unconsumed event with that
name exists, or the worker died. Nothing polls in between, and no process stays alive.

**Events are consumed once.** Each event row records which step consumed it (`consumed_seq`).
On replay, a `wait_for_event` that already got its event gets the same one back, not the next.
A signal sent before the run starts waiting is kept until a step asks for it. Budget top-ups
(`ratchet.budget`) are added to the run's budget as soon as the engine checks it, and marked
as consumed in the same transaction.

`ctx.sleep` stores its wake-up time on its first run, so a replay a day later does not start
the sleep over.

## The budget is soft

Each run can have a budget in USD. Steps add their cost to it through `ctx.record_cost`, which
the agent adapters call for you. The engine checks the budget **before each step**. Once the
spending reaches it, the run waits for `ratchet.budget`, and a signal such as
`{"add_usd": 5}` resumes it from the same step.

The check happens between steps, not during them, so a run can go over:

- The Claude adapters pass the remaining budget to the session as its own cap, which the CLI
  enforces **between turns**. A session can end one turn over.
- `ctx.parallel` checks once before starting its branches. Branches running together can each
  go one turn over.
- A step that spends without reporting it does not count.

When a session is cut off before it reports its cost (timeout, kill, crash), the adapter
estimates the cost from the token usage streamed so far, and bills that estimate. The estimate
is billed first and cleared after. If the process dies in between, the attempt is billed twice:
**for a budget, too much is safer than too little.**

## Determinism checks

Replay only works if the workflow makes the same calls in the same order. The engine checks
what it can:

- **Step names are unique within a run**, and step N must have the same name on every replay.
  If it does not, the run fails with `NonDeterminismError`, rather than handing the wrong
  stored output to the wrong step.
- **Outputs come back as JSON**, on the first run as on every replay. A tuple becomes a list,
  dict keys become strings. Code never sees a value on the first run that it would not see on
  a replay. A step that returns something that is not JSON fails the run at once.
- **The workflow's source is hashed** when the run starts. If the code changed while the run
  was in flight, the engine logs a warning. If the step names no longer match, the run fails.
  There is no versioning beyond that.
- **Steps do not nest.** A step that calls `ctx.step` inside itself is rejected. The inner
  step's output would be stored inside an attempt that may itself be repeated.

## Parallel steps

`ctx.parallel(ctx.call(...), ...)` runs several steps at once, each in its own thread. Each
branch is an ordinary step, with its own name, attempts, cost and scratch. Step numbers are
assigned in order before any branch starts, so replays line up however the threads finish.

Every branch runs to its end before `parallel` returns. The outcome is decided in this order:

1. a branch that lost the lease stops the run;
2. then the first failure raises `StepFailed`;
3. then the first branch that waits parks the run.

On resume, the finished branches return their stored outputs, and only the others run again.

Signals reach only the main thread. On SIGTERM or Ctrl-C, the engine marks the run as cancelled
and gives the branches up to 60 s to stop. The Claude adapter checks that mark, and kills its
agent's whole process group.

## Ending without completing: cancel, deadline, cleanup

A run has to stop when someone asks (`ratchet cancel`, which stores a `cancel_reason`) or when
its `deadline` passes (`timeout_s`). Both are checked in three places:

- **Before the run executes.** `tick` treats a waiting run that has to stop as due, so a run
  parked for a person or an event ends without waiting for them.
- **Before each step**, when the step is entered. A plain step already running finishes first,
  and its output is stored.
- **At every heartbeat**, which sets `ctx.cancelled()`. The agent adapters poll it, kill the
  agent's process group and stop the step. A cancel therefore reaches a running agent within
  `heartbeat_s`.

A run that fails, or has to stop, then **ends**:

1. The ending (final status, reason, and the number of its cleanup step) is written to the run,
   fenced by the lease like any step write.
2. The workflow's `cleanup` runs as a step. Cleanup steps take negative numbers (-1, -2...), so
   they never collide with the workflow's own steps: a failed run that is retried replays its
   steps 1..N and goes on, as before.
3. The run gets its final status, and the ending is cleared.

If the process dies between 1 and 3, the next `tick` finds the ending and goes straight to the
cleanup. The workflow does not run again. A cleanup can wait (`Pause`, `NeedsHuman`); the run
stays waiting, still ending, until it is resumed. Like any step, a cleanup can run more than
once, so it must be safe to repeat.

Cancelling is the only way to `cancelled`, and it is final: `retry` reopens failed runs only.

## Child runs

`ctx.invoke(name, workflow, input)` inserts a run with the id `<parent>.<name>` and a
`parent_id`. The insert is idempotent by id, so a parent that crashes after it and replays finds
the same child. The parent's step stores the child's id in scratch, and the parent waits for the
event `ratchet.child:<child id>`.

When a child reaches a final status, the engine sends that event to its parent in the same
process, right after the child's final write. If the child finished before the parent started
waiting, the event is already there and the parent is due at once. On resume, the parent's step
reads the child's status, not the event's payload: the child's row is the source of truth.

`tick` keeps going while new runs become due, so a child and then its parent can both finish in
one tick. Each run executes at most once per tick, which keeps a tick finite.

Cancelling a run cancels its unfinished children, recursively. So does a run that ends any other
way without completing, before its own cleanup runs.

## Why SQLite

- **No server to run, back up or upgrade.** The state of every run is one file.
- **WAL mode**, so `ratchet show` and `ratchet inbox` can read while a worker writes.
- **`BEGIN IMMEDIATE`**: a transaction takes the write lock up front, so two workers trying to
  take the same lease cannot both read "free" and then both write. With `busy_timeout` at 5 s,
  a writer waits for the lock instead of failing.
- **Heartbeats survive a busy database.** A heartbeat that cannot get the lock tries again on
  the next beat, without giving up the lease.

The limit is the one SQLite has: **one host, several processes**. To spread runs across
machines you need a database server, and that is out of scope.

## Not covered

These are deliberately left to you. The README lists them too:

- **Making external effects idempotent.** See the crash table above.
- **Workflow versioning**, beyond the code-hash warning and the name check.
- **A hard budget cap.**
- **Running across several machines.**
