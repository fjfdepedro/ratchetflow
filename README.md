# ratchetflow

[![CI](https://github.com/fjfdepedro/ratchetflow/actions/workflows/ci.yml/badge.svg)](https://github.com/fjfdepedro/ratchetflow/actions/workflows/ci.yml)

Durable steps for long-running agent workflows. Like a ratchet, it only moves forward, and if it
slips it goes back one tooth at most. **If the process dies, you lose the step in flight, nothing
more.**

A Python library with no dependencies, built on SQLite. No server: the state lives in one file,
and a `ratchet tick` (cron or systemd) resumes whatever is due.

> **Status: alpha.** It works and is tested, including against the real `claude` CLI, but the
> API may still change before 1.0. Feedback and issues are welcome.

Requires Python 3.11+ on Linux or macOS (the agent adapter relies on POSIX process groups).

```bash
# not on PyPI yet; from a clone:
uv pip install -e .        # import ratchetflow; the CLI is called `ratchet`
```

## In 30 lines

```python
from ratchetflow import Engine, Fatal, NeedsHuman
from ratchetflow.agents import claude

engine = Engine("~/.local/state/ratchet/runs.db")

@engine.workflow("write-article")
def write_article(ctx, brief):
    folder = brief["folder"]
    outline = ctx.step("outline", claude, ctx, f"Outline an article on {brief['topic']}. Return JSON.",
                       cwd=folder, validate=check_outline, timeout_s=900)

    def approval():
        answer = ctx.scratch.get("signal")         # the human's answer, if there was one
        if answer is None:
            raise NeedsHuman("outline.ok", "approve the outline?", details=outline["data"])
        return answer

    ctx.step("approval", approval)
    ctx.step("sources", fetch_sources, outline["data"]["links"], retries=5)
    ctx.step("draft", claude, ctx, "Write the article from the outline.", cwd=folder, timeout_s=3600)

    for i in range(3):                             # stable names: edit-0, edit-1...
        r = ctx.step(f"edit-{i}", claude, ctx, "Edit the draft.", cwd=folder, validate=check_edit)
        if not r["data"]["issues"]:
            break

    ctx.wait_for_event("sign-off", "editor.signed_off", timeout_s=24 * 3600)
    return ctx.step("publish", publish_unless_published, folder)   # idempotent: checks first
```

```bash
ratchet --app flows.py:engine start write-article '{"topic": "...", "folder": "..."}' --id article-42 --budget 10
ratchet --app flows.py:engine runs                # state of every run
ratchet --app flows.py:engine show article-42     # steps, attempts, cost and duration
ratchet --app flows.py:engine inbox               # runs waiting for you, and how to answer
ratchet --app flows.py:engine signal article-42 outline.ok '{"decision": "go on"}'
ratchet --app flows.py:engine signal article-42 ratchet.budget '{"add_usd": 5}'
ratchet --app flows.py:engine tick --loop 60      # or a systemd timer every minute
```

`runs`, `show`, `signal` and `retry` can also take `--db path/to/runs.db` instead of `--app`.
There is a token-free demo in [`examples/demo.py`](examples/demo.py).

## How it works

- A workflow is a plain function. Anything with side effects or that is not deterministic goes
  inside `ctx.step(name, fn, *args)`.
- Every finished step stores its output (JSON) in SQLite. When a run resumes, the workflow runs
  again from the top and the finished steps return their output without running again.
- The output comes back as JSON would (tuples become lists, dict keys become strings), on the
  first run and on every replay alike.
- If on replay step N is not called the same as the first time, the run fails with
  `NonDeterminismError`: something outside the steps changed the flow.
- One process runs a run at a time (a lease with a heartbeat). If the worker dies, the lease
  expires and `tick` picks the run up. A worker that lost its lease can no longer write steps.

| Inside a step you raise... | The engine... |
|---|---|
| `Retryable` or any exception | retries with growing backoff, up to `retries` attempts |
| `Fatal` | fails the run without retrying |
| `Pause(until)` | parks the run until that time; the process moves on |
| `NeedsHuman(event, reason, details=..., questions=[...])` | parks the run until `ratchet signal <run> <event>`; the payload lands in `ctx.scratch["signal"]` |

An attempt that kills the whole process (OOM, segfault) counts too, so a step that brings the
worker down fails after `retries` attempts instead of looping forever. A step that failed and
was caught by the workflow raises `StepFailed` again on replay; it does not run again.

Also: `ctx.sleep(name, s)`, `ctx.wait_for_event(name, event, timeout_s)`,
`ctx.record_cost(usd)` (counts towards the run's `--budget`: once over it, the run waits for
`ratchet.budget`) and `ctx.save_scratch(...)` for a step's intermediate state.

Starting a run again with the same `--id` neither duplicates it nor wakes it before its time.

`ratchet inbox` lists the runs waiting for a person: the whole question, its `details` (whatever
JSON the step passed so you can decide without digging), its open `questions`, numbered and
apart (for example the doubts an agent left in its report, which would otherwise sit unseen in
`details`), and the exact `ratchet signal` to answer.
Budget top-ups show up there too; runs waiting for an external event, such as CI, do not. Add
`--json` to feed it to a notifier.

## The Claude Code adapter

`ratchetflow.agents.claude` runs `claude -p --output-format stream-json` as a step:

- stores the `session_id` as soon as the CLI announces it; if the process dies, the retry runs
  `--resume` instead of starting over;
- quota used up ("resets 1:30pm") -> `Pause` until the reset plus two minutes;
- overload or 5xx -> retry; timeout -> kills the whole process group (no zombie agents);
- SIGTERM or SIGHUP to `ratchet tick`/`start` also kills the agent and releases the lease;
- extracts the last ```` ```json ```` block and passes it through `validate`; if it does not
  validate, it retries in the same session asking for a fix;
- caps the session with `--max-budget-usd` at what is left of the run's budget; if the session
  runs out, the run waits for `ratchet.budget` and then resumes the same session instead of
  retrying and spending again. The CLI checks the cap between turns, so a session can go past
  it by up to one turn: a first turn with Sonnet already costs a few cents (0.09 $ with a
  0.02 $ cap in one measurement);
- with `expect_json=False`, `validate` gets the answer's text, for agents whose real output is
  the files they write (check them in `validate` and raise to retry in the same session);
- adds `total_cost_usd` to the step's cost. An attempt cut off before the CLI reports its cost
  (timeout, kill, crash) is billed from the token usage streamed so far, at the prices in
  `ratchetflow.agents.claude.PRICES` (override with `prices=`). It is a lower bound: the stream
  under-reports output tokens.

## What ratchetflow does not do for you

- **Idempotent external effects.** If the process dies after publishing and before storing the
  step, the step runs again. A step that touches the outside world checks first whether it
  already did it.
- **Workflow versioning.** If you change the code with runs in flight, it warns (code hash) and
  fails if the step names no longer match. Nothing more.
- **A hard budget.** The budget is checked before each step and passed to the Claude session as
  a cap that the CLI enforces between turns, so a run can end up one turn over it. A step that
  spends some other way can go past it too.
- **Spreading work across machines.** It is SQLite: one host, several processes.

## Development

```bash
uv sync && uv run pytest -q
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the ground rules and [CHANGELOG.md](CHANGELOG.md) for
what changed.

## License

MIT
