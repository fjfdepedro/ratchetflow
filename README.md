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
ratchet --app flows.py:engine tick --loop 60      # or a systemd timer (below)
ratchet --app flows.py:engine export > steps.jsonl  # one line per step: cost, time, tokens
```

`runs`, `show`, `signal` and `retry` can also take `--db path/to/runs.db` instead of `--app`.
There is a token-free demo in [`examples/demo.py`](examples/demo.py).

### Resume runs without a terminal open (Linux)

`ratchet systemd` writes a user timer that runs `tick` again a minute after the last one ends,
so ticks never overlap:

```bash
ratchet --app flows.py:engine systemd                 # prints the .service and .timer
ratchet --app flows.py:engine systemd --install       # writes them to ~/.config/systemd/user
systemctl --user daemon-reload && systemctl --user enable --now ratchet-flows.timer
loginctl enable-linger                                # keep it running after you log out
journalctl --user -u ratchet-flows.service            # what each tick did
```

The unit uses absolute paths, the current Python and the current `PATH` (so it finds `claude`).
`--every 5min` changes the pause, `--name` the unit. Run it from the folder the app expects to
run in: that becomes the unit's working directory. Stopping the unit sends SIGTERM, which
stops the agents and frees the lease.

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
`ctx.now(name)` and `ctx.random(name)` (read once and stored, so a replay the next day gets the
same date: use them instead of `datetime.now()`, `date.today()` or `random` in the workflow body),
`ctx.record_cost(usd)` (counts towards the run's `--budget`: once over it, the run waits for
`ratchet.budget`) and `ctx.save_scratch(...)` for a step's intermediate state.

Steps can run at once, each in its own thread:

```python
style, facts = ctx.parallel(
    ctx.call("review-style", claude, ctx, "Review the style of draft.md."),
    ctx.call("review-facts", claude, ctx, "Check every fact in draft.md.", model="claude-sonnet-5-5"),
)
```

Each branch is an ordinary step (its own name, retries, cost and scratch). Every branch runs to
its end before `parallel` returns or raises: a failed branch raises `StepFailed`, and one that
pauses or needs a person makes the run wait, with the finished branches kept for the replay.
SIGTERM or Ctrl-C stops every branch's agent (`ctx.cancelled()` lets your own long steps notice
too). The budget is checked once before starting, so branches together can overshoot it by one
turn each.

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
- returns the tokens of every attempt (`usage`: input, output, cache read and write) and the
  `models` the session used, which `ratchet export` puts next to each step's cost and time (an
  attempt cut off before the CLI's result is not in `usage`, though its cost is estimated);
- adds `total_cost_usd` to the step's cost. An attempt cut off before the CLI reports its cost
  (timeout, kill, crash) is billed from the token usage streamed so far, at the prices in
  `ratchetflow.agents.claude.PRICES` (override with `prices=`). It is a lower bound: the stream
  under-reports output tokens.

## The Codex CLI adapter

`ratchetflow.agents.codex` runs `codex exec --json` as a step, with the same guarantees:

- stores the thread id as soon as the CLI announces it; if the process dies or the answer does
  not validate, the retry runs `codex exec resume <thread>` asking it to check and finish;
- usage limit reached ("Try again at 3:45 PM") -> `Pause` until then plus two minutes (every 15
  minutes when no time is given); any other failed turn -> retry; timeout -> kills the group;
- `output_schema=` (a JSON Schema) goes to `--output-schema`, and the answer comes back parsed
  in `data`; `sandbox=` goes to `--sandbox`;
- returns the tokens of every attempt in `usage`. Codex reports no dollars, and with a ChatGPT
  sign-in the subscription pays, so **by default the step costs 0 and the budget does not apply**.
  With an API key, pass `prices={"gpt-5": (input, output, cached_input)}` (USD per million tokens,
  by model prefix) to bill it. An attempt cut off mid-turn bills nothing: Codex only reports
  tokens when a turn completes.

```python
from ratchetflow.agents import codex

out = ctx.step("tests", codex, ctx, "Add tests for the parser; answer with a JSON summary",
               cwd=repo, sandbox="workspace-write", validate=check_summary)
```

## The Claude API adapter

`ratchetflow.agents.anthropic_api` talks to the Messages API with your own tools, through the
official SDK (`pip install "ratchetflow[anthropic]"`; the core keeps no dependencies). The
conversation is the step's state:

- it is stored after every answer and every round of tool results, so a crash costs at most the
  call in flight. If the process dies while your tools run, only that round runs again, and the
  answer that asked for them is not paid for twice: **tools that touch the world should be safe
  to run twice**;
- each answer's cost goes to the step as it arrives, at `PRICES` (cache writes included), and the
  budget is checked before every call; when it runs out the run waits for `ratchet.budget` and
  goes on with the same conversation;
- an answer that does not validate goes back in the same conversation, asking for a fix;
- 429 is retried after `retry-after` (a `Pause` past one minute), 5xx, 529 and network errors
  are retried, and a spend limit, a refusal, `max_tokens` or another 4xx fail the run;
- any other keyword (`thinking`, `output_config`, ...) goes to the request as is.

```python
from ratchetflow.agents import anthropic_api

out = ctx.step("research", anthropic_api, ctx, "Find three sources on X; answer in JSON",
               tools=[SEARCH_TOOL], handlers={"search": search}, validate=check_sources)
```

## The Claude Agent SDK adapter

`ratchetflow.agents.claude_sdk` runs a session through the Python Agent SDK
(`pip install "ratchetflow[agent-sdk]"`), for flows that need what only the SDK has: hooks,
`can_use_tool`, your own tools as in-process MCP servers. Pass any `ClaudeAgentOptions`; the step
works on a copy. It keeps the CLI adapter's guarantees:

- stores the session id as soon as a message carries it, and a retry resumes it;
- a rejected rate limit pauses until the reset the SDK reports, and quota text, overload,
  `max_budget_usd` and `max_turns` are handled as in the CLI adapter;
- `max_budget_usd` defaults to what is left of the run's budget; `total_cost_usd` goes to the
  step, and an attempt cut off before its result is billed from the usage seen so far;
- with `output_format`, `validate` gets the `structured_output`.

```python
from claude_agent_sdk import ClaudeAgentOptions
from ratchetflow.agents import claude_sdk

out = ctx.step("review", claude_sdk, ctx, "Review the draft; answer in JSON",
               options=ClaudeAgentOptions(cwd=folder, allowed_tools=["Read"]),
               validate=check_review)
```

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
uv sync --group agent-sdk && uv run pytest -q tests/test_claude_sdk_agent.py  # its own job
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the ground rules and [CHANGELOG.md](CHANGELOG.md) for
what changed.

## License

MIT
