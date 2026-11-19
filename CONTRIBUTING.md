# Contributing

Thanks for taking a look. ratchetflow is small on purpose; these rules keep it that way.

## Setup

```bash
uv sync
uv run pytest -q
```

## Ground rules

- **No dependencies in the core** (`src/ratchetflow/`). An adapter that needs a library goes in an
  optional extra.
- **Tests spend no tokens.** The Claude adapter is tested against a fake CLI
  (`tests/test_claude_agent.py`). Anything that depends on time goes through the `Clock` in
  `tests/conftest.py`, so tests never sleep for real waits.
- **A new test must fail first.** Break the code on purpose, watch the test fail, then restore
  it. A test that never failed proves nothing.
- **English everywhere**: code, comments, messages, tests and docs.
- **Examples stay generic**: no company, team or product names.

## Map

| File | What |
|---|---|
| `src/ratchetflow/engine.py` | `Engine` (registration, start/run/tick/signal/retry, lease with heartbeat) and `Context` (step, sleep, wait_for_event, budget, scratch) |
| `src/ratchetflow/store.py` | SQLite schema (runs, steps, events) and every query; step writes are fenced by the lease owner |
| `src/ratchetflow/errors.py` | `Retryable`, `Fatal`, `Pause`, `NeedsHuman` and the engine's errors |
| `src/ratchetflow/agents/claude.py` | Step that runs `claude -p` with session resume, quota, timeout, validation and cost estimates |
| `src/ratchetflow/agents/codex.py` | Step that runs `codex exec --json` with thread resume, usage limits, validation and tokens |
| `src/ratchetflow/agents/anthropic_api.py` | Step that runs a Messages API conversation with your tools (extra `anthropic`), stored turn by turn |
| `src/ratchetflow/agents/_common.py` | What the adapters share: the child process and its JSON lines, prices, validation |
| `src/ratchetflow/cli.py` | `ratchet runs/show/inbox/signal/export/retry/tick/start/systemd` |
| `examples/demo.py` | Token-free flow with a question for a human |

## Ideas on the table

- More adapters: the Claude Agent SDK.
