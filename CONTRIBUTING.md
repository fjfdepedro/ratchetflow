# Contributing

Thanks for taking a look. ratchetflow is small on purpose; these rules keep it that way.

## Setup

```bash
uv sync --group agent-sdk              # without it, the Agent SDK tests are skipped
uv run pytest -q
uv run ruff check src tests examples   # CI fails on any finding
uv run mypy                            # and on any type error
uv run pytest -q --cov                 # and under 90% branch coverage
```

The Agent SDK is about 350 MB. Without it, everything else works, but coverage drops below the
floor.

What each guarantee rests on is in [docs/design.md](docs/design.md). A change that alters one
updates that page too.

## Ground rules

- **No dependencies in the core** (`src/ratchetflow/`). An adapter that needs a library goes in an
  optional extra.
- **Tests spend no tokens.** Each adapter is tested against a fake: a fake `claude` and
  `codex` CLI, a fake API client, a fake SDK `query`. When a real session shows the fake was
  wrong, the fake is fixed to match it. Anything that depends on time goes through the `Clock` in
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
| `src/ratchetflow/agents/claude_sdk.py` | Step that runs a Claude Agent SDK session (extra `agent-sdk`) with the CLI adapter's guarantees |
| `src/ratchetflow/agents/codex.py` | Step that runs `codex exec --json` with thread resume, usage limits, validation and tokens |
| `src/ratchetflow/agents/anthropic_api.py` | Step that runs a Messages API conversation with your tools (extra `anthropic`), stored turn by turn |
| `src/ratchetflow/agents/_common.py` | What the adapters share: the child process and its JSON lines, prices, validation |
| `src/ratchetflow/cli.py` | `ratchet runs/show/inbox/signal/export/retry/tick/start/systemd` |
| `examples/demo.py` | Token-free flow with a question for a human |
| `examples/article.py` | A real flow: agents, two human gates, parallel reviews, an idempotent publish |
| `docs/design.md` | The guarantees: what a crash costs at each point, leases, waits, budget |
| `.github/workflows/ci.yml` | Tests (3.11-3.13 Linux, 3.13 macOS), lint and types, coverage, the demo, the wheel |
| `.github/workflows/publish.yml` | A GitHub release `vX.Y.Z` runs the tests and uploads to PyPI |

## Releasing

1. In a PR: set the version in `pyproject.toml` and `src/ratchetflow/__init__.py`, run
   `uv lock`, and turn `[Unreleased]` in the changelog into `[X.Y.Z] - date`, with its compare
   link.
2. Once it is merged, create the GitHub release `vX.Y.Z` from `main`, with that changelog
   section as its notes. `publish.yml` checks that the tag matches the version, runs the tests,
   and uploads to PyPI through trusted publishing. Nothing is uploaded by hand.
