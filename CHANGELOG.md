# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/); before 1.0 the API may change in minor versions.

## [Unreleased]

### Added

- `docs/design.md`: the guarantees. What a crash at each point of a step costs, leases and
  fencing, waits and events, the soft budget, the determinism checks, and why SQLite.
- `examples/article.py`: a real flow. Agents with validators, two human gates with details and
  open questions, parallel reviews, `ctx.now` for the file name, and an idempotent publish.
- `SECURITY.md`, with private vulnerability reporting, and issue forms for bugs and ideas.

### Changed

- CI lints with ruff, type-checks `src` with mypy and measures branch coverage, failing under
  90%. The code moved to `X | None` and `collections.abc` imports; no behaviour changed.

### Fixed

- The README still said the package was not on PyPI.
- A test of SIGTERM in `ctx.parallel` failed now and then on a busy runner.

## [0.2.0] - 2026-11-20

### Added

- `ratchet inbox` (and `--json`): runs waiting for a person, with the whole question, its
  details and the exact `ratchet signal` to answer. Budget top-ups are listed too.
- `NeedsHuman(event, reason, details=...)`: JSON context shown in the inbox.
- `ratchet systemd [--every 1min] [--name N] [--install]`: a systemd user service and timer
  that run `tick` again after each one ends.
- `ctx.parallel(ctx.call(name, fn, ...), ...)`: steps that run at once, each in its own thread,
  with outputs in order. SIGTERM stops every branch's agent; `ctx.cancelled()` tells other long
  steps.
- `ctx.now(name)` and `ctx.random(name)`: the time and a random number, read once per run and
  returned unchanged on every replay.
- `ratchet export [run ...] [--status S]`: every step as one JSON line (cost, working and waiting
  time, attempts, error, and for agent steps the models, turns and tokens), for metrics.
- The Claude adapter returns `usage` (tokens of all the step's attempts) and `models`.
- `ratchetflow.agents.codex`: a step that runs `codex exec --json`, resumes its thread after a
  crash or an invalid answer, pauses until a usage limit resets and records tokens (and cost,
  with `prices=`).
- `ratchetflow.agents.anthropic_api` (extra `ratchetflow[anthropic]`): a Messages API
  conversation with your own tools as one step, stored after every answer and every round of
  tool results, billed per answer and checked against the budget before every call. Not yet
  run against the real API: its tests use a fake client.
- `ratchetflow.agents.claude_sdk` (extra `ratchetflow[agent-sdk]`): a Claude Agent SDK session
  as a step, resumed by session id, paused on a rejected rate limit until its reset, and capped
  at the budget left.
- `NeedsHuman(..., questions=[...])`: open points to settle along with the decision, listed
  numbered and apart in `ratchet inbox` (and as `questions` in `--json`).
- `ctx.budget_left()`.
- `claude(..., expect_json=False)`: `validate` receives the answer's text, to check the files an
  agent wrote.
- `claude(..., max_budget_usd=...)`: caps the session; by default, what is left of the run's budget.
- Steps record their working time (`busy_s`), apart from the time they spent waiting for a
  person, a pause or a retry. `ratchet show` prints both: `0s (+37m waiting)`. Databases from
  0.1.0 gain the column on open; their old steps show wall time as before.

### Fixed

- The sdist is built from a closed list (package, tests, demo and docs). Before, a build from a
  local checkout took in any untracked file next to the code; CI now fails if anything else
  slips in. It also stops shipping the CI workflows and `uv.lock`.
- A Claude session that ran out of `--max-budget-usd` was retried, spending again. The run now
  waits for `ratchet.budget` and resumes the same session.
- `ratchet show` cut a waiting run's question to its first line.
- `ratchet show` reported the time a step waited for a person as the step's duration.

## [0.1.0] - 2026-11-18

First release. Install `ratchetflow`, import `ratchetflow`; the CLI is `ratchet`.

### Added

- Durable workflow engine on SQLite: `ctx.step`, `ctx.sleep`, `ctx.wait_for_event`, per-run
  USD budget, step scratch state, and a lease with heartbeat so one process runs a run at a time.
- Step errors that steer the run: `Retryable`, `Fatal`, `Pause(until)`, `NeedsHuman(event)`.
- Replay safety: `NonDeterminismError` when step names change, outputs normalised to JSON so a
  replay sees the same values, caught step failures replayed instead of re-run.
- Crash safety: attempts counted before they run, step writes fenced by the lease owner,
  SIGTERM/SIGHUP handled by the CLI.
- `claude -p` adapter: session resume, quota pause until the reset, overload retries, process
  group kill on timeout, JSON extraction and validation, cost tracking, and cost estimates for
  attempts that are cut off.
- `ratchet` CLI: `start`, `tick`, `runs`, `show`, `signal`, `retry`.
- CI on GitHub Actions for Python 3.11–3.13.

[Unreleased]: https://github.com/fjfdepedro/ratchetflow/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/fjfdepedro/ratchetflow/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/fjfdepedro/ratchetflow/releases/tag/v0.1.0
