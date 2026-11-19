# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/); before 1.0 the API may change in minor versions.

## [Unreleased]

### Added

- `ratchet inbox` (and `--json`): runs waiting for a person, with the whole question, its
  details and the exact `ratchet signal` to answer. Budget top-ups are listed too.
- `NeedsHuman(event, reason, details=...)`: JSON context shown in the inbox.
- `ctx.now(name)` and `ctx.random(name)`: the time and a random number, read once per run and
  returned unchanged on every replay.
- `ratchet export [run ...] [--status S]`: every step as one JSON line (cost, working and waiting
  time, attempts, error, and for agent steps the models, turns and tokens), for metrics.
- The Claude adapter returns `usage` (tokens of all the step's attempts) and `models`.
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

[Unreleased]: https://github.com/fjfdepedro/ratchetflow/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/fjfdepedro/ratchetflow/releases/tag/v0.1.0
