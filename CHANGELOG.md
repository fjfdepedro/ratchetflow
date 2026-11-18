# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/); before 1.0 the API may change in minor versions.

## [Unreleased]

### Added

- `ratchet inbox` (and `--json`): runs waiting for a person, with the whole question, its
  details and the exact `ratchet signal` to answer. Budget top-ups are listed too.
- `NeedsHuman(event, reason, details=...)`: JSON context shown in the inbox.
- `ctx.budget_left()`.
- `claude(..., expect_json=False)`: `validate` receives the answer's text, to check the files an
  agent wrote.
- `claude(..., max_budget_usd=...)`: caps the session; by default, what is left of the run's budget.

### Fixed

- A Claude session that ran out of `--max-budget-usd` was retried, spending again. The run now
  waits for `ratchet.budget` and resumes the same session.
- `ratchet show` cut a waiting run's question to its first line.

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
