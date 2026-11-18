# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/); before 1.0 the API may change in minor versions.

## [Unreleased]

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
