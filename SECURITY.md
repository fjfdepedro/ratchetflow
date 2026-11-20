# Security

## Supported versions

Only the latest release gets fixes. ratchetflow is alpha (0.x), so upgrading within 0.x may
need small changes; the [changelog](CHANGELOG.md) says which.

## Reporting a vulnerability

Please do not open a public issue. Use GitHub's private reporting instead:
[Report a vulnerability](https://github.com/fjfdepedro/ratchetflow/security/advisories/new).
Expect a first answer within a week.

## What is in scope

- The engine and store (`src/ratchetflow/`): for example, a way for one run to read or write
  another run's steps, or for a worker that lost its lease to write a step.
- The agent adapters: for example, an argument or a prompt that reaches a shell it should not.

## What ratchetflow does not protect against

- **Anyone who can write the database file.** The state is a plain SQLite file. Whoever can
  write it can change runs, outputs and signals. Keep it where only the workers' user can
  write it.
- **What your agents do.** An adapter runs the CLI or SDK with the tools and permissions you
  give it. Limit them with the agent's own options (`--allowedTools`, `sandbox`, and so on).
- **What your steps do.** A step is your code, run with your process's rights.
