"""Exceptions a step can raise to tell the engine what to do with the run."""

from __future__ import annotations

from datetime import datetime


class Retryable(Exception):
    """Transient failure (network, 5xx, 529): the engine retries the step with growing backoff."""


class Pause(Exception):
    """Wait until a known time (e.g. the API quota reset).

    The run goes to `waiting` with `wake_at` and the process moves on; `ratchet tick` resumes it.
    """

    def __init__(self, until: datetime, reason: str = ""):
        super().__init__(reason or f"paused until {until.isoformat()}")
        self.until = until
        self.reason = reason


class NeedsHuman(Exception):
    """Only a person can unblock the run (budget used up, a decision to approve...).

    The run waits for `event`, sent with `ratchet signal <run> <event>`.
    """

    def __init__(self, event: str, reason: str = ""):
        super().__init__(reason or f"waiting for event {event!r}")
        self.event = event
        self.reason = reason


class Fatal(Exception):
    """A failure retrying won't fix: the run ends as `failed` without using up its retries."""


class StepFailed(Exception):
    """A step failed for good (retries exhausted or `Fatal`)."""


class NonDeterminismError(Exception):
    """On replay, step N is not called the same as the first time.

    Happens when the flow depends on something outside the steps (clock, randomness, files) or
    when the workflow code changed while the run was in flight.
    """


class _Suspend(BaseException):
    """Internal: stops the workflow without going through the user's `except Exception` blocks."""
