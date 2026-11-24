"""Durable execution engine.

A workflow is a plain Python function that receives a `Context`. Anything with side effects or
that is not deterministic (LLM calls, git, an API) goes inside `ctx.step(...)`; the clock and
random numbers, through `ctx.now(name)` and `ctx.random(name)`.
Every finished step is stored; if the process dies, the workflow runs again from the top and the
finished steps return their stored output without running again.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import os
import random as _random
import socket
import sqlite3
import threading
import time
import traceback
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, NoReturn

from .errors import (
    Fatal,
    NeedsHuman,
    NonDeterminismError,
    Pause,
    StepFailed,
    _Suspend,
)
from .store import FINAL, RunRow, StepRow, Store

BUDGET_EVENT = "ratchet.budget"


@dataclass
class _Suspension:
    wake_at: float | None = None
    event: str | None = None
    reason: str = ""
    lost_lease: bool = False


class _SuspendWith(_Suspend):
    def __init__(self, s: _Suspension):
        super().__init__(s.reason)
        self.suspension = s


def _lost_lease() -> _SuspendWith:
    return _SuspendWith(_Suspension(lost_lease=True, reason="lease lost"))


class _Stop(_Suspend):
    """Internal: the run was cancelled or passed its deadline; stop between steps."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class Ending:
    """What a workflow's `cleanup` gets when a run ends without completing.

    `status` is `failed` or `cancelled` (by hand or by its deadline), and `reason` says why.
    `outputs` holds the output of every finished step, and `scratch` the scratch of every step
    that saved some (an agent's session, the id of a sandbox a step was creating when the run
    stopped), both by step name: enough to find what the run left behind.
    """
    run_id: str
    workflow: str
    input: Any
    status: str
    reason: str
    outputs: dict[str, Any]
    scratch: dict[str, dict]


@dataclass
class Call:
    """One branch of `ctx.parallel`, made with `ctx.call(...)`."""
    name: str
    fn: Callable[..., Any]
    args: tuple
    kwargs: dict
    retries: int
    backoff_s: float
    max_backoff_s: float


@dataclass
class Workflow:
    name: str
    fn: Callable[..., Any]
    cleanup: Callable[[Ending], Any] | None = None
    code_hash: str = field(init=False)

    def __post_init__(self) -> None:
        try:
            src = inspect.getsource(self.fn)
        except (OSError, TypeError):
            src = self.fn.__qualname__
        self.code_hash = hashlib.sha256(src.encode()).hexdigest()[:12]


class Context:
    """What the workflow sees. One `Context` per execution (not per run)."""

    def __init__(self, engine: Engine, run: RunRow):
        self._engine = engine
        self._store = engine.store
        self.run_id = run.id
        self.input = run.input
        self._budget = run.budget_usd
        self._seq = 0
        self._names: set[str] = set()
        self._local = threading.local()   # the step running in each thread (ctx.parallel)
        self._cancel = threading.Event()

    def cancelled(self) -> bool:
        """True once the run is being stopped, so a long step can stop too.

        It turns true when `ratchet cancel` or the run's deadline is noticed by the heartbeat
        while a step runs, and on SIGTERM or Ctrl-C while `ctx.parallel` runs (signals only
        reach the main thread). The agent adapters check it: the agent's process group dies.
        """
        return self._cancel.is_set()

    @property
    def _current(self) -> StepRow | None:
        return getattr(self._local, "row", None)

    @_current.setter
    def _current(self, row: StepRow | None) -> None:
        self._local.row = row

    # --- public API -------------------------------------------------------

    def step(self, name: str, fn: Callable[..., Any], *args: Any, retries: int = 3,
             backoff_s: float = 2.0, max_backoff_s: float = 300.0, **kwargs: Any) -> Any:
        """Runs `fn(*args, **kwargs)` once per run and stores its output (JSON).

        - `Retryable` or any other exception: retried up to `retries` attempts in total. An
          attempt that kills the whole process counts too, so a step that crashes the worker
          does not loop forever.
        - `Fatal`: fails without retrying.
        - `Pause` / `NeedsHuman`: the run waits; on resume the step runs again (with its
          `scratch` intact, so it can pick up where it left off). Waiting costs no attempt.
        - The output comes back as JSON would: tuples become lists, dict keys become strings.
          The first run and every replay see the same value.
        """
        seq, row = self._enter(name)
        if row is not None and row.status == "done":
            return row.output
        if row is not None and row.status == "failed":
            # It already failed and the workflow caught it: replay the failure, not the step.
            raise StepFailed(f"{name}: {row.error}")
        self._check_budget(seq)
        return self._run_step(seq, name, fn, args, kwargs, retries, backoff_s, max_backoff_s)

    def call(self, name: str, fn: Callable[..., Any], *args: Any, retries: int = 3,
             backoff_s: float = 2.0, max_backoff_s: float = 300.0, **kwargs: Any) -> Call:
        """A step to hand to `ctx.parallel`; on its own it does nothing."""
        return Call(name, fn, args, kwargs, retries, backoff_s, max_backoff_s)

    def parallel(self, *calls: Call) -> list[Any]:
        """Runs several steps at once, each in its own thread, and returns their outputs in order.

        Each one is an ordinary step (name, retries, cost, scratch), so `claude(ctx, ...)` works
        inside. Step numbers are taken in order before anything starts, so replays line up.
        Every branch runs to its end before this returns or raises: a branch that fails makes it
        raise `StepFailed`, and one that pauses or needs a person makes the run wait; on resume,
        finished branches return their stored output. The budget is checked once, before
        starting, so branches running together can overshoot it by one turn each.
        """
        if not calls or not all(isinstance(c, Call) for c in calls):
            raise TypeError("ctx.parallel takes ctx.call(...) items")
        slots: list[tuple[int, StepRow | None, Call]] = []
        for c in calls:
            seq, row = self._enter(c.name)
            slots.append((seq, row, c))
        results: list[Any] = [None] * len(calls)
        errors: list[BaseException | None] = [None] * len(calls)
        todo = []
        for i, (_seq, row, c) in enumerate(slots):
            if row is not None and row.status == "done":
                results[i] = row.output
            elif row is not None and row.status == "failed":
                errors[i] = StepFailed(f"{c.name}: {row.error}")
            else:
                todo.append(i)
        if todo:
            self._check_budget(slots[todo[0]][0])

        def branch(i: int) -> None:
            seq, _row, c = slots[i]
            try:
                results[i] = self._run_step(seq, c.name, c.fn, c.args, c.kwargs, c.retries,
                                            c.backoff_s, c.max_backoff_s)
            except BaseException as e:  # noqa: BLE001 — sorted out below, after every branch
                errors[i] = e

        threads = [threading.Thread(target=branch, args=(i,), daemon=True) for i in todo]
        for t in threads:
            t.start()
        try:
            for t in threads:
                t.join()
        except BaseException:
            # SIGTERM or Ctrl-C landed here, in the main thread: tell the branches and give them
            # time to kill their agents before the process goes.
            self._cancel.set()
            for t in threads:
                t.join(timeout=60)
            raise
        # What wins: a lost lease (stop now), then the first failure, then the first wait.
        lost = [e for e in errors if isinstance(e, _SuspendWith) and e.suspension.lost_lease]
        failed = [e for e in errors if e is not None and not isinstance(e, _SuspendWith)]
        waiting = [e for e in errors if isinstance(e, _SuspendWith)]
        if lost or failed or waiting:
            raise (lost or failed or waiting)[0]
        return results

    def _run_step(self, seq: int, name: str, fn: Callable[..., Any], args: tuple, kwargs: dict,
                  retries: int, backoff_s: float, max_backoff_s: float) -> Any:
        row = self._start(seq, name)
        self._absorb_human_signal(row)
        self._current = row
        attempts = row.attempts
        try:
            while True:
                self._engine._raise_if_lease_lost()
                if attempts >= retries:
                    # Only reachable when earlier attempts died with the process.
                    error = row.error or "the process died during the step"
                    self._fail_step(seq, attempts, error)
                    raise StepFailed(f"{name}: {attempts} attempts used up ({error})")
                attempts += 1
                self._write(seq, attempts=attempts)   # counted before running: a crash counts
                began = self._engine.now()
                try:
                    try:
                        out = fn(*args, **kwargs)
                    finally:
                        # Working time only: waits for a person, pauses and backoff stay out.
                        self._store.add_busy(self.run_id, seq, self._engine.now() - began)
                except (Pause, NeedsHuman) as e:
                    self._write(seq, attempts=attempts - 1)
                    self._suspend_from(seq, e)
                except Fatal as e:
                    self._fail_step(seq, attempts, _short(e))
                    raise StepFailed(f"{name}: {e}") from e
                except Exception as e:  # noqa: BLE001 — anything else is retried
                    if attempts >= retries:
                        self._fail_step(seq, attempts, _short(e))
                        raise StepFailed(f"{name}: {e}") from e
                    self._write(seq, error=_short(e))
                    self._engine.log(f"[{self.run_id}] {name}: attempt {attempts}/{retries}"
                                     f" failed ({_short(e)}); retrying")
                    self._engine.sleeper(min(backoff_s * 2 ** (attempts - 1), max_backoff_s))
                    continue
                out = _jsonable(out, name)
                self._write(seq, status="done", output=out, error=None,
                            finished_at=self._engine.now())
                return out
        finally:
            self._current = None

    def sleep(self, name: str, seconds: float) -> None:
        """Durable sleep: the process moves on and `tick` resumes the run when it is time."""
        seq, row = self._enter(name)
        if row is not None and row.status == "done":
            return
        now = self._engine.now()
        row = self._start(seq, name)
        wake_at = row.scratch.get("wake_at")
        if wake_at is None:
            wake_at = now + seconds
            self._write(seq, scratch={"wake_at": wake_at})
        if now >= wake_at:
            self._write(seq, status="done", output=None, finished_at=now)
            return
        self._write(seq, status="waiting")
        raise _SuspendWith(_Suspension(wake_at=wake_at, reason=f"sleep {name}"))

    def now(self, name: str) -> datetime:
        """The time, read once per run: a replay gets the same value, even the next day.

        Use it instead of `datetime.now()` or `date.today()` in the workflow body, where a value
        that changes between replays (a date in a file name, say) breaks the run.
        """
        return datetime.fromtimestamp(self._remember(name, self._engine.now))

    def random(self, name: str) -> float:
        """A random float in [0, 1), drawn once per run and the same on every replay."""
        return self._remember(name, _random.random)

    def _remember(self, name: str, read: Callable[[], Any]) -> Any:
        seq, row = self._enter(name)
        if row is not None and row.status == "done":
            return row.output
        self._start(seq, name)
        value = read()
        self._write(seq, status="done", output=value, finished_at=self._engine.now())
        return value

    def wait_for_event(self, name: str, event: str, timeout_s: float | None = None) -> Any:
        """Waits for `ratchet signal <run> <event>`. Returns the payload, or None on timeout."""
        seq, row = self._enter(name)
        if row is not None and row.status == "done":
            return row.output
        now = self._engine.now()
        row = self._start(seq, name)
        ok, payload = self._store.consume_event(self.run_id, event, seq)
        if ok:
            self._write(seq, status="done", output=payload, finished_at=now)
            return payload
        deadline = row.started_at + timeout_s if timeout_s is not None else None
        if deadline is not None and now >= deadline:
            self._write(seq, status="done", output=None, finished_at=now)
            return None
        self._write(seq, status="waiting")
        raise _SuspendWith(_Suspension(wake_at=deadline, event=event,
                                       reason=f"waiting for {event}"))

    def record_cost(self, usd: float) -> None:
        """Adds cost to the current step. Counts towards the run's budget."""
        if self._current is None:
            raise RuntimeError("record_cost only inside a ctx.step")
        self._store.add_cost(self.run_id, self._current.seq, usd)

    def budget_left(self) -> float | None:
        """USD left in the run's budget (pending top-ups included), or None without a budget."""
        if self._budget is None:
            return None
        self._budget = self._store.absorb_budget_events(self.run_id, BUDGET_EVENT)
        return self._budget - self._store.run_cost(self.run_id)

    @property
    def scratch(self) -> dict:
        """Intermediate state of the current step (e.g. an agent's session_id)."""
        if self._current is None:
            raise RuntimeError("scratch only inside a ctx.step")
        step = self._store.get_step(self.run_id, self._current.seq)
        return dict(step.scratch) if step else {}

    def save_scratch(self, **values: Any) -> None:
        """Writes intermediate state of the current step to disk, right now."""
        data = self.scratch
        data.update(values)
        assert self._current is not None
        self._write(self._current.seq, scratch=data)

    # --- internals --------------------------------------------------------

    def _write(self, seq: int, **fields: Any) -> None:
        """Every step write is fenced: a worker that lost its lease stops here."""
        if not self._store.update_step(self.run_id, seq, owner=self._engine.owner, **fields):
            raise _lost_lease()

    def _start(self, seq: int, name: str) -> StepRow:
        row = self._store.start_step(self.run_id, seq, name, self._engine.now(),
                                     owner=self._engine.owner)
        if row is None:
            raise _lost_lease()
        return row

    def _enter(self, name: str) -> tuple[int, StepRow | None]:
        if name in self._names:
            raise ValueError(f"step name repeated in the same run: {name!r}"
                             " (use names like 'review-{i}')")
        if self._current is not None:
            raise RuntimeError(f"steps do not nest: {name!r} inside {self._current.name!r}")
        self._names.add(name)
        reason = self._store.stop_reason(self.run_id, self._engine.now())
        if reason is not None:
            raise _Stop(reason)
        self._seq += 1
        row = self._store.get_step(self.run_id, self._seq)
        if row is not None and row.name != name:
            raise NonDeterminismError(
                f"step {self._seq} was called {row.name!r} and now {name!r}: the flow depends"
                " on something outside the steps or the code changed while the run was in flight")
        return self._seq, row

    def _check_budget(self, seq: int) -> None:
        if self._budget is None:
            return
        # Top-ups: `ratchet signal <run> ratchet.budget '{"add_usd": 20}'`.
        self._budget = self._store.absorb_budget_events(self.run_id, BUDGET_EVENT)
        spent = self._store.run_cost(self.run_id)
        if spent >= self._budget:
            raise _SuspendWith(_Suspension(
                event=BUDGET_EVENT,
                reason=f"budget used up: {spent:.2f} of {self._budget:.2f} USD"))

    def _absorb_human_signal(self, row: StepRow) -> None:
        """If the step stopped with `NeedsHuman`, the answer lands in `ctx.scratch['signal']`."""
        event = row.scratch.get("waiting_event")
        if not event:
            return
        ok, payload = self._store.consume_event(self.run_id, event, row.seq, reuse=False)
        scratch = {k: v for k, v in row.scratch.items() if k != "waiting_event"}
        if ok:
            scratch["signal"] = payload
        self._write(row.seq, scratch=scratch)

    def _suspend_from(self, seq: int, e: Exception) -> NoReturn:
        self._write(seq, status="waiting", error=_short(e))
        if isinstance(e, Pause):
            raise _SuspendWith(_Suspension(wake_at=e.until.timestamp(), reason=str(e)))
        assert isinstance(e, NeedsHuman)
        step = self._store.get_step(self.run_id, seq)
        assert step is not None
        self._write(seq, scratch={**step.scratch, "waiting_event": e.event, "question": str(e),
                                  "details": _jsonable(e.details, f"details of {e.event!r}"),
                                  "questions": e.questions})
        raise _SuspendWith(_Suspension(event=e.event, reason=str(e)))

    def _fail_step(self, seq: int, attempts: int, error: str) -> None:
        self._write(seq, status="failed", attempts=attempts, error=error,
                    finished_at=self._engine.now())


class Engine:
    def __init__(self, db: str | Path = "ratchet.db", *, owner: str | None = None,
                 lease_ttl_s: float = 300, heartbeat_s: float = 60,
                 clock: Callable[[], float] = time.time,
                 sleeper: Callable[[float], None] = time.sleep,
                 log: Callable[[str], None] = print):
        self.store = Store(db)
        self.owner = owner or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:6]}"
        self.lease_ttl_s = lease_ttl_s
        self.heartbeat_s = heartbeat_s
        self.now = clock
        self.sleeper = sleeper
        self.log = log
        self.workflows: dict[str, Workflow] = {}
        self._lease_lost = threading.Event()

    # --- registration -----------------------------------------------------

    def workflow(self, name: str | None = None, *,
                 cleanup: Callable[[Ending], Any] | None = None) -> Callable[[Callable], Callable]:
        """Registers a workflow.

        `cleanup(ending)` runs when a run ends without completing: it failed, was cancelled, or
        passed its deadline. It gets an `Ending` (what the finished steps returned and what the
        others left in scratch), so it can tear down whatever the run left behind. It runs as a
        step: stored, retried up to 3 attempts, resumed by `tick` if the process dies. A cleanup
        that fails for good is noted in the run's error; the run still ends.
        """
        def deco(fn: Callable) -> Callable:
            wf = Workflow(name or fn.__name__, fn, cleanup)
            self.workflows[wf.name] = wf
            return fn
        return deco

    # --- lifecycle --------------------------------------------------------

    def start(self, workflow: str, input: Any = None, *, run_id: str | None = None,
              budget_usd: float | None = None, timeout_s: float | None = None,
              execute: bool = True) -> str:
        """Creates a run and executes it. With `run_id` it is idempotent: an existing run is
        neither duplicated nor woken up early.

        With `timeout_s`, the run is cancelled once that much time has passed since it was
        created, waits included: a run waiting for a person who never answers ends too.
        """
        wf = self._wf(workflow)
        run_id = run_id or uuid.uuid4().hex[:12]
        if self.store.get_run(run_id) is None:
            now = self.now()
            deadline = now + timeout_s if timeout_s is not None else None
            self.store.insert_run(run_id, wf.name, input, wf.code_hash, budget_usd, now,
                                  deadline=deadline)
        if execute:
            self.run(run_id)
        return run_id

    def run(self, run_id: str) -> RunRow:
        """Executes (or resumes) a run if it is due and nobody else holds it. Returns its state."""
        run = self.store.get_run(run_id)
        if run is None:
            raise KeyError(run_id)
        if run.status in FINAL:
            return run
        if run.status == "waiting" and not self.store.is_runnable(run_id, self.now()):
            return run  # its time has not come and its event has not arrived
        if not self.store.acquire_lease(run_id, self.owner, self.now(), self.lease_ttl_s):
            self.log(f"[{run_id}] another process is running it; leaving it alone")
            return self.store.get_run(run_id)  # type: ignore[return-value]
        run = self.store.get_run(run_id)
        assert run is not None
        wf = self._wf(run.workflow)
        if run.code_hash and run.code_hash != wf.code_hash:
            self.log(f"[{run_id}] warning: the code of {wf.name!r} changed since the run started"
                     f" ({run.code_hash} -> {wf.code_hash})")
        self._lease_lost.clear()
        ctx = Context(self, run)
        stop = threading.Event()
        hb = threading.Thread(target=self._heartbeat, args=(run_id, stop, ctx), daemon=True)
        hb.start()
        try:
            if run.ending is not None:
                # The process died (or waited) while the run was ending: finish the cleanup.
                self._end(run, run.ending["status"], run.ending["reason"])
            elif (reason := self.store.stop_reason(run_id, self.now())) is not None:
                self._end(run, "cancelled", reason)
            else:
                self._execute(run, wf, ctx)
        except KeyboardInterrupt:
            # Stopped by hand (or SIGTERM through the CLI): release the lease so the run can be
            # resumed right away instead of waiting for the TTL.
            self.store.release_lease(run_id, self.owner)
            raise
        finally:
            stop.set()
            hb.join(timeout=1)
        return self.store.get_run(run_id)  # type: ignore[return-value]

    def _execute(self, run: RunRow, wf: Workflow, ctx: Context) -> None:
        run_id = run.id
        try:
            out = wf.fn(ctx, *_args(run.input))
        except _Stop as s:
            self._end(run, "cancelled", s.reason)
        except _SuspendWith as s:
            sus = s.suspension
            if sus.lost_lease:
                self.log(f"[{run_id}] lost the lease: another process took the run over")
            else:
                self.store.finish_run(run_id, self.owner, "waiting", self.now(),
                                      wake_at=sus.wake_at, wait_event=sus.event, error=sus.reason)
                self.log(f"[{run_id}] waiting: {sus.reason}")
        except KeyboardInterrupt:
            # An agent adapter stops its step this way once `ctx.cancelled()` turns true. If the
            # heartbeat set it because the run must stop, end the run; a real Ctrl-C goes on up.
            reason = self.store.stop_reason(run_id, self.now()) if ctx.cancelled() else None
            if reason is None:
                raise
            self._end(run, "cancelled", reason)
        except Exception as e:  # noqa: BLE001
            self._end(run, "failed", f"{_short(e)}\n{traceback.format_exc(limit=5)}")
        else:
            try:
                output = _jsonable(out, "<workflow result>")
            except Fatal as e:
                self._end(run, "failed", _short(e))
            else:
                self.store.finish_run(run_id, self.owner, "completed", self.now(), output=output)
                self.log(f"[{run_id}] completed")

    def _end(self, run: RunRow, status: str, reason: str) -> None:
        """Ends a run that did not complete: runs the workflow's cleanup, then the final status.

        The ending is stored first, so a crash during the cleanup resumes the cleanup (and only
        that) at the next `tick`.
        """
        ending = run.ending
        if ending is None:
            ending = {"status": status, "reason": reason,
                      "seq": self.store.next_cleanup_seq(run.id)}
            if not self.store.set_ending(run.id, self.owner, ending):
                self.log(f"[{run.id}] lost the lease: another process took the run over")
                return
        wf = self.workflows.get(run.workflow)
        note = ""
        if wf is not None and wf.cleanup is not None:
            seq = ending["seq"]
            name = "ratchet.cleanup" if seq == -1 else f"ratchet.cleanup{seq}"
            steps = self.store.list_steps(run.id)
            info = Ending(run.id, run.workflow, run.input, status, reason,
                          outputs={s.name: s.output for s in steps
                                   if s.status == "done" and s.seq > 0},
                          scratch={s.name: s.scratch for s in steps if s.scratch and s.seq > 0})
            try:
                Context(self, run)._run_step(seq, name, wf.cleanup, (info,), {}, 3, 2.0, 300.0)
            except _SuspendWith as s:
                sus = s.suspension
                if sus.lost_lease:
                    self.log(f"[{run.id}] lost the lease: another process took the run over")
                else:   # a cleanup that pauses or asks a person: the run waits, still ending
                    self.store.finish_run(run.id, self.owner, "waiting", self.now(),
                                          wake_at=sus.wake_at, wait_event=sus.event,
                                          error=sus.reason)
                    self.log(f"[{run.id}] cleanup waiting: {sus.reason}")
                return
            except StepFailed as e:
                note = f"\ncleanup failed: {e}"
        if status == "cancelled":
            self.store.cancel_open_steps(run.id, self.owner)
        self.store.finish_run(run.id, self.owner, status, self.now(), error=reason + note)
        first = reason.splitlines()[0] if reason else status
        self.log(f"[{run.id}] {status}: {first}" + (" (cleanup failed)" if note else ""))

    def tick(self) -> list[str]:
        """Resumes everything that is due: expired waits, arrived events and dead workers."""
        done = []
        for run_id in self.store.runnable_ids(self.now()):
            run = self.store.get_run(run_id)
            if run and run.workflow in self.workflows:
                self.run(run_id)
                done.append(run_id)
        return done

    def signal(self, run_id: str, event: str, payload: Any = None) -> None:
        if self.store.get_run(run_id) is None:
            raise KeyError(run_id)
        self.store.add_event(run_id, event, payload, self.now())

    def retry(self, run_id: str) -> None:
        """Reopens a `failed` run: its failed steps get all their attempts back."""
        self.store.reopen_failed(run_id)

    def cancel(self, run_id: str, reason: str = "cancelled") -> bool:
        """Asks a run to stop; False if it has already finished.

        A run nobody holds ends at the next `tick`. A run in flight stops between steps, and its
        agents within a heartbeat (`heartbeat_s`). Either way its `cleanup` runs, then the run
        is `cancelled`.
        """
        if self.store.get_run(run_id) is None:
            raise KeyError(run_id)
        return self.store.request_cancel(run_id, reason, self.now())

    # --- internals --------------------------------------------------------

    def _wf(self, name: str) -> Workflow:
        try:
            return self.workflows[name]
        except KeyError:
            raise KeyError(f"workflow not registered: {name!r}") from None

    def _heartbeat(self, run_id: str, stop: threading.Event, ctx: Context) -> None:
        while not stop.wait(self.heartbeat_s):
            try:
                if self.store.stop_reason(run_id, self.now()) is not None:
                    ctx._cancel.set()   # the adapters kill their agent; the run ends after
            except sqlite3.Error:
                pass
            try:
                renewed = self.store.renew_lease(run_id, self.owner, self.now(), self.lease_ttl_s)
            except sqlite3.Error as e:
                # A busy database is not a lost lease: try again on the next beat.
                self.log(f"[{run_id}] could not renew the lease ({e}); retrying")
                continue
            if not renewed:
                self._lease_lost.set()
                return

    def _raise_if_lease_lost(self) -> None:
        if self._lease_lost.is_set():
            raise _lost_lease()


def _args(input_: Any) -> tuple:
    return () if input_ is None else (input_,)


def _short(e: BaseException) -> str:
    return f"{type(e).__name__}: {e}"[:500]


def _jsonable(value: Any, where: str) -> Any:
    """Returns the value as it will come back from the database, so replays see the same."""
    try:
        return json.loads(json.dumps(value))
    except (TypeError, ValueError) as e:
        raise Fatal(f"{where}: a step's output must be JSON ({e})") from e
