"""How a run ends without completing: cancelled, past its deadline, or failed, and its cleanup."""

import sqlite3
import time

import pytest

from ratchetflow import Ending, Fatal, NeedsHuman
from ratchetflow.store import Store

from .conftest import Crash


def waits_for_ok(engine, cleanups=None):
    @engine.workflow("w", cleanup=(cleanups.append if cleanups is not None else None))
    def w(ctx, topic="t"):
        made = ctx.step("sandbox", lambda: {"id": f"sb-{topic}"})
        ctx.wait_for_event("ok", "ok")
        return ctx.step("after", lambda: made["id"])


def test_cancelling_a_waiting_run_ends_it_at_the_next_tick(make_engine):
    e = make_engine()
    cleanups: list[Ending] = []
    waits_for_ok(e, cleanups)
    e.start("w", "x", run_id="r")
    assert e.store.get_run("r").status == "waiting"

    assert e.cancel("r", "not needed any more")
    assert e.store.get_run("r").status == "waiting"   # nothing runs until the tick
    assert e.tick() == ["r"]
    run = e.store.get_run("r")
    assert run.status == "cancelled"
    assert run.error == "not needed any more"
    assert run.ending is None
    [end] = cleanups
    assert (end.run_id, end.status, end.reason) == ("r", "cancelled", "not needed any more")
    assert end.outputs == {"sandbox": {"id": "sb-x"}}
    assert end.input == "x"
    steps = e.store.list_steps("r")
    assert [(s.name, s.status) for s in steps][:2] == [("sandbox", "done"), ("ok", "cancelled")]
    assert [s.name for s in steps][-1] == "ratchet.cleanup"
    assert steps[-1].seq == -1 and steps[-1].status == "done"


def test_a_finished_run_cannot_be_cancelled(make_engine):
    e = make_engine()

    @e.workflow("w")
    def w(ctx):
        return 1

    e.start("w", run_id="r")
    assert not e.cancel("r")
    assert e.store.get_run("r").status == "completed"
    with pytest.raises(KeyError):
        e.cancel("nope")


def test_cancel_stops_a_run_in_flight_between_steps(make_engine):
    e = make_engine()
    ran = []

    @e.workflow("w")
    def w(ctx):
        ctx.step("a", lambda: (ran.append("a"), e.cancel(ctx.run_id, "stop it")))
        ctx.step("b", lambda: ran.append("b"))

    e.start("w", run_id="r")
    assert ran == ["a"]
    assert e.store.get_run("r").status == "cancelled"
    assert [s.name for s in e.store.list_steps("r")] == ["a"]   # no cleanup registered


def test_the_heartbeat_tells_a_long_step_to_stop(make_engine):
    """What the agent adapters do: poll ctx.cancelled() and stop with KeyboardInterrupt."""
    e = make_engine(heartbeat_s=0.01)
    cleanups: list[Ending] = []

    @e.workflow("w", cleanup=cleanups.append)
    def w(ctx):
        def agent():
            ctx.save_scratch(session="s-1")
            e.cancel(ctx.run_id, "from another terminal")
            end = time.monotonic() + 5
            while not ctx.cancelled():
                if time.monotonic() > end:
                    return "never noticed"
                time.sleep(0.005)
            raise KeyboardInterrupt("run stopped")
        return ctx.step("agent", agent)

    e.start("w", run_id="r")
    run = e.store.get_run("r")
    assert run.status == "cancelled"
    assert run.owner is None
    [end] = cleanups
    assert end.scratch == {"agent": {"session": "s-1"}}
    assert end.outputs == {}


def test_a_real_ctrl_c_still_goes_up(make_engine):
    e = make_engine()
    cleanups: list = []

    @e.workflow("w", cleanup=cleanups.append)
    def w(ctx):
        ctx.step("a", lambda: (_ for _ in ()).throw(KeyboardInterrupt()))

    with pytest.raises(KeyboardInterrupt):
        e.start("w", run_id="r")
    assert cleanups == []
    assert e.store.runnable_ids(e.now()) == ["r"]   # resumable at once


def test_a_waiting_run_is_cancelled_when_it_passes_its_deadline(make_engine, clock):
    e = make_engine()
    cleanups: list[Ending] = []
    waits_for_ok(e, cleanups)
    e.start("w", run_id="r", timeout_s=3600)
    clock.advance(3599)
    assert e.tick() == []
    clock.advance(2)
    assert e.tick() == ["r"]
    run = e.store.get_run("r")
    assert run.status == "cancelled"
    assert run.error.startswith("timed out: the run passed its deadline")
    assert cleanups[0].status == "cancelled"


def test_the_deadline_is_checked_between_steps(make_engine, clock):
    e = make_engine()
    ran = []

    @e.workflow("w")
    def w(ctx):
        ctx.step("slow", lambda: (ran.append("slow"), clock.advance(120)))
        ctx.step("next", lambda: ran.append("next"))

    e.start("w", run_id="r", timeout_s=60)
    assert ran == ["slow"]
    assert e.store.get_run("r").status == "cancelled"


def test_restarting_with_the_same_id_keeps_the_first_deadline(make_engine, clock):
    e = make_engine()
    waits_for_ok(e)
    e.start("w", run_id="r", timeout_s=60)
    clock.advance(30)
    e.start("w", run_id="r", timeout_s=3600)
    assert e.store.get_run("r").deadline == clock.t - 30 + 60


def test_a_failed_run_is_cleaned_up_too(make_engine):
    e = make_engine()
    cleanups: list[Ending] = []

    @e.workflow("w", cleanup=cleanups.append)
    def w(ctx):
        ctx.step("sandbox", lambda: "sb-1")
        ctx.step("boom", lambda: (_ for _ in ()).throw(Fatal("bad input")))

    e.start("w", run_id="r")
    run = e.store.get_run("r")
    assert run.status == "failed"
    [end] = cleanups
    assert end.status == "failed"
    assert "bad input" in end.reason
    assert end.outputs == {"sandbox": "sb-1"}


def test_a_crash_during_the_cleanup_resumes_only_the_cleanup(make_engine, clock):
    e1 = make_engine(lease_ttl_s=60)
    ran = []

    def cleanup(end):
        ran.append(("cleanup", end.status))
        if ran.count(("cleanup", "failed")) == 1:
            raise Crash()

    @e1.workflow("w", cleanup=cleanup)
    def w(ctx):
        ran.append("body")
        ctx.step("a", lambda: ran.append("a"))
        raise Fatal("gave up")

    with pytest.raises(Crash):
        e1.start("w", run_id="r")
    assert e1.store.get_run("r").ending["status"] == "failed"

    e2 = make_engine()
    e2.workflows = e1.workflows
    clock.advance(61)
    assert e2.tick() == ["r"]
    run = e2.store.get_run("r")
    assert run.status == "failed"
    assert "gave up" in run.error
    # The workflow did not run again: only its cleanup did.
    assert ran == ["body", "a", ("cleanup", "failed"), ("cleanup", "failed")]
    assert e2.store.list_steps("r")[-1].attempts == 2


def test_a_cleanup_that_fails_for_good_is_noted_and_the_run_still_ends(make_engine):
    e = make_engine()

    def cleanup(_end):
        raise RuntimeError("sandbox API down")

    @e.workflow("w", cleanup=cleanup)
    def w(ctx):
        ctx.wait_for_event("ok", "ok")

    e.start("w", run_id="r")
    e.cancel("r")
    e.tick()
    run = e.store.get_run("r")
    assert run.status == "cancelled"
    assert "cleanup failed" in run.error and "sandbox API down" in run.error
    assert e.store.list_steps("r")[-1].status == "failed"


def test_a_cleanup_can_wait_for_a_person(make_engine):
    e = make_engine()
    asked = []

    def cleanup(end):
        if not asked:
            asked.append(1)
            raise NeedsHuman("cleanup.ok", "delete the branch by hand, then say so")
        return "done"

    @e.workflow("w", cleanup=cleanup)
    def w(ctx):
        raise Fatal("no")

    e.start("w", run_id="r")
    run = e.store.get_run("r")
    assert (run.status, run.wait_event) == ("waiting", "cleanup.ok")
    assert run.ending["status"] == "failed"
    e.signal("r", "cleanup.ok")
    e.tick()
    assert e.store.get_run("r").status == "failed"


def test_a_retried_run_goes_on_and_cleans_up_again(make_engine):
    e = make_engine()
    cleanups: list[Ending] = []
    fail = [True]

    def flaky():
        if fail[0]:
            raise Fatal("first time")
        return "ok"

    @e.workflow("w", cleanup=cleanups.append)
    def w(ctx):
        ctx.step("a", lambda: 1)
        ctx.step("b", flaky)
        return ctx.step("c", lambda: 3)

    e.start("w", run_id="r")
    assert e.store.get_run("r").status == "failed"
    fail[0] = False
    e.retry("r")
    e.tick()
    run = e.store.get_run("r")
    assert run.status == "completed" and run.output == 3
    assert len(cleanups) == 1
    names = [s.name for s in e.store.list_steps("r")]
    assert names == ["a", "b", "c", "ratchet.cleanup"]   # cleanup out of the workflow's way

    # Failing again after the retry runs a second cleanup, with its own name.
    e2 = make_engine()

    @e2.workflow("v", cleanup=cleanups.append)
    def v(ctx):
        ctx.step("x", lambda: (_ for _ in ()).throw(Fatal("again")))

    e2.start("v", run_id="s")
    e2.retry("s")
    e2.tick()
    assert [s.name for s in e2.store.list_steps("s")] == ["x", "ratchet.cleanup",
                                                          "ratchet.cleanup-2"]


def test_a_database_from_0_2_gains_the_new_run_columns(tmp_path):
    path = tmp_path / "old.db"
    old = sqlite3.connect(path)
    old.executescript("""
        CREATE TABLE runs (id TEXT PRIMARY KEY, workflow TEXT NOT NULL, input_json TEXT NOT NULL,
            status TEXT NOT NULL, wake_at REAL, wait_event TEXT, owner TEXT, lease_until REAL,
            code_hash TEXT, budget_usd REAL, output_json TEXT, error TEXT,
            created_at REAL NOT NULL, updated_at REAL NOT NULL);
        INSERT INTO runs (id, workflow, input_json, status, created_at, updated_at)
            VALUES ('r', 'w', 'null', 'waiting', 0, 0);
    """)
    old.commit()
    old.close()
    store = Store(path)
    run = store.get_run("r")
    assert (run.cancel_reason, run.deadline, run.ending, run.parent_id) == (None,) * 4
    assert store.request_cancel("r", "bye", 1)
    assert store.runnable_ids(1) == ["r"]
    Store(path)
