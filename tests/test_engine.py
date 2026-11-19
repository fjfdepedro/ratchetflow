import sqlite3
import time
from datetime import datetime

import pytest

from ratchetflow import Fatal, NeedsHuman, NonDeterminismError, Pause, Retryable, StepFailed
from ratchetflow.engine import BUDGET_EVENT
from ratchetflow.store import Store

from .conftest import Crash


def register(engine, calls, crash_on=None):
    @engine.workflow("demo")
    def demo(ctx, topic):
        def do(name):
            calls.append(name)
            if name == crash_on and calls.count(name) == 1:
                raise Crash()
            return f"{name}:{topic}"

        a = ctx.step("plan", do, "plan")
        b = ctx.step("implement", do, "implement")
        return [a, b]


def test_a_crash_only_costs_the_step_in_flight(make_engine, clock):
    calls = []
    e1 = make_engine(lease_ttl_s=60)
    register(e1, calls, crash_on="implement")
    with pytest.raises(Crash):
        e1.start("demo", "T-1", run_id="r1")

    e2 = make_engine()
    register(e2, calls)
    assert e2.tick() == []          # the dead worker's lease is still live: nobody steps on it
    clock.advance(61)
    assert e2.tick() == ["r1"]
    run = e2.store.get_run("r1")
    assert run.status == "completed"
    assert run.output == ["plan:T-1", "implement:T-1"]
    assert calls == ["plan", "implement", "implement"]   # plan did not run again


def test_ctrl_c_releases_the_lease_to_resume_right_away(make_engine):
    calls = []
    e = make_engine()

    @e.workflow("w")
    def w(ctx):
        ctx.step("a", lambda: calls.append("a"))
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        e.start("w", run_id="r")
    assert e.store.runnable_ids(e.now()) == ["r"]


def test_retries_with_growing_backoff(make_engine):
    e = make_engine()
    attempts = []

    def flaky():
        attempts.append(1)
        if len(attempts) < 3:
            raise Retryable("529")
        return "ok"

    @e.workflow("w")
    def w(ctx):
        return ctx.step("x", flaky, retries=3, backoff_s=2)

    e.start("w", run_id="r")
    assert e.store.get_run("r").status == "completed"
    assert make_engine.sleeps == [2, 4]
    assert e.store.list_steps("r")[0].attempts == 3


def test_backoff_grows_with_a_base_below_one(make_engine):
    e = make_engine()

    @e.workflow("w")
    def w(ctx):
        ctx.step("x", lambda: 1 / 0, retries=4, backoff_s=0.5)

    e.start("w", run_id="r")
    assert make_engine.sleeps == [0.5, 1.0, 2.0]


def test_runs_out_of_retries_and_retry_reopens_it(make_engine):
    e = make_engine()
    broken = {"yes": True}

    def step():
        if broken["yes"]:
            raise RuntimeError("boom")
        return 1

    @e.workflow("w")
    def w(ctx):
        return ctx.step("x", step, retries=2)

    e.start("w", run_id="r")
    run = e.store.get_run("r")
    assert run.status == "failed" and "boom" in run.error
    broken["yes"] = False
    e.retry("r")
    assert e.run("r").status == "completed"


def test_fatal_does_not_retry(make_engine):
    e = make_engine()
    n = []

    def step():
        n.append(1)
        raise Fatal("bad input")

    @e.workflow("w")
    def w(ctx):
        ctx.step("x", step, retries=5)

    e.start("w", run_id="r")
    assert e.store.get_run("r").status == "failed" and len(n) == 1


def test_a_step_that_kills_the_process_runs_out_of_attempts(make_engine, clock):
    e = make_engine(lease_ttl_s=60)

    @e.workflow("w")
    def w(ctx):
        def die():
            raise Crash()
        ctx.step("poison", die, retries=3)

    with pytest.raises(Crash):
        e.start("w", run_id="r")
    for _ in range(3):
        clock.advance(61)
        try:
            e.tick()
        except Crash:
            pass
    run = e.store.get_run("r")
    assert run.status == "failed" and "poison" in run.error
    assert e.store.list_steps("r")[0].attempts == 3


def test_a_failed_step_caught_by_the_workflow_does_not_run_again(make_engine, clock):
    e = make_engine()
    effects = []

    @e.workflow("w")
    def w(ctx):
        try:
            ctx.step("effect", lambda: effects.append(1) or 1 / 0, retries=1)
        except StepFailed:
            pass
        ctx.sleep("s", 10)
        return "end"

    e.start("w", run_id="r")
    clock.advance(10)
    e.tick()
    assert effects == [1] and e.store.get_run("r").output == "end"


def test_pause_waits_until_the_time_and_reruns_the_step_with_its_scratch(make_engine, clock):
    e = make_engine()
    seen = []

    @e.workflow("w")
    def w(ctx):
        def agent():
            seen.append(ctx.scratch.get("session_id"))
            if len(seen) == 1:
                ctx.save_scratch(session_id="s-1")
                raise Pause(datetime.fromtimestamp(clock() + 3600), "quota")
            return "done"
        return ctx.step("agent", agent)

    e.start("w", run_id="r")
    run = e.store.get_run("r")
    assert run.status == "waiting" and run.wake_at == clock() + 3600
    clock.advance(1800)
    assert e.tick() == []
    clock.advance(1801)
    assert e.tick() == ["r"]
    assert e.store.get_run("r").output == "done"
    assert seen == [None, "s-1"]                    # the retry sees the stored session
    assert e.store.list_steps("r")[0].attempts == 1  # a pause costs no attempt


def test_needs_human_inside_a_step_gets_the_answer(make_engine):
    e = make_engine()

    @e.workflow("w")
    def w(ctx):
        def approval():
            sig = ctx.scratch.get("signal")
            if sig is None:
                raise NeedsHuman("outline.ok", "the outline needs a human look")
            return sig["decision"]
        return ctx.step("approval", approval)

    e.start("w", run_id="r")
    assert e.store.get_run("r").wait_event == "outline.ok"
    assert e.tick() == []
    e.signal("r", "outline.ok", {"decision": "go on"})
    assert e.tick() == ["r"]
    assert e.store.get_run("r").output == "go on"


def test_starting_again_does_not_wake_a_waiting_run(make_engine):
    e = make_engine()
    n = []

    @e.workflow("w")
    def w(ctx):
        def expensive():
            n.append(1)
            if ctx.scratch.get("signal") is None:
                raise NeedsHuman("ok")
            return "done"
        return ctx.step("expensive", expensive)

    e.start("w", run_id="r")
    e.start("w", run_id="r")
    e.run("r")
    assert len(n) == 1
    e.signal("r", "ok", {})
    e.tick()
    assert e.store.get_run("r").status == "completed" and len(n) == 2


def test_wait_for_event_and_its_timeout(make_engine, clock):
    e = make_engine()

    @e.workflow("w")
    def w(ctx):
        a = ctx.wait_for_event("merge", "pr.merged")
        b = ctx.wait_for_event("qa-ok", "qa.ok", timeout_s=600)
        return [a, b]

    e.start("w", run_id="r")
    e.signal("r", "pr.merged", {"pr": 7})
    e.tick()
    assert e.store.get_run("r").wait_event == "qa.ok"
    clock.advance(601)
    e.tick()
    assert e.store.get_run("r").output == [{"pr": 7}, None]


def test_durable_sleep(make_engine, clock):
    e = make_engine()

    @e.workflow("w")
    def w(ctx):
        ctx.sleep("wait-ci", 300)
        return "after"

    e.start("w", run_id="r")
    assert e.store.get_run("r").status == "waiting"
    clock.advance(300)
    e.tick()
    assert e.store.get_run("r").output == "after"


def test_budget_stops_the_run_until_it_is_raised(make_engine):
    e = make_engine()
    done = []

    @e.workflow("w")
    def w(ctx):
        for i in range(3):
            def step(i=i):
                ctx.record_cost(4.0)
                done.append(i)
            ctx.step(f"round-{i}", step)
        return "end"

    e.start("w", run_id="r", budget_usd=6)
    run = e.store.get_run("r")
    assert run.status == "waiting" and run.wait_event == BUDGET_EVENT
    assert done == [0, 1]                         # stops before starting round 2
    e.signal("r", BUDGET_EVENT, {"add_usd": 10})
    e.tick()
    run = e.store.get_run("r")
    assert run.status == "completed" and run.budget_usd == 16 and done == [0, 1, 2]


def test_detects_a_non_deterministic_flow(make_engine, clock):
    e = make_engine()
    names = iter(["a", "b"])

    @e.workflow("w")
    def w(ctx):
        ctx.step(next(names), lambda: 1)
        ctx.sleep("s", 10)

    e.start("w", run_id="r")
    clock.advance(10)
    e.tick()
    run = e.store.get_run("r")
    assert run.status == "failed" and NonDeterminismError.__name__ in run.error


def test_repeated_step_names_fail(make_engine):
    e = make_engine()

    @e.workflow("w")
    def w(ctx):
        ctx.step("x", lambda: 1)
        ctx.step("x", lambda: 2)

    e.start("w", run_id="r")
    assert "repeated" in e.store.get_run("r").error


def test_start_with_an_id_is_idempotent(make_engine):
    e = make_engine()
    n = []

    @e.workflow("w")
    def w(ctx, x):
        return ctx.step("x", lambda: n.append(x) or x)

    e.start("w", 1, run_id="job-abc")
    e.start("w", 1, run_id="job-abc")
    assert n == [1] and len(e.store.list_runs()) == 1


def test_non_json_output_fails_clearly(make_engine):
    e = make_engine()

    @e.workflow("w")
    def w(ctx):
        ctx.step("x", lambda: object())

    e.start("w", run_id="r")
    assert "JSON" in e.store.get_run("r").error


def test_a_step_output_is_the_same_on_replay(make_engine, clock):
    e = make_engine()
    seen = []

    @e.workflow("w")
    def w(ctx):
        seen.append(ctx.step("x", lambda: {1: (2, 3)}))
        ctx.sleep("s", 10)

    e.start("w", run_id="r")
    clock.advance(10)
    e.tick()
    assert seen == [{"1": [2, 3]}, {"1": [2, 3]}]


def test_the_database_path_expands_the_tilde(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    Store("~/x/r.db")
    assert (tmp_path / "x" / "r.db").exists()


def test_a_worker_without_the_lease_does_not_overwrite_the_step(make_engine, clock):
    e1 = make_engine(lease_ttl_s=60)
    e2 = make_engine(lease_ttl_s=60)

    @e2.workflow("w")
    def w2(ctx):
        return ctx.step("slow", lambda: "e2")

    def slow():
        clock.advance(61)          # e1 hangs for longer than its lease...
        e2.tick()                  # ...and e2 takes the run over and finishes it
        return "e1"

    @e1.workflow("w")
    def w1(ctx):
        return ctx.step("slow", slow)

    e1.start("w", run_id="r")
    run = e1.store.get_run("r")
    assert run.status == "completed" and run.output == "e2"
    assert e1.store.list_steps("r")[0].output == "e2"


def test_the_heartbeat_survives_a_sqlite_error(make_engine):
    e = make_engine(heartbeat_s=0.01)
    beats = []
    renew = e.store.renew_lease

    def sometimes_locked(*a):
        beats.append(1)
        if len(beats) == 1:
            raise sqlite3.OperationalError("database is locked")
        return renew(*a)

    e.store.renew_lease = sometimes_locked

    @e.workflow("w")
    def w(ctx):
        def wait():
            end = time.monotonic() + 2
            while len(beats) < 3 and time.monotonic() < end:
                time.sleep(0.01)
        ctx.step("x", wait)
        return len(beats) >= 3

    e.start("w", run_id="r")
    assert e.store.get_run("r").output is True


def test_budget_left(make_engine):
    e = make_engine()
    seen = []

    @e.workflow("w")
    def w(ctx):
        seen.append(ctx.budget_left())
        ctx.step("x", lambda: ctx.record_cost(4.0))
        seen.append(ctx.budget_left())

    e.start("w", run_id="r", budget_usd=6)
    e.start("w", run_id="nobudget")
    assert seen == [6, 2, None, None]


def test_the_inbox_lists_only_runs_waiting_for_a_person(make_engine):
    e = make_engine()
    long_question = "Approve the outline? " + "It covers a lot of ground. " * 20

    @e.workflow("human")
    def human(ctx):
        def approval():
            if ctx.scratch.get("signal") is None:
                raise NeedsHuman("outline.ok", long_question, details={"sections": ["intro", "body"]})
            return "approved"
        return ctx.step("approval", approval)

    @e.workflow("external")
    def external(ctx):
        return ctx.wait_for_event("ci", "ci.passed")

    @e.workflow("money")
    def money(ctx):
        ctx.step("a", lambda: ctx.record_cost(5))
        ctx.step("b", lambda: 1)

    e.start("human", run_id="h")
    e.start("external", run_id="x")
    e.start("money", run_id="m", budget_usd=1)

    inbox = {item["run_id"]: item for item in e.store.inbox(BUDGET_EVENT)}
    assert set(inbox) == {"h", "m"}                       # waiting on CI is not a person's job
    assert inbox["h"]["event"] == "outline.ok"
    assert inbox["h"]["step"] == "approval"
    assert inbox["h"]["question"] == long_question        # whole, not cut
    assert inbox["h"]["details"] == {"sections": ["intro", "body"]}
    assert inbox["m"]["event"] == BUDGET_EVENT and "budget" in inbox["m"]["question"]

    e.signal("h", "outline.ok", {})
    e.tick()
    assert "h" not in {item["run_id"] for item in e.store.inbox(BUDGET_EVENT)}


def test_busy_time_counts_the_work_not_the_waits(make_engine, clock):
    e = make_engine()
    tries = []

    @e.workflow("w")
    def w(ctx):
        def work():
            clock.advance(5)               # every attempt works for 5 s
            tries.append(1)
            if len(tries) == 1:
                raise Retryable("flaky")   # the backoff after it is not work
            if ctx.scratch.get("signal") is None:
                raise NeedsHuman("ok")
            return "done"
        return ctx.step("work", work)

    e.start("w", run_id="r")
    clock.advance(3600)                    # an hour waiting for a person
    e.signal("r", "ok", {})
    e.tick()
    step = e.store.list_steps("r")[0]
    assert step.status == "done"
    assert step.busy_s == 15               # three attempts of 5 s
    assert step.finished_at - step.started_at == 3615


def test_a_database_from_0_1_gains_the_busy_column(tmp_path):
    import sqlite3

    from ratchetflow.store import SCHEMA, Store

    path = tmp_path / "old.db"
    old = sqlite3.connect(path)
    old.executescript(SCHEMA.replace("    busy_s       REAL,"
                                     "                   -- time spent running the step,"
                                     " waits left out\n", ""))
    old.execute("INSERT INTO runs (id, workflow, input_json, status, created_at, updated_at)"
                " VALUES ('r', 'w', 'null', 'completed', 0, 0)")
    old.execute("INSERT INTO steps (run_id, seq, name, status, started_at, finished_at)"
                " VALUES ('r', 1, 'x', 'done', 0, 10)")
    old.commit()
    assert "busy_s" not in {r[1] for r in old.execute("PRAGMA table_info(steps)")}
    old.close()
    store = Store(path)
    assert store.list_steps("r")[0].busy_s is None   # unknown, not zero
    Store(path)                                       # a second open does not fail
