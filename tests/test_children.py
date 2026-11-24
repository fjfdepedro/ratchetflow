"""Child runs: `ctx.invoke` runs another workflow as its own run and waits for it."""

import pytest

from ratchetflow import NeedsHuman, StepFailed
from ratchetflow.engine import BUDGET_EVENT


def register(engine, ran, child_fails=False, ask=False, cleanups=None):
    @engine.workflow("research", cleanup=(cleanups.append if cleanups is not None else None))
    def research(ctx, topic):
        ran.append(f"research:{topic}")

        def approve():
            if ask and ctx.scratch.get("signal") is None:
                raise NeedsHuman("sources.ok", f"use these sources for {topic}?")
            return "approved"

        ctx.step("approve", approve)
        if child_fails:
            ctx.step("fetch", lambda: (_ for _ in ()).throw(ValueError("site down")), retries=1)
        return ctx.step("notes", lambda: f"notes on {topic}")

    @engine.workflow("article")
    def article(ctx, topic):
        ran.append("article")
        try:
            notes = ctx.invoke("research", "research", topic)
        except StepFailed as e:
            notes = f"no notes ({e})"
        return ctx.step("write", lambda: f"article from {notes}")


def test_a_child_runs_in_the_same_tick_and_hands_back_its_output(make_engine):
    e = make_engine()
    ran: list[str] = []
    register(e, ran)
    e.start("article", "tides", run_id="a")
    run = e.store.get_run("a")
    assert (run.status, run.wait_event) == ("waiting", "ratchet.child:a.research")
    child = e.store.get_run("a.research")
    assert (child.status, child.parent_id, child.input) == ("pending", "a", "tides")

    assert e.tick() == ["a.research", "a"]
    assert e.store.get_run("a").output == "article from notes on tides"
    assert e.store.get_run("a.research").status == "completed"
    assert e.tick() == []
    assert len(e.store.children("a")) == 1          # replays found the same child
    assert e.store.list_steps("a")[0].scratch == {"child": "a.research"}


def test_a_childs_question_shows_up_in_the_inbox(make_engine):
    e = make_engine()
    ran: list[str] = []
    register(e, ran, ask=True)
    e.start("article", "tides", run_id="a")
    e.tick()
    [item] = e.store.inbox(BUDGET_EVENT)
    assert (item["run_id"], item["event"]) == ("a.research", "sources.ok")
    e.signal("a.research", "sources.ok", "yes")
    assert e.tick() == ["a.research", "a"]
    assert e.store.get_run("a").status == "completed"


def test_a_failed_child_raises_step_failed_in_the_parent(make_engine):
    e = make_engine()
    ran: list[str] = []
    register(e, ran, child_fails=True)
    e.start("article", "tides", run_id="a")
    e.tick()
    run = e.store.get_run("a")
    assert run.status == "completed"
    assert run.output.startswith("article from no notes (research: child run a.research failed")
    assert "site down" in run.output
    assert e.store.list_steps("a")[0].status == "failed"
    ran.clear()
    e.tick()
    assert ran == []                                # nothing is due any more


def test_cancelling_the_parent_cancels_the_child_and_cleans_it_up(make_engine):
    e = make_engine()
    ran: list[str] = []
    cleanups: list = []
    register(e, ran, ask=True, cleanups=cleanups)
    e.start("article", "tides", run_id="a")
    e.tick()
    assert e.cancel("a", "changed my mind")
    assert e.store.get_run("a.research").cancel_reason == "parent run a cancelled"
    assert sorted(e.tick()) == ["a", "a.research"]
    assert e.store.get_run("a").status == "cancelled"
    assert e.store.get_run("a.research").status == "cancelled"
    assert [c.run_id for c in cleanups] == ["a.research"]


def test_a_parent_past_its_deadline_cancels_its_child(make_engine, clock):
    e = make_engine()
    ran: list[str] = []
    register(e, ran, ask=True)
    e.start("article", "tides", run_id="a", timeout_s=60)
    e.tick()
    clock.advance(61)
    e.tick()
    assert e.store.get_run("a").status == "cancelled"
    child = e.store.get_run("a.research")
    assert (child.status, child.error) == ("cancelled", "parent run a cancelled")


def test_the_child_gets_what_is_left_of_the_budget(make_engine):
    e = make_engine()

    @e.workflow("child")
    def child(ctx):
        return ctx.budget_left()

    @e.workflow("parent")
    def parent(ctx):
        ctx.step("spend", lambda: ctx.record_cost(4))
        a = ctx.invoke("inherits", "child")
        b = ctx.invoke("capped", "child", budget_usd=1.5)
        return [a, b]

    e.start("parent", run_id="p", budget_usd=10)
    e.tick()
    e.tick()
    assert e.store.get_run("p").output == [6, 1.5]


def test_no_child_starts_once_the_budget_is_used_up(make_engine):
    e = make_engine()

    @e.workflow("child")
    def child(ctx):
        return 1

    @e.workflow("parent")
    def parent(ctx):
        ctx.step("spend", lambda: ctx.record_cost(5))
        return ctx.invoke("c", "child")

    e.start("parent", run_id="p", budget_usd=5)
    assert e.store.get_run("p").wait_event == BUDGET_EVENT
    assert e.store.children("p") == []


def test_the_child_timeout_is_its_own(make_engine, clock):
    e = make_engine()

    @e.workflow("slow")
    def slow(ctx):
        ctx.wait_for_event("never", "never")

    @e.workflow("parent")
    def parent(ctx):
        try:
            return ctx.invoke("c", "slow", timeout_s=30)
        except StepFailed as err:
            return str(err)

    e.start("parent", run_id="p")
    e.tick()
    clock.advance(31)
    e.tick()
    out = e.store.get_run("p").output
    assert out.startswith("c: child run p.c cancelled: timed out")


def test_an_unknown_child_workflow_fails_the_parent(make_engine):
    e = make_engine()

    @e.workflow("parent")
    def parent(ctx):
        return ctx.invoke("c", "nope")

    e.start("parent", run_id="p")
    run = e.store.get_run("p")
    assert run.status == "failed"
    assert "workflow not registered: 'nope'" in run.error


@pytest.mark.parametrize("bad", [object(), {1, 2}])
def test_the_child_input_must_be_json(make_engine, bad):
    e = make_engine()

    @e.workflow("child")
    def child(ctx, x):
        return x

    @e.workflow("parent")
    def parent(ctx):
        return ctx.invoke("c", "child", bad)

    e.start("parent", run_id="p")
    assert "must be JSON" in e.store.get_run("p").error
    assert e.store.children("p") == []


def test_show_lists_the_parent_and_its_children(make_engine, capsys):
    from ratchetflow.cli import main

    e = make_engine()
    register(e, [])
    e.start("article", "tides", run_id="a")
    e.tick()
    capsys.readouterr()
    main(["--db", str(e.store.path), "show", "a"])
    assert "child: a.research  research  completed" in capsys.readouterr().out
    main(["--db", str(e.store.path), "show", "a.research"])
    assert "parent: a" in capsys.readouterr().out
