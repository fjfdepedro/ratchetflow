"""`ratchet report`: one run as a self-contained HTML page."""

import pytest

from ratchetflow import NeedsHuman, Retryable
from ratchetflow.cli import main
from ratchetflow.report import FOLD_S, _Axis, _bars, _stages, render
from ratchetflow.store import RunRow, StepRow


def _row(seq, name, start, end, busy=None, scratch=None, output=None):
    return StepRow("r", seq, name, "done", 1, output, None, scratch or {}, 0.0, start, end, busy)


RUN = RunRow("r", "w", None, "completed", None, None, None, None, None, None, None, None, 0, 0)


def test_a_run_with_a_person_a_retry_and_costs(make_engine, clock):
    e = make_engine()
    tries = []

    @e.workflow("article")
    def article(ctx, topic):
        def plan():
            clock.advance(40)
            ctx.record_cost(0.5)
            return {"sections": 2}

        def approval():
            if ctx.scratch.get("signal") is None:
                raise NeedsHuman("plan.ok", "go on with the plan?", questions=["is 2 enough?"])
            return ctx.scratch["signal"]

        def write():
            clock.advance(20)
            tries.append(1)
            if len(tries) == 1:
                raise Retryable("rate limited")
            ctx.record_cost(1.25)
            return "draft"

        ctx.step("plan", plan)
        ctx.step("approval", approval)
        return ctx.step("write", write)

    e.start("article", "<script>alert(1)</script>", run_id="a1", budget_usd=2)
    waiting = render(e.store, "a1", now=clock())
    assert "Waiting for you." in waiting and "is 2 enough?" in waiting
    assert "ratchet signal a1 plan.ok" in waiting

    clock.advance(3 * 3600)            # the person answers three hours later
    e.signal("a1", "plan.ok", {"go": True})
    e.tick()
    page = render(e.store, "a1", now=clock())

    assert "<script>alert" not in page and "&lt;script&gt;" in page
    assert 'class="node human st-done"' in page
    assert "×2 attempts" in page and "$1.75 / $2.00" in page
    assert ">3.0h</text>" in page       # the wait for the person is folded, with its length
    assert "Waiting for you." not in page and "<svg" in page
    # Nothing is loaded from the network.
    assert "http://" not in page and "https://" not in page and "<script" not in page


def test_a_failed_run_says_how_to_retry(make_engine):
    e = make_engine()

    @e.workflow("w")
    def w(ctx):
        return ctx.step("boom", lambda: 1 / 0, retries=1)

    e.start("w", run_id="f")
    page = render(e.store, "f")
    assert "Failed." in page and "ratchet retry f" in page
    assert 'class="node step st-failed"' in page


def test_overlapping_steps_share_a_stage_and_the_next_one_waits():
    steps = [_row(1, "draft", 0, 10), _row(2, "review-a", 10, 30), _row(3, "review-b", 10.2, 25),
             _row(4, "publish", 30, 31)]
    stages = _stages(_bars(RUN, steps, now=31))
    assert [[b.step.name for b in st] for st in stages] == [
        ["draft"], ["review-a", "review-b"], ["publish"]]


def test_old_rows_without_busy_time_draw_a_gate_as_waiting():
    gate = _row(1, "ok", 0, 600, scratch={"question": "go on?", "signal": {}})
    work = _row(2, "work", 600, 660)
    gate_bar, work_bar = _bars(RUN, [gate, work], now=660)
    assert gate_bar.busy_end == 0 and work_bar.busy_end == 660


def test_the_axis_folds_quiet_stretches_only():
    steps = [_row(1, "a", 0, 10, busy=10), _row(2, "gate", 10, 10 + 5000, busy=0),
             _row(3, "b", 5010, 5020, busy=10)]
    axis = _Axis(_bars(RUN, steps, now=5020), 0, 5020, 700)
    assert len(axis.folds) == 1 and axis.folds[0][1] == pytest.approx(5000)
    # Working time keeps its proportions: 10 s before the fold, 10 s after it.
    assert axis(10) - axis(0) == pytest.approx(axis(5020) - axis(5010))
    short = [_row(1, "a", 0, 10, busy=10), _row(2, "b", 10 + FOLD_S / 2, 30, busy=10)]
    assert _Axis(_bars(RUN, short, now=30), 0, 30, 700).folds == []


def test_cli_writes_the_page(make_engine, tmp_path, capsys):
    e = make_engine()

    @e.workflow("w")
    def w(ctx):
        return ctx.step("x", lambda: 1)

    e.start("w", run_id="r1")
    db = str(tmp_path / "r.db")
    out = tmp_path / "page.html"
    assert main(["--db", db, "report", "r1", "-o", str(out)]) == 0
    assert out.read_text().startswith("<!doctype html>")
    assert main(["--db", db, "report", "r1", "-o", "-"]) == 0
    assert "</html>" in capsys.readouterr().out
    with pytest.raises(SystemExit, match="no run nope"):
        main(["--db", db, "report", "nope"])


def test_a_cancelled_run_says_so_and_lists_its_cleanup(make_engine):
    e = make_engine()

    @e.workflow("w", cleanup=lambda end: "torn down")
    def w(ctx):
        ctx.step("a", lambda: 1)
        ctx.wait_for_event("ok", "ok")

    e.start("w", run_id="r")
    e.cancel("r", "wrong topic")
    e.tick()
    page = render(e.store, "r")
    assert "<b>Cancelled.</b> wrong topic" in page
    assert "ratchet.cleanup" in page
    assert "ratchet retry" not in page
