"""The `claude -p` adapter against a fake CLI: no real tokens in the tests."""

import json
import os
import stat
import sys
import textwrap
import time
from datetime import datetime

import pytest

from ratchetflow.agents import claude
from ratchetflow.engine import BUDGET_EVENT
from ratchetflow.agents.claude import _reset_time, estimate_cost

FAKE = textwrap.dedent(f"""\
    #!{sys.executable}
    import json, os, subprocess, sys, time
    log = os.environ["FAKE_LOG"]
    with open(log, "a") as f:
        f.write(json.dumps({{"argv": sys.argv[1:], "stdin": sys.stdin.read()}}) + "\\n")
    calls = sum(1 for _ in open(log))
    modes = os.environ["FAKE_MODES"].split(",")
    mode = modes[min(calls, len(modes)) - 1]
    out = lambda ev: (print(json.dumps(ev)), sys.stdout.flush())
    out({{"type": "system", "subtype": "init", "session_id": "s-" + str(calls)}})
    if mode == "die":
        os._exit(1)
    if mode == "hang":
        child = subprocess.Popen(["sleep", "300"])
        open(os.environ["FAKE_LOG"] + ".child", "w").write(str(child.pid))
        time.sleep(300)
    if mode == "quota":
        out({{"type": "result", "is_error": True, "session_id": "s-1",
             "result": "You've hit your session limit · resets 1:30pm"}})
        sys.exit(1)
    if mode == "overbudget":
        out({{"type": "result", "is_error": True, "subtype": "error_max_budget_usd",
             "result": None, "total_cost_usd": 0.5}})
        sys.exit(1)
    if mode == "partial":
        msg = {{"id": "m1", "model": "claude-opus-5-5", "usage": {{
            "input_tokens": 1000, "output_tokens": 0, "cache_read_input_tokens": 0,
            "cache_creation_input_tokens": 2000,
            "cache_creation": {{"ephemeral_5m_input_tokens": 0, "ephemeral_1h_input_tokens": 2000}}}}}}
        out({{"type": "assistant", "message": msg}})
        out({{"type": "assistant", "message": msg}})   # one event per content block
        os._exit(1)
    if mode == "noisy":
        sys.stderr.write("x" * 300_000)
        sys.stderr.flush()
    if mode == "latin1":
        sys.stdout.flush()
        sys.stdout.buffer.write(b"\\xff\\xfe not utf-8\\n")
        sys.stdout.buffer.flush()
    if mode == "junk":
        out({{"type": "result", "is_error": False, "result": "not json", "total_cost_usd": 0.5}})
        sys.exit(0)
    out({{"type": "result", "is_error": False, "num_turns": 3, "total_cost_usd": 1.25,
         "result": "Done.\\n```json\\n{{\\"steps\\": [\\"a\\", \\"b\\"]}}\\n```"}})
    """)


@pytest.fixture
def fake(tmp_path, monkeypatch):
    bin_ = tmp_path / "claude-fake"
    bin_.write_text(FAKE)
    bin_.chmod(bin_.stat().st_mode | stat.S_IEXEC)
    log = tmp_path / "calls.jsonl"
    monkeypatch.setenv("FAKE_LOG", str(log))

    def set_modes(*modes):
        monkeypatch.setenv("FAKE_MODES", ",".join(modes))

    def calls():
        return [json.loads(l) for l in log.read_text().splitlines()] if log.exists() else []

    return str(bin_), set_modes, calls, log


def check(data):
    assert isinstance(data.get("steps"), list)
    return data


def flow(engine, bin_, **kw):
    @engine.workflow("w")
    def w(ctx):
        return ctx.step("plan", claude, ctx, "make the plan", bin=bin_, validate=check, **kw)


def test_validated_output_and_cost_on_the_step(make_engine, fake):
    bin_, modes, calls, _ = fake
    modes("ok")
    e = make_engine()
    flow(e, bin_)
    e.start("w", run_id="r")
    run = e.store.get_run("r")
    assert run.status == "completed"
    assert run.output["data"] == {"steps": ["a", "b"]}
    assert e.store.run_cost("r") == 1.25
    assert calls()[0]["stdin"] == "make the plan"


def test_if_it_dies_halfway_the_retry_resumes_the_session(make_engine, fake):
    bin_, modes, calls, _ = fake
    modes("die", "ok")
    e = make_engine()
    flow(e, bin_)
    e.start("w", run_id="r")
    assert e.store.get_run("r").status == "completed"
    second = calls()[1]
    assert second["argv"][-2:] == ["--resume", "s-1"]
    assert "do not redo what is already done" in second["stdin"]


def test_invalid_output_is_retried_asking_for_a_fix(make_engine, fake):
    bin_, modes, calls, _ = fake
    modes("junk", "ok")
    e = make_engine()
    flow(e, bin_)
    e.start("w", run_id="r")
    assert e.store.get_run("r").status == "completed"
    assert "invalid output" in calls()[1]["stdin"]
    assert e.store.run_cost("r") == 1.75          # both attempts are paid for


def test_quota_used_up_pauses_until_the_reset(make_engine, fake):
    bin_, modes, _, _ = fake
    modes("quota")
    e = make_engine()
    flow(e, bin_, now=lambda: datetime(2026, 11, 18, 11, 0))
    e.start("w", run_id="r")
    run = e.store.get_run("r")
    assert run.status == "waiting"
    assert datetime.fromtimestamp(run.wake_at) == datetime(2026, 11, 18, 13, 32)


def test_timeout_kills_the_process_group(make_engine, fake):
    bin_, modes, _, log = fake
    modes("hang")
    e = make_engine()
    flow(e, bin_, timeout_s=1)
    t0 = time.monotonic()
    e.start("w", run_id="r")
    assert time.monotonic() - t0 < 60
    assert e.store.get_run("r").status == "failed"   # 3 timeouts = 3 attempts
    child = int(open(str(log) + ".child").read())
    time.sleep(0.2)
    with pytest.raises(ProcessLookupError):
        os.kill(child, 0)                           # the `sleep 300` grandchild died too


@pytest.mark.parametrize("mode", ["noisy", "latin1"])
def test_a_flood_on_stderr_or_odd_bytes_do_not_hang_the_step(make_engine, fake, mode):
    bin_, modes, _, _ = fake
    modes(mode)
    e = make_engine()
    flow(e, bin_, timeout_s=5, retries=1)
    e.start("w", run_id="r")
    run = e.store.get_run("r")
    assert run.status == "completed", run.error


@pytest.mark.parametrize("text,expected", [
    ("resets 1:30pm", datetime(2026, 11, 18, 13, 32)),
    ("resets 9am", datetime(2026, 11, 19, 9, 2)),       # already past today -> tomorrow
    ("resets at 23:15", datetime(2026, 11, 18, 23, 17)),
    ("no time", datetime(2026, 11, 18, 11, 15)),
])
def test_reset_time(text, expected):
    assert _reset_time(text, datetime(2026, 11, 18, 11, 0)) == expected


def test_a_cut_off_attempt_bills_its_estimated_cost(make_engine, fake):
    bin_, modes, _, _ = fake
    modes("partial", "ok")
    e = make_engine()
    flow(e, bin_)
    e.start("w", run_id="r")
    assert e.store.get_run("r").status == "completed"
    # 1000 input at $4/MTok + 2000 written to the 1-hour cache at 2x = $0.02, counted once
    assert e.store.run_cost("r") == pytest.approx(0.02 + 1.25)


def test_an_estimate_left_by_a_dead_process_is_billed_on_the_next_attempt(fake):
    bin_, modes, _, _ = fake
    modes("ok")

    class Ctx:
        def __init__(self):
            self.scratch = {"unbilled_usd": 0.3}
            self.costs = []

        def save_scratch(self, **kw):
            self.scratch.update(kw)

        def record_cost(self, usd):
            self.costs.append(usd)

    ctx = Ctx()
    claude(ctx, "make the plan", bin=bin_, validate=check)
    assert ctx.costs == [0.3, 1.25] and ctx.scratch["unbilled_usd"] == 0


def test_the_estimate_matches_what_the_cli_reported():
    # Totals of a real Haiku 5.5 session; the CLI reported total_cost_usd 0.00511279.
    usage = {"input_tokens": 4, "output_tokens": 124, "cache_read_input_tokens": 51199,
             "cache_creation_input_tokens": 22692,
             "cache_creation": {"ephemeral_5m_input_tokens": 0, "ephemeral_1h_input_tokens": 22692}}
    assert estimate_cost(usage, "claude-haiku-5-5") == pytest.approx(0.00511279)


def test_an_unknown_model_is_priced_at_the_top():
    usage = {"input_tokens": 1_000_000}
    assert estimate_cost(usage, "claude-future-9") == estimate_cost(usage, "claude-fable-5-1")


def test_text_output_goes_to_validate_as_text(make_engine, fake):
    bin_, modes, _, _ = fake
    modes("ok")
    e = make_engine()

    def wrote_it(text):
        assert text.startswith("Done.")
        return {"checked": True}

    @e.workflow("w")
    def w(ctx):
        return ctx.step("write", claude, ctx, "write the files", bin=bin_,
                        validate=wrote_it, expect_json=False)

    e.start("w", run_id="r")
    run = e.store.get_run("r")
    assert run.status == "completed" and run.output["data"] == {"checked": True}


def test_the_session_is_capped_at_the_budget_left(make_engine, fake):
    bin_, modes, calls, _ = fake
    modes("ok")
    e = make_engine()

    @e.workflow("w")
    def w(ctx):
        ctx.step("before", lambda: ctx.record_cost(1.5))
        return ctx.step("plan", claude, ctx, "make the plan", bin=bin_, validate=check)

    e.start("w", run_id="r", budget_usd=5)
    argv = calls()[0]["argv"]
    assert float(argv[argv.index("--max-budget-usd") + 1]) == pytest.approx(3.5)


def test_a_session_that_hits_its_budget_waits_for_more(make_engine, fake):
    bin_, modes, calls, _ = fake
    modes("overbudget", "ok")
    e = make_engine()
    flow(e, bin_)
    e.start("w", run_id="r", budget_usd=10)
    run = e.store.get_run("r")
    assert run.status == "waiting" and run.wait_event == BUDGET_EVENT
    assert len(calls()) == 1                       # no retry burning money
    e.signal("r", BUDGET_EVENT, {"add_usd": 5})
    e.tick()
    assert e.store.get_run("r").status == "completed"
    assert calls()[1]["argv"][-2:] == ["--resume", "s-1"]
