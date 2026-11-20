"""The `codex exec` adapter against a fake CLI: no real tokens in the tests."""

import json
import stat
import sys
import textwrap
from datetime import datetime

import pytest

from ratchetflow.agents import codex

FAKE = textwrap.dedent(f"""\
    #!{sys.executable}
    import fcntl, json, os, sys
    log = os.environ["FAKE_LOG"]
    argv = sys.argv[1:]
    schema = None
    if "--output-schema" in argv:
        schema = json.load(open(argv[argv.index("--output-schema") + 1]))
    with open(log, "a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        f.write(json.dumps({{"argv": argv, "stdin": sys.stdin.read(), "schema": schema}}) + "\\n")
        f.flush()
        calls = sum(1 for _ in open(log))
    modes = os.environ["FAKE_MODES"].split(",")
    mode = modes[min(calls, len(modes)) - 1]
    out = lambda ev: (print(json.dumps(ev)), sys.stdout.flush())
    if "resume" not in argv:
        out({{"type": "thread.started", "thread_id": "t-" + str(calls)}})
    out({{"type": "turn.started"}})
    if mode == "die":
        os._exit(1)
    if mode == "limit":
        msg = "You've hit your usage limit. Try again at 3:45 PM."
        out({{"type": "error", "message": msg}})
        out({{"type": "turn.failed", "error": {{"message": msg}}}})
        sys.exit(1)
    if mode == "limit_date":  # the real message, when the reset is days away
        msg = ("You\u2019ve hit your usage limit. To continue using Codex, start a free trial of "
               "Plus today (https://chatgpt.com/explore/plus), or try again at Nov 27th, 2026 "
               "8:31 AM.")
        out({{"type": "error", "message": msg}})
        out({{"type": "turn.failed", "error": {{"message": msg}}}})
        sys.exit(1)
    if mode == "failed":
        err = {{"message": "stream disconnected before completion"}}
        out({{"type": "turn.failed", "error": err}})
        sys.exit(1)
    text = "not json" if mode == "junk" else 'Done.\\n```json\\n{{"steps": ["a", "b"]}}\\n```'
    thought = {{"id": "i0", "type": "reasoning", "text": "thinking"}}
    out({{"type": "item.completed", "item": thought}})
    out({{"type": "item.completed", "item": {{"id": "i1", "type": "agent_message", "text": text}}}})
    out({{"type": "turn.completed", "usage": {{"input_tokens": 1000, "cached_input_tokens": 800,
                                              "output_tokens": 50,
                                              "reasoning_output_tokens": 20}}}})
    """)


@pytest.fixture
def fake(tmp_path, monkeypatch):
    bin_ = tmp_path / "codex-fake"
    bin_.write_text(FAKE)
    bin_.chmod(bin_.stat().st_mode | stat.S_IEXEC)
    log = tmp_path / "calls.jsonl"
    monkeypatch.setenv("FAKE_LOG", str(log))

    def set_modes(*modes):
        monkeypatch.setenv("FAKE_MODES", ",".join(modes))

    def calls():
        return [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []

    return str(bin_), set_modes, calls


def check(data):
    assert isinstance(data.get("steps"), list)
    return data


def flow(engine, bin_, **kw):
    @engine.workflow("w")
    def w(ctx):
        return ctx.step("plan", codex, ctx, "make the plan", bin=bin_, **kw)


def test_validated_output_and_tokens_without_a_cost(make_engine, fake):
    bin_, modes, calls = fake
    modes("ok")
    e = make_engine()
    flow(e, bin_, validate=check, model="gpt-5.5", sandbox="read-only", cwd="/tmp")
    e.start("w", run_id="r", budget_usd=1)
    run = e.store.get_run("r")
    assert run.status == "completed", run.error
    assert run.output["data"] == {"steps": ["a", "b"]}
    assert run.output["session_id"] == "t-1"
    assert run.output["usage"] == {"input": 200, "cache_read": 800, "output": 50, "cache_write": 0}
    assert e.store.run_cost("r") == 0                      # a subscription: nothing billed
    first = calls()[0]
    assert first["argv"] == ["exec", "--json", "--model", "gpt-5.5", "--sandbox", "read-only",
                             "--cd", "/tmp", "-"]
    assert first["stdin"] == "make the plan"


def test_with_prices_the_tokens_count_towards_the_budget(make_engine, fake):
    bin_, modes, _ = fake
    modes("ok")
    e = make_engine()
    flow(e, bin_, model="gpt-5.5", prices={"gpt-5": (2.0, 10.0, 0.5)})
    e.start("w", run_id="r")
    # 200 input at $2 + 800 cached at $0.5 + 50 output at $10, per million
    assert e.store.run_cost("r") == pytest.approx((200 * 2 + 800 * 0.5 + 50 * 10) / 1e6)


def test_if_it_dies_halfway_the_retry_resumes_the_thread(make_engine, fake):
    bin_, modes, calls = fake
    modes("die", "ok")
    e = make_engine()
    flow(e, bin_, validate=check)
    e.start("w", run_id="r")
    assert e.store.get_run("r").status == "completed"
    second = calls()[1]
    assert second["argv"][-3:] == ["resume", "t-1", "-"]
    assert "do not redo what is already done" in second["stdin"]


def test_invalid_output_is_retried_in_the_same_thread(make_engine, fake):
    bin_, modes, calls = fake
    modes("junk", "ok")
    e = make_engine()
    flow(e, bin_, validate=check)
    e.start("w", run_id="r")
    run = e.store.get_run("r")
    assert run.status == "completed"
    assert "invalid output" in calls()[1]["stdin"]
    assert calls()[1]["argv"][-3:] == ["resume", "t-1", "-"]
    assert run.output["usage"]["output"] == 100             # both attempts' tokens


def test_usage_limit_pauses_until_try_again(make_engine, fake):
    bin_, modes, calls = fake
    modes("limit")
    e = make_engine()
    flow(e, bin_, now=lambda: datetime(2026, 11, 19, 11, 0))
    e.start("w", run_id="r")
    run = e.store.get_run("r")
    assert run.status == "waiting"
    assert datetime.fromtimestamp(run.wake_at) == datetime(2026, 11, 19, 15, 47)
    assert len(calls()) == 1


def test_usage_limit_days_away_waits_for_that_date(make_engine, fake):
    bin_, modes, calls = fake
    modes("limit_date")
    e = make_engine()
    flow(e, bin_, now=lambda: datetime(2026, 11, 20, 11, 0))
    e.start("w", run_id="r")
    run = e.store.get_run("r")
    assert run.status == "waiting"
    assert datetime.fromtimestamp(run.wake_at) == datetime(2026, 11, 27, 8, 33)
    assert len(calls()) == 1


def test_a_failed_turn_is_retried(make_engine, fake):
    bin_, modes, calls = fake
    modes("failed", "ok")
    e = make_engine()
    flow(e, bin_)
    e.start("w", run_id="r")
    assert e.store.get_run("r").status == "completed"
    assert calls()[1]["argv"][-3:] == ["resume", "t-1", "-"]
    assert "stream disconnected" in calls()[1]["stdin"]


def test_output_schema_is_handed_to_codex_and_its_answer_parsed(make_engine, fake):
    bin_, modes, calls = fake
    modes("ok")
    e = make_engine()
    schema = {"type": "object", "properties": {"steps": {"type": "array"}}}
    flow(e, bin_, output_schema=schema)
    e.start("w", run_id="r")
    run = e.store.get_run("r")
    assert run.output["data"] == {"steps": ["a", "b"]}
    assert calls()[0]["schema"] == schema
    path = calls()[0]["argv"][calls()[0]["argv"].index("--output-schema") + 1]
    assert not __import__("os").path.exists(path)          # the temp file is cleaned up


def test_a_session_that_ends_without_finishing_its_turn_is_not_a_success(make_engine, fake):
    bin_, modes, calls = fake
    modes("die", "ok")
    e = make_engine()
    flow(e, bin_)                                           # nothing to validate
    e.start("w", run_id="r")
    assert len(calls()) == 2
    assert "without finishing the turn" in calls()[1]["stdin"]
