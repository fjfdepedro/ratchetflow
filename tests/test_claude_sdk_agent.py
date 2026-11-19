"""The Agent SDK adapter against a fake `query`: no CLI, no network and no tokens.

Runs only where the `agent-sdk` dependency group is installed (`uv sync --group agent-sdk`):
the messages are the SDK's own types.
"""

import asyncio
import time
from datetime import datetime

import pytest

sdk = pytest.importorskip("claude_agent_sdk")

from ratchetflow.agents import claude_sdk  # noqa: E402
from ratchetflow.engine import BUDGET_EVENT  # noqa: E402

GOOD = 'Done.\n```json\n{"steps": ["a", "b"]}\n```'


def init(sid):
    return sdk.SystemMessage(subtype="init", data={"session_id": sid})


def assistant(sid, msg_id="m1", usage=None):
    return sdk.AssistantMessage(content=[sdk.TextBlock(text="working")], model="claude-opus-5-5",
                                usage=usage, message_id=msg_id, session_id=sid)


def result(sid, text=GOOD, *, error=False, subtype="success", cost=1.25, **kw):
    return sdk.ResultMessage(subtype=subtype, duration_ms=10, duration_api_ms=5, is_error=error,
                             num_turns=3, session_id=sid, total_cost_usd=cost, result=text,
                             usage={"input_tokens": 2, "output_tokens": 50,
                                    "cache_read_input_tokens": 900,
                                    "cache_creation_input_tokens": 0},
                             model_usage={"claude-opus-5-5": {}}, **kw)


class FakeQuery:
    """Each call plays the next script: messages to yield, then an exception or the end."""

    def __init__(self, *scripts):
        self.scripts = list(scripts)
        self.calls = []

    def __call__(self, *, prompt, options):
        self.calls.append({"prompt": prompt, "options": options})
        script = self.scripts.pop(0)

        async def gen():
            for item in script:
                if isinstance(item, BaseException):
                    raise item
                if item == "hang":
                    await asyncio.sleep(20)
                yield item

        return gen()


def check(data):
    assert isinstance(data.get("steps"), list)
    return data


def flow(engine, query, **kw):
    @engine.workflow("w")
    def w(ctx):
        return ctx.step("plan", claude_sdk, ctx, "make the plan", query=query, **kw)


def test_validated_output_cost_tokens_and_models(make_engine):
    q = FakeQuery([init("s-1"), assistant("s-1"), result("s-1")])
    e = make_engine()
    flow(e, q, validate=check)
    e.start("w", run_id="r")
    run = e.store.get_run("r")
    assert run.status == "completed", run.error
    assert run.output["data"] == {"steps": ["a", "b"]}
    assert run.output["models"] == ["claude-opus-5-5"]
    assert run.output["usage"] == {"input": 2, "output": 50, "cache_read": 900, "cache_write": 0}
    assert e.store.run_cost("r") == 1.25
    assert q.calls[0]["prompt"] == "make the plan" and q.calls[0]["options"].resume is None


def test_options_are_copied_and_capped_at_the_budget_left(make_engine):
    q = FakeQuery([result("s-1")])
    mine = sdk.ClaudeAgentOptions(cwd="/tmp", max_turns=7)
    e = make_engine()

    @e.workflow("w")
    def w(ctx):
        ctx.step("before", lambda: ctx.record_cost(1.5))
        return ctx.step("plan", claude_sdk, ctx, "go", options=mine, query=q)

    e.start("w", run_id="r", budget_usd=5)
    sent = q.calls[0]["options"]
    assert sent.max_budget_usd == pytest.approx(3.5)
    assert sent.cwd == "/tmp" and sent.max_turns == 7
    assert mine.max_budget_usd is None                     # the caller's options are untouched


def test_if_it_dies_halfway_the_retry_resumes_the_session(make_engine):
    q = FakeQuery([init("s-1"), sdk.ProcessError("exit 1", exit_code=1)], [result("s-1")])
    e = make_engine()
    flow(e, q, validate=check)
    e.start("w", run_id="r")
    assert e.store.get_run("r").status == "completed"
    assert q.calls[1]["options"].resume == "s-1"
    assert "do not redo what is already done" in q.calls[1]["prompt"]


def test_invalid_output_is_retried_in_the_same_session(make_engine):
    q = FakeQuery([init("s-1"), result("s-1", "not json", cost=0.5)], [result("s-1")])
    e = make_engine()
    flow(e, q, validate=check)
    e.start("w", run_id="r")
    assert e.store.get_run("r").status == "completed"
    assert "invalid output" in q.calls[1]["prompt"]
    assert e.store.run_cost("r") == 1.75


def test_a_cut_off_attempt_bills_its_estimated_cost(make_engine):
    usage = {"input_tokens": 1000, "output_tokens": 0, "cache_read_input_tokens": 0,
             "cache_creation_input_tokens": 0}
    q = FakeQuery([init("s-1"), assistant("s-1", usage=usage), assistant("s-1", usage=usage),
                   sdk.CLIConnectionError("lost")], [result("s-1")])
    e = make_engine()
    flow(e, q)
    e.start("w", run_id="r")
    assert e.store.get_run("r").status == "completed"
    assert e.store.run_cost("r") == pytest.approx(1000 * 4 / 1e6 + 1.25)   # counted once


def test_a_rejected_rate_limit_pauses_until_its_reset(make_engine):
    resets = int(datetime(2026, 11, 19, 15, 0).timestamp())
    limit = sdk.RateLimitEvent(rate_limit_info=sdk.RateLimitInfo(status="rejected",
                                                                 resets_at=resets),
                               uuid="u", session_id="s-1")
    q = FakeQuery([init("s-1"), limit, result("s-1", "limit", error=True, cost=0),
                   sdk.ResultError("failed", data={"subtype": "success"})])
    e = make_engine()
    flow(e, q)
    e.start("w", run_id="r")
    run = e.store.get_run("r")
    assert run.status == "waiting"
    assert datetime.fromtimestamp(run.wake_at) == datetime(2026, 11, 19, 15, 2)


def test_quota_text_pauses_as_with_the_cli(make_engine):
    q = FakeQuery([result("s-1", "You've hit your session limit · resets 1:30pm", error=True,
                          cost=0)])
    e = make_engine()
    flow(e, q, now=lambda: datetime(2026, 11, 19, 11, 0))
    e.start("w", run_id="r")
    run = e.store.get_run("r")
    assert run.status == "waiting"
    assert datetime.fromtimestamp(run.wake_at) == datetime(2026, 11, 19, 13, 32)


def test_a_session_that_hits_its_budget_waits_for_more(make_engine):
    over = result("s-1", None, error=True, subtype="error_max_budget_usd", cost=0.5)
    q = FakeQuery([init("s-1"), over], [result("s-1")])
    e = make_engine()
    flow(e, q)
    e.start("w", run_id="r", budget_usd=10)
    run = e.store.get_run("r")
    assert run.status == "waiting" and run.wait_event == BUDGET_EVENT
    assert len(q.calls) == 1
    e.signal("r", BUDGET_EVENT, {"add_usd": 5})
    e.tick()
    assert e.store.get_run("r").status == "completed"
    assert q.calls[1]["options"].resume == "s-1"


def test_max_turns_is_fatal_and_a_missing_cli_too(make_engine):
    q = FakeQuery([result("s-1", None, error=True, subtype="error_max_turns", cost=0.1)])
    e = make_engine()
    flow(e, q)
    e.start("w", run_id="r")
    assert e.store.get_run("r").status == "failed" and len(q.calls) == 1

    q2 = FakeQuery([sdk.CLINotFoundError()])
    e2 = make_engine()

    @e2.workflow("w2")
    def w2(ctx):
        return ctx.step("plan", claude_sdk, ctx, "go", query=q2)

    e2.start("w2", run_id="r2")
    assert e2.store.get_run("r2").status == "failed" and len(q2.calls) == 1


def test_structured_output_goes_to_validate(make_engine):
    q = FakeQuery([result("s-1", "", structured_output={"steps": ["x"]})])
    e = make_engine()
    flow(e, q, validate=check)
    e.start("w", run_id="r")
    assert e.store.get_run("r").output["data"] == {"steps": ["x"]}


def test_timeout_stops_the_session(make_engine):
    q = FakeQuery([init("s-1"), "hang"], [result("s-1")])
    e = make_engine()
    flow(e, q, timeout_s=1)
    t0 = time.monotonic()
    e.start("w", run_id="r")
    assert time.monotonic() - t0 < 10
    assert e.store.get_run("r").status == "completed"
    assert q.calls[1]["options"].resume == "s-1"


USAGE_1K = {"input_tokens": 1000, "output_tokens": 0, "cache_read_input_tokens": 0,
            "cache_creation_input_tokens": 0}


@pytest.mark.parametrize("end", [sdk.CLIConnectionError("lost"), "hang"])
def test_the_last_attempt_cut_off_still_bills_its_estimate(make_engine, end):
    q = FakeQuery([init("s-1"), assistant("s-1", usage=USAGE_1K), end])
    e = make_engine()
    flow(e, q, timeout_s=1, retries=1)
    e.start("w", run_id="r")
    assert e.store.get_run("r").status == "failed"
    assert e.store.run_cost("r") == pytest.approx(1000 * 4 / 1e6)
