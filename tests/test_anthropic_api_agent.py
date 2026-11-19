"""The Claude API adapter against a fake client: no network and no tokens in the tests."""

import copy
from datetime import datetime

import anthropic
import httpx2
import pytest

from ratchetflow.agents import anthropic_api
from ratchetflow.engine import BUDGET_EVENT
from tests.conftest import Crash

USAGE = {"input_tokens": 1000, "output_tokens": 100, "cache_read_input_tokens": 0,
         "cache_creation_input_tokens": 0}
COST = (1000 * 4 + 100 * 20) / 1e6          # claude-opus-5-5 at $4 / $20 per million


def answer(*content, stop="end_turn"):
    return {"model": "claude-opus-5-5", "content": list(content), "stop_reason": stop,
            "usage": USAGE}


def text(t):
    return {"type": "text", "text": t}


def tool_use(id_, name, input_):
    return {"type": "tool_use", "id": id_, "name": name, "input": input_}


def api_error(cls, status, headers=None):
    request = httpx2.Request("POST", "https://api.anthropic.com/v1/messages")
    response = httpx2.Response(status, headers=headers or {}, request=request)
    return cls("boom", response=response, body=None)


class FakeClient:
    """Plays back answers in order; an exception in the list is raised instead."""

    def __init__(self, *script):
        self.script = list(script)
        self.requests = []
        self.messages = self

    def stream(self, **request):
        self.requests.append(copy.deepcopy(request))
        item = self.script.pop(0)

        class Stream:
            def __enter__(self):
                if isinstance(item, BaseException):
                    raise item
                return self

            def __exit__(self, *exc):
                return False

            def get_final_message(self):
                return item

        return Stream()


GOOD = 'Done.\n```json\n{"steps": ["a", "b"]}\n```'


def check(data):
    assert isinstance(data.get("steps"), list)
    return data


def flow(engine, client, **kw):
    @engine.workflow("w")
    def w(ctx):
        return ctx.step("plan", anthropic_api, ctx, "make the plan", client=client, **kw)


def test_final_answer_validated_with_cost_and_tokens(make_engine):
    client = FakeClient(answer(text(GOOD)))
    e = make_engine()
    flow(e, client, validate=check, system="Be brief.", output_config={"effort": "high"})
    e.start("w", run_id="r")
    run = e.store.get_run("r")
    assert run.status == "completed", run.error
    assert run.output["data"] == {"steps": ["a", "b"]}
    assert e.store.run_cost("r") == pytest.approx(COST)
    assert run.output["usage"] == {"input": 1000, "output": 100, "cache_read": 0, "cache_write": 0}
    req = client.requests[0]
    assert req["model"] == "claude-opus-5-5" and req["system"] == "Be brief."
    assert req["output_config"] == {"effort": "high"}
    assert req["messages"] == [{"role": "user", "content": "make the plan"}]


def test_tools_run_and_their_results_go_back_in_one_message(make_engine):
    client = FakeClient(
        answer(text("Looking."), tool_use("t1", "search", {"q": "x"}),
               tool_use("t2", "missing", {}), tool_use("t3", "broken", {}), stop="tool_use"),
        answer(text(GOOD)))

    def broken(_input):
        raise ValueError("no index")

    e = make_engine()
    flow(e, client, tools=[{"name": "search"}], validate=check,
         handlers={"search": lambda i: {"hits": [i["q"]]}, "broken": broken})
    e.start("w", run_id="r")
    assert e.store.get_run("r").status == "completed"
    results = client.requests[1]["messages"][-1]
    assert results["role"] == "user"
    assert results["content"] == [
        {"type": "tool_result", "tool_use_id": "t1", "content": '{"hits": ["x"]}'},
        {"type": "tool_result", "tool_use_id": "t2", "content": "unknown tool 'missing'",
         "is_error": True},
        {"type": "tool_result", "tool_use_id": "t3", "content": "ValueError: no index",
         "is_error": True},
    ]
    assert e.store.run_cost("r") == pytest.approx(2 * COST)


def test_a_crash_while_tools_run_does_not_pay_for_the_answer_again(make_engine, clock):
    client = FakeClient(answer(tool_use("t1", "publish", {}), stop="tool_use"), answer(text(GOOD)))
    ran = []

    def publish(_input):
        ran.append(1)
        if len(ran) == 1:
            raise Crash()                      # the process dies inside the tool
        return "published"

    e = make_engine(lease_ttl_s=60)
    flow(e, client, tools=[{"name": "publish"}], handlers={"publish": publish})
    with pytest.raises(Crash):
        e.start("w", run_id="r")
    e2 = make_engine()
    flow(e2, client, tools=[{"name": "publish"}], handlers={"publish": publish})
    clock.advance(61)                          # the dead worker's lease runs out
    assert e2.tick() == ["r"]
    assert e2.store.get_run("r").status == "completed"
    assert len(client.requests) == 2           # the answer that asked for the tool: paid once
    assert len(ran) == 2                       # only the tool ran again
    assert e2.store.run_cost("r") == pytest.approx(2 * COST)


def test_an_invalid_answer_asks_for_a_fix_in_the_same_conversation(make_engine):
    client = FakeClient(answer(text("not json")), answer(text(GOOD)))
    e = make_engine()
    flow(e, client, validate=check)
    e.start("w", run_id="r")
    run = e.store.get_run("r")
    assert run.status == "completed"
    second = client.requests[1]["messages"]
    assert [m["role"] for m in second] == ["user", "assistant", "user"]
    assert "not valid" in second[-1]["content"]
    assert run.output["turns"] == 2 and run.output["cost_usd"] == pytest.approx(2 * COST)


def test_pause_turn_goes_on_with_the_answer_so_far(make_engine):
    client = FakeClient(answer(text("searching"), stop="pause_turn"), answer(text("done")))
    e = make_engine()
    flow(e, client, expect_json=False, validate=lambda t: t)
    e.start("w", run_id="r")
    assert e.store.get_run("r").output["data"] == "done"
    assert client.requests[1]["messages"][-1] == {"role": "assistant",
                                                   "content": [text("searching")]}


def test_the_budget_is_checked_before_every_call(make_engine):
    client = FakeClient(answer(tool_use("t1", "search", {}), stop="tool_use"), answer(text("ok")))
    e = make_engine()
    flow(e, client, handlers={"search": lambda i: "x"}, expect_json=False, validate=lambda t: t)
    e.start("w", run_id="r", budget_usd=COST / 2)
    run = e.store.get_run("r")
    assert run.status == "waiting" and run.wait_event == BUDGET_EVENT
    assert len(client.requests) == 1
    e.signal("r", BUDGET_EVENT, {"add_usd": 1})
    e.tick()
    assert e.store.get_run("r").status == "completed"
    assert len(client.requests) == 2           # went on with the same conversation


def test_rate_limit_with_a_long_retry_after_pauses(make_engine):
    client = FakeClient(api_error(anthropic.RateLimitError, 429, {"retry-after": "600"}))
    e = make_engine()
    flow(e, client, now=lambda: datetime(2026, 11, 19, 11, 0))
    e.start("w", run_id="r")
    run = e.store.get_run("r")
    assert run.status == "waiting"
    assert datetime.fromtimestamp(run.wake_at) == datetime(2026, 11, 19, 11, 10)


@pytest.mark.parametrize("error,status", [
    (lambda: api_error(anthropic.RateLimitError, 429, {"retry-after": "5"}), "completed"),
    (lambda: api_error(anthropic.OverloadedError, 529), "completed"),
    (lambda: api_error(anthropic.InternalServerError, 500), "completed"),
    (lambda: anthropic.APIConnectionError(request=httpx2.Request("POST", "https://x.test")),
     "completed"),
    (lambda: api_error(anthropic.BadRequestError, 400), "failed"),
    (lambda: api_error(anthropic.EnforcedSpendLimitReachedError, 429), "failed"),
])
def test_errors_are_retried_or_fatal(make_engine, error, status):
    client = FakeClient(error(), answer(text("ok")))
    e = make_engine()
    flow(e, client, expect_json=False, validate=lambda t: t)
    e.start("w", run_id="r")
    run = e.store.get_run("r")
    assert run.status == status, run.error
    assert len(client.requests) == (2 if status == "completed" else 1)


@pytest.mark.parametrize("stop", ["refusal", "max_tokens"])
def test_refusal_and_max_tokens_are_fatal(make_engine, stop):
    client = FakeClient(answer(text("..."), stop=stop))
    e = make_engine()
    flow(e, client)
    e.start("w", run_id="r")
    assert e.store.get_run("r").status == "failed"
    assert len(client.requests) == 1


def test_messages_open_the_conversation(make_engine):
    client = FakeClient(answer(text("ok")))
    e = make_engine()
    opening = [{"role": "user", "content": "hi"}, {"role": "assistant", "content": "hello"},
               {"role": "user", "content": "go"}]

    @e.workflow("w")
    def w(ctx):
        return ctx.step("chat", anthropic_api, ctx, messages=opening, client=client)

    e.start("w", run_id="r")
    assert client.requests[0]["messages"] == opening


def test_a_crash_after_the_tools_ran_does_not_run_them_again(make_engine, clock):
    client = FakeClient(answer(tool_use("t1", "publish", {}), stop="tool_use"), Crash(),
                        answer(text("ok")))
    ran = []

    def setup(engine):
        flow(engine, client, tools=[{"name": "publish"}], expect_json=False,
             validate=lambda t: t, handlers={"publish": lambda i: ran.append(1) or "published"})

    e = make_engine(lease_ttl_s=60)
    setup(e)
    with pytest.raises(Crash):                 # dies on the call that carries the results
        e.start("w", run_id="r")
    e2 = make_engine()
    setup(e2)
    clock.advance(61)
    assert e2.tick() == ["r"]
    assert e2.store.get_run("r").status == "completed"
    assert ran == [1]
    assert client.requests[2]["messages"][-1]["content"][0]["content"] == "published"
