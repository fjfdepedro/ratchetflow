"""A step that talks to the Claude API directly, with your own tools, through the `anthropic` SDK.

Needs the optional extra: `pip install "ratchetflow[anthropic]"`.

The conversation is the step's state. It is stored in the step's scratch after every answer and
after every round of tool results, so a crash costs at most the call in flight:

- a retry goes on from the last stored message instead of starting over;
- if the process died while your tools were running, only that round of tools runs again (the
  answer that asked for them is not paid for twice). Tools that touch the world should be safe
  to run twice;
- an answer that does not validate is sent back in the same conversation, asking for a fix.

Each answer's cost (from its `usage`, at `PRICES`) goes to the step as soon as it arrives, and the
run's budget is checked before every call: when it runs out, the run waits for `ratchet.budget`
and then goes on with the same conversation.

Errors: 429 -> retried after `retry-after` (a `Pause` when it is longer than a minute); a spend
limit or a payment problem -> `Fatal`; 5xx, 529 and network errors -> `Retryable`; other 4xx ->
`Fatal`. The SDK already retries each call twice before any of this.

Usage:

    def search(input):                    # your tool: gets the input, returns text or JSON
        return index.search(input["query"])

    out = ctx.step("research", anthropic_api, ctx, "Find three sources on X; answer in JSON",
                   tools=[{"name": "search", "description": "Search the index",
                           "input_schema": {"type": "object",
                                            "properties": {"query": {"type": "string"}},
                                            "required": ["query"]}}],
                   handlers={"search": search}, validate=check_sources)
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any

from ..engine import BUDGET_EVENT
from ..errors import Fatal, NeedsHuman, Pause, Retryable
from ._common import Prices, add_usage, validated
from .claude import PRICES, estimate_cost

DEFAULT_MODEL = "claude-opus-5-5"
DEFAULT_FIX_PROMPT = ("Your answer was not valid: {error}. Answer again with the output the first "
                      "message asked for.")

_SPEND_LIMITS = ("EnforcedSpendLimitReachedError", "ContractSpendLimitReachedError",
                 "LinkedOrganizationSpendLimitReachedError", "PaymentRequiredError")


def anthropic_api(ctx: Any, prompt: str | None = None, *,
                  messages: Sequence[dict] | None = None, model: str = DEFAULT_MODEL,
                  system: Any = None, tools: Sequence[dict] | None = None,
                  handlers: Mapping[str, Callable[[dict], Any]] | None = None,
                  max_tokens: int = 64000, max_turns: int = 50,
                  validate: Callable[[Any], Any] | None = None, expect_json: bool = True,
                  fix_prompt: str = DEFAULT_FIX_PROMPT, client: Any = None,
                  now: Callable[[], datetime] = datetime.now,
                  prices: Prices | None = None, **params: Any) -> dict:
    """Runs the conversation to its final answer and returns its text, data, cost and tokens.

    Give either `prompt` or the opening `messages`. `tools` are tool definitions as the API
    takes them (server tools included); `handlers` maps each of your tools' names to a function
    that gets the tool's input and returns text or anything JSON-serialisable (raise to report
    an error to the model). Any other keyword (`thinking`, `output_config`, `metadata`...) goes
    to the request as is. `client` defaults to `anthropic.Anthropic()`.
    """
    if client is None:
        client = _default_client()
    handlers = dict(handlers or {})
    scratch = ctx.scratch
    history: list[dict] = list(scratch.get("messages") or [])
    if not history:
        if (prompt is None) == (messages is None):
            raise Fatal("give either prompt or messages")
        history = ([{"role": "user", "content": prompt}] if prompt is not None
                   else list(messages or []))
    elif scratch.get("answered"):
        # The last answer did not validate: ask for a fix in the same conversation.
        history.append({"role": "user",
                        "content": fix_prompt.format(error=scratch.get("last_error") or "none")})
        ctx.save_scratch(messages=history, answered=False)
    turns = int(scratch.get("turns") or 0)
    usage = scratch.get("usage")
    spent = float(scratch.get("cost_usd") or 0)
    request = {"model": model, "max_tokens": max_tokens, **params}
    if system is not None:
        request["system"] = system
    if tools:
        request["tools"] = list(tools)

    while True:
        pending = _pending_tool_uses(history)
        if pending:
            history.append({"role": "user", "content": [_run_tool(b, handlers) for b in pending]})
            ctx.save_scratch(messages=history)
        if turns >= max_turns:
            raise Fatal(f"the conversation went over {max_turns} turns")
        left = ctx.budget_left() if hasattr(ctx, "budget_left") else None
        if left is not None and left <= 0:
            raise NeedsHuman(BUDGET_EVENT, "the conversation used up the run's budget")

        answer = _call(client, request, history, ctx, now)
        turns += 1
        cost = estimate_cost(answer.get("usage") or {}, answer.get("model") or model,
                             prices or PRICES)
        ctx.record_cost(cost)
        spent += cost
        usage = add_usage(usage, _short_usage(answer.get("usage") or {}))
        history.append({"role": "assistant", "content": answer.get("content") or []})
        stop = answer.get("stop_reason")
        ctx.save_scratch(messages=history, turns=turns, usage=usage, cost_usd=spent,
                         answered=stop not in ("tool_use", "pause_turn"))

        if stop in ("tool_use", "pause_turn"):
            continue
        if stop == "refusal":
            details = answer.get("stop_details") or {}
            raise Fatal(f"the model declined: {details.get('category') or 'no category'}")
        if stop == "max_tokens":
            raise Fatal(f"the answer hit max_tokens ({max_tokens}); raise it")
        break

    text = "".join(b.get("text", "") for b in history[-1]["content"] if b.get("type") == "text")
    data = validated(ctx, text, validate, expect_json)
    return {"text": text, "data": data, "turns": turns, "usage": usage, "models": [model],
            "cost_usd": spent}


def _default_client() -> Any:
    try:
        import anthropic
    except ImportError as e:
        raise Fatal('the Claude API adapter needs: pip install "ratchetflow[anthropic]"') from e
    return anthropic.Anthropic()


def _call(client: Any, request: dict, history: list[dict], ctx: Any,
          now: Callable[[], datetime]) -> dict:
    """One streamed request; the SDK's errors become the engine's."""
    import anthropic

    try:
        with client.messages.stream(messages=history, **request) as stream:
            return _as_dict(stream.get_final_message())
    except anthropic.APIStatusError as e:
        ctx.save_scratch(last_error=f"{e.status_code}: {e.message}"[:400])
        if type(e).__name__ in _SPEND_LIMITS:
            raise Fatal(f"spend limit or billing: {e.message}") from e
        if isinstance(e, anthropic.RateLimitError):
            wait = _retry_after(e)
            if wait > 60:
                raise Pause(now() + timedelta(seconds=wait),
                            reason=f"rate limited for {wait:.0f}s") from e
            raise Retryable(f"rate limited: {e.message}") from e
        if e.status_code >= 500 or e.status_code in (408, 409):
            raise Retryable(f"{e.status_code}: {e.message}") from e
        raise Fatal(f"{e.status_code}: {e.message}") from e
    except anthropic.APIConnectionError as e:
        ctx.save_scratch(last_error=f"connection: {e}"[:400])
        raise Retryable(f"connection: {e}") from e


def _retry_after(e: Any) -> float:
    try:
        return float(e.response.headers.get("retry-after") or 0)
    except (TypeError, ValueError, AttributeError):
        return 0.0


def _as_dict(message: Any) -> dict:
    if isinstance(message, dict):
        return message
    return message.to_dict()


def _pending_tool_uses(history: list[dict]) -> list[dict]:
    """Tool calls in the last answer that have no results yet."""
    last = history[-1]
    if last["role"] != "assistant" or not isinstance(last["content"], list):
        return []
    return [b for b in last["content"] if b.get("type") == "tool_use"]


def _run_tool(block: dict, handlers: Mapping[str, Callable[[dict], Any]]) -> dict:
    result = {"type": "tool_result", "tool_use_id": block["id"]}
    handler = handlers.get(block["name"])
    if handler is None:
        return {**result, "content": f"unknown tool {block['name']!r}", "is_error": True}
    try:
        out = handler(block.get("input") or {})
    except Exception as e:  # noqa: BLE001 — the model sees the error and can adapt
        return {**result, "content": f"{type(e).__name__}: {e}", "is_error": True}
    return {**result, "content": out if isinstance(out, str) else json.dumps(out)}


_USAGE = {"input": "input_tokens", "output": "output_tokens",
          "cache_read": "cache_read_input_tokens", "cache_write": "cache_creation_input_tokens"}


def _short_usage(usage: dict) -> dict:
    return {short: usage.get(key) for short, key in _USAGE.items()}
