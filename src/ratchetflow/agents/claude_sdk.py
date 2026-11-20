"""A step that is a Claude Agent SDK session (`claude_agent_sdk.query`).

Needs the optional extra: `pip install "ratchetflow[agent-sdk]"`.

It gives the same guarantees as `claude()`, plus what only the SDK has: hooks, `can_use_tool`,
your own tools as in-process MCP servers, and every other `ClaudeAgentOptions` field.

- Stores the session id as soon as a message carries it; a retry resumes the session
  (`options.resume`) with a prompt to check what is done and finish.
- Rate limit rejected -> `Pause` until the reset the CLI reports; quota text or 429 -> `Pause`
  as in `claude()`; overload -> `Retryable`; `max_budget_usd` reached -> waits for
  `ratchet.budget`, then resumes the session; `max_turns` reached -> `Fatal`.
- `max_budget_usd` defaults to what is left of the run's budget. Cost comes from the result's
  `total_cost_usd`; an attempt cut off before it is billed from the usage seen so far.
- With `output_format` in the options, `validate` gets the `structured_output`; otherwise it
  gets the JSON extracted from the answer (or the text, with `expect_json=False`).

Usage:

    from claude_agent_sdk import ClaudeAgentOptions

    out = ctx.step("review", claude_sdk, ctx, "Review the draft; answer in JSON",
                   options=ClaudeAgentOptions(cwd=folder, allowed_tools=["Read"]),
                   validate=check_review)
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

from ..errors import Fatal, Pause, Retryable
from ._common import DEFAULT_RESUME_PROMPT, Prices, add_usage, bill_unbilled, sum_tokens, validated
from .claude import PRICES, _raise_for_error, _short_usage, estimate_cost


def claude_sdk(ctx: Any, prompt: str, *, options: Any = None, timeout_s: float = 1800,
               validate: Callable[[Any], Any] | None = None, expect_json: bool = True,
               resume_prompt: str = DEFAULT_RESUME_PROMPT,
               now: Callable[[], datetime] = datetime.now,
               prices: Prices | None = None, query: Callable[..., Any] | None = None) -> dict:
    """Runs one Agent SDK session as a step and returns its text, data, session and cost.

    `options` is a `ClaudeAgentOptions` (never modified: the step works on a copy). `query`
    replaces `claude_agent_sdk.query`, for tests.
    """
    sdk = _sdk()
    query = query or sdk.query
    scratch = ctx.scratch
    bill_unbilled(ctx, scratch.get("unbilled_usd"))  # an earlier attempt died with the process
    options = options or sdk.ClaudeAgentOptions()
    changes: dict = {}
    if options.max_budget_usd is None and hasattr(ctx, "budget_left"):
        left = ctx.budget_left()
        if left is not None:
            changes["max_budget_usd"] = max(left, 0.01)
    session_id = scratch.get("session_id")
    if session_id:
        changes["resume"] = session_id
        prompt = resume_prompt.format(error=scratch.get("last_error") or "none")
    options = dataclasses.replace(options, **changes)
    stop = getattr(ctx, "cancelled", None) or (lambda: False)

    try:
        seen = asyncio.run(_drive(sdk, query, prompt, options, ctx, timeout_s, stop,
                                  prices or PRICES))
    except BaseException:
        try:  # cut off before the result: bill the estimate now
            bill_unbilled(ctx, ctx.scratch.get("unbilled_usd"))
        except BaseException:  # noqa: BLE001 — never hide the original error
            pass
        raise

    result = seen["result"]
    if result is None:
        error = seen["error"]
        text = f"{type(error).__name__}: {error}" if error else "the session ended without a result"
        ctx.save_scratch(last_error=text[:400])
        bill_unbilled(ctx, ctx.scratch.get("unbilled_usd"))  # this may be the last attempt
        if isinstance(error, sdk.CLINotFoundError):
            raise Fatal(text)
        raise Retryable(text[:200])

    # Bill first, then clear: if we die in between, the estimate is billed twice.
    if result.total_cost_usd:
        ctx.record_cost(float(result.total_cost_usd))
    tokens = _short_usage(result.usage or {})
    if not any(tokens.values()):  # a session cut by its budget reports no tokens: count ours
        tokens = seen["tokens"]
    usage = add_usage(ctx.scratch.get("usage"), tokens)
    models = sorted({*(ctx.scratch.get("models") or []), *(result.model_usage or {})})
    ctx.save_scratch(unbilled_usd=0, usage=usage, models=models)
    text = result.result or ""
    if result.is_error:
        rejected = seen["rate_limit_reset"]
        if rejected:
            ctx.save_scratch(last_error="rate limit rejected")
            raise Pause(datetime.fromtimestamp(rejected) + timedelta(minutes=2),
                        reason="rate limit rejected")
        detail = text or "; ".join(result.errors or []) or result.subtype
        _raise_for_error(ctx, detail, {"subtype": result.subtype,
                                       "api_error_status": result.api_error_status}, now)

    if result.structured_output is not None:
        data = validated(ctx, json.dumps(result.structured_output),
                         validate or (lambda d: d), True)
    else:
        data = validated(ctx, text, validate, expect_json)
    return {"text": text, "data": data, "session_id": result.session_id,
            "cost_usd": result.total_cost_usd, "turns": result.num_turns,
            "usage": usage, "models": models}


def _sdk() -> Any:
    try:
        import claude_agent_sdk
    except ImportError as e:
        raise Fatal('the Agent SDK adapter needs: pip install "ratchetflow[agent-sdk]"') from e
    return claude_agent_sdk


async def _drive(sdk: Any, query: Callable[..., Any], prompt: str, options: Any, ctx: Any,
                 timeout_s: float, stop: Callable[[], bool], prices: Prices) -> dict:
    seen: dict = {"result": None, "error": None, "rate_limit_reset": None, "tokens": {}}
    announced = False
    usage_by_msg: dict[str, float] = {}
    tokens_by_msg: dict[str, dict] = {}

    async def consume() -> None:
        nonlocal announced
        async for msg in query(prompt=prompt, options=options):
            sid = getattr(msg, "session_id", None)
            if isinstance(msg, sdk.SystemMessage):
                sid = (msg.data or {}).get("session_id")
            if sid and not announced:
                ctx.save_scratch(session_id=sid)  # to disk now: a retry resumes it
                announced = True
            if isinstance(msg, sdk.AssistantMessage) and msg.usage and msg.message_id:
                usd = estimate_cost(msg.usage, msg.model or "", prices)
                if usage_by_msg.get(msg.message_id) != usd:
                    usage_by_msg[msg.message_id] = usd
                    ctx.save_scratch(unbilled_usd=sum(usage_by_msg.values()))
                tokens_by_msg[msg.message_id] = _short_usage(msg.usage)
                seen["tokens"] = sum_tokens(tokens_by_msg.values())
            elif isinstance(msg, sdk.RateLimitEvent):
                info = msg.rate_limit_info
                seen["rate_limit_reset"] = info.resets_at if info.status == "rejected" else None
            elif isinstance(msg, sdk.ResultMessage):
                seen["result"] = msg

    task = asyncio.ensure_future(consume())
    deadline = asyncio.get_running_loop().time() + timeout_s
    while not task.done():
        await asyncio.wait({task}, timeout=0.5)
        if task.done():
            break
        if stop() or asyncio.get_running_loop().time() > deadline:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            if stop():
                raise KeyboardInterrupt("run stopped")
            raise TimeoutError(f"the agent went over {timeout_s:.0f}s")
    error = task.exception()
    if error is not None:
        if not isinstance(error, sdk.ClaudeSDKError):
            raise error
        seen["error"] = error  # a failed session still yields its result first
    return seen
