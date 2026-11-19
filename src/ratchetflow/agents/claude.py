"""A step that is a headless Claude Code session (`claude -p`).

What it adds over spawning the subprocess yourself:

- Stores the `session_id` in the step's scratch **as soon as the CLI announces it**. If the
  process dies halfway, the retry runs `--resume <session_id>` instead of starting over.
- Quota used up (429 "session limit ... resets 1:30pm") -> `Pause` until the reset time.
- Overload / 5xx -> `Retryable`.
- Timeout -> kills the whole process group (the CLI spawns children: MCP, subagents, shells).
- Step cost -> `ctx.record_cost`, which counts towards the run's budget. An attempt that is
  cut off (timeout, kill, crash) never gets the CLI's own figure, so its cost is estimated
  from the token usage streamed so far (see `PRICES`).
- Structured output: extracts the JSON from the answer and passes it through `validate`.

Usage:

    out = ctx.step("outline", claude, ctx, "Outline the article and return it as JSON",
                   cwd=folder, validate=check_outline, timeout_s=900)
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

from ..engine import BUDGET_EVENT
from ..errors import Fatal, NeedsHuman, Pause, Retryable
from ._common import (DEFAULT_RESUME_PROMPT, Prices, add_usage, bill_unbilled, price,
                      reset_time, run_jsonl, validated)

# USD per million tokens: (input, output, cache read), from Anthropic's price list (Nov 2026).
# Cache writes cost 1.25x input (5-minute TTL) or 2x input (1-hour TTL). Only used to estimate
# attempts that were cut off; pass `prices=` to `claude()` to override.
PRICES: Prices = {
    "claude-fable-5-2": (8.0, 40.0, 0.20),
    "claude-mythos-5-2": (8.0, 40.0, 0.20),
    "claude-fable-5-1": (10.0, 50.0, 0.25),
    "claude-mythos-5-1": (10.0, 50.0, 0.25),
    "claude-fable-5": (10.0, 50.0, 1.00),
    "claude-mythos-5": (10.0, 50.0, 1.00),
    "claude-opus-5-5": (4.0, 20.0, 0.20),
    "claude-opus-5": (5.0, 25.0, 0.50),
    "claude-opus-4": (5.0, 25.0, 0.50),
    "claude-sonnet-5-5": (2.0, 10.0, 0.10),
    "claude-sonnet-5": (2.0, 10.0, 0.20),
    "claude-sonnet-4": (3.0, 15.0, 0.30),
    "claude-haiku-5-5": (0.10, 0.50, 0.01),
    "claude-haiku-4": (1.0, 5.0, 0.10),
}

_QUOTA = re.compile(r"(session|usage|rate) limit|hit your .*limit", re.I)
_RESET = re.compile(r"resets\s+(?:at\s+)?(\d{1,2})(?::(\d{2}))?\s*(am|pm)?", re.I)
_OVERLOAD = re.compile(r"overloaded|529|50[0-4]|internal server error|ECONNRESET|ETIMEDOUT", re.I)


def claude(ctx: Any, prompt: str, *, cwd: Optional[str | Path] = None,
           model: Optional[str] = None, max_turns: Optional[int] = None,
           timeout_s: float = 1800, validate: Optional[Callable[[Any], Any]] = None,
           extra_args: Sequence[str] = (), bin: str = "claude",
           resume_prompt: str = DEFAULT_RESUME_PROMPT,
           env: Optional[dict] = None, now: Callable[[], datetime] = datetime.now,
           prices: Optional[Prices] = None,
           expect_json: bool = True, max_budget_usd: Optional[float] = None) -> dict:
    """Runs one `claude -p` session as a step and returns its text, data, session and cost.

    `validate` receives the JSON extracted from the answer, or with `expect_json=False` the
    answer's raw text (useful when the agent's real output is files it wrote). Whatever it
    returns becomes `data`. `max_budget_usd` caps the session; by default it is what is left of
    the run's budget, so one long step cannot run far past it. The CLI checks the cap between
    turns: a session can overshoot by up to one turn.
    """
    scratch = ctx.scratch
    # A previous attempt died with the whole process: bill what it had spent before going on.
    bill_unbilled(ctx, scratch.get("unbilled_usd"))
    session_id = scratch.get("session_id")
    cmd = [bin, "-p", "--output-format", "stream-json", "--verbose", *extra_args]
    if model:
        cmd += ["--model", model]
    if max_turns:
        cmd += ["--max-turns", str(max_turns)]
    if max_budget_usd is None and hasattr(ctx, "budget_left"):
        max_budget_usd = ctx.budget_left()
    if max_budget_usd is not None:
        cmd += ["--max-budget-usd", f"{max(max_budget_usd, 0.01):.4f}"]
    if session_id:
        cmd += ["--resume", session_id]
        stdin_text = resume_prompt.format(error=scratch.get("last_error") or "none")
    else:
        stdin_text = prompt

    try:
        result = _run_stream(cmd, stdin_text, cwd, env, timeout_s,
                             on_session=lambda sid: ctx.save_scratch(session_id=sid),
                             on_usage=lambda usd: ctx.save_scratch(unbilled_usd=usd),
                             prices=prices or PRICES,
                             stop=getattr(ctx, "cancelled", None) or (lambda: False))
    except BaseException:
        # Cut off before the CLI reported its cost: bill the estimate now.
        try:
            bill_unbilled(ctx, ctx.scratch.get("unbilled_usd"))
        except BaseException:  # noqa: BLE001 — never hide the original error
            pass
        raise

    # Bill first, then clear: if we die in between, the estimate is billed twice (too much is
    # safer than too little for a budget).
    if result.get("total_cost_usd"):
        ctx.record_cost(float(result["total_cost_usd"]))
    usage = add_usage(ctx.scratch.get("usage"), _short_usage(result.get("usage") or {}))
    models = sorted({*(ctx.scratch.get("models") or []), *(result.get("modelUsage") or {})})
    ctx.save_scratch(unbilled_usd=0, usage=usage, models=models)
    text = result.get("result") or ""
    if result.get("is_error"):
        _raise_for_error(ctx, text, result, now)

    data = validated(ctx, text, validate, expect_json)

    return {"text": text, "data": data, "session_id": result.get("session_id"),
            "cost_usd": result.get("total_cost_usd"), "turns": result.get("num_turns"),
            "usage": usage, "models": models or ([model] if model else [])}


_USAGE = {"input": "input_tokens", "output": "output_tokens",
          "cache_read": "cache_read_input_tokens", "cache_write": "cache_creation_input_tokens"}


def _short_usage(usage: dict) -> dict:
    return {short: usage.get(key) for short, key in _USAGE.items()}


def _raise_for_error(ctx: Any, text: str, result: dict, now: Callable[[], datetime]) -> None:
    ctx.save_scratch(last_error=text[:400])
    if _QUOTA.search(text) or result.get("api_error_status") == 429:
        raise Pause(_reset_time(text, now()), reason=f"quota used up: {text[:120]}")
    if _OVERLOAD.search(text):
        raise Retryable(text[:200])
    if result.get("subtype") == "error_max_budget_usd":
        # Retrying would spend again: wait for `ratchet signal <run> ratchet.budget`, then resume.
        raise NeedsHuman(BUDGET_EVENT, "the session used up its budget")
    if result.get("subtype") == "error_max_turns":
        raise Fatal("the session ran out of --max-turns")
    raise Retryable(text[:200] or "error without text")


def _reset_time(text: str, now: datetime, margin: timedelta = timedelta(minutes=2),
                fallback: timedelta = timedelta(minutes=15)) -> datetime:
    return reset_time(_RESET.search(text), now, margin, fallback)


def estimate_cost(usage: dict, model: str,
                  prices: Optional[Prices] = None) -> float:
    """USD for one API message's `usage`, as Anthropic bills it."""
    inp, out, read = price(model, prices or PRICES)
    written = usage.get("cache_creation_input_tokens") or 0
    detail = usage.get("cache_creation") or {}
    written_1h = detail.get("ephemeral_1h_input_tokens") or 0
    written_5m = detail.get("ephemeral_5m_input_tokens", written - written_1h) or 0
    return (
        (usage.get("input_tokens") or 0) * inp
        + (usage.get("output_tokens") or 0) * out
        + (usage.get("cache_read_input_tokens") or 0) * read
        + written_5m * inp * 1.25
        + written_1h * inp * 2
    ) / 1_000_000


def _run_stream(cmd: list[str], stdin_text: str, cwd: Optional[str | Path],
                env: Optional[dict], timeout_s: float,
                on_session: Callable[[str], None],
                on_usage: Callable[[float], None] = lambda _usd: None,
                prices: Optional[Prices] = None,
                stop: Callable[[], bool] = lambda: False) -> dict:
    result: dict = {}
    announced = False
    # Usage per API message. The stream repeats a message once per content block, and its
    # output_tokens is the count when the message started, so the estimate is a lower bound.
    usage_by_msg: dict[str, float] = {}

    def on_event(ev: dict) -> None:
        nonlocal announced, result
        sid = ev.get("session_id")
        if sid and not announced:
            on_session(sid)  # to disk now: if we die next, the retry resumes it
            announced = True
        msg = ev.get("message") if ev.get("type") == "assistant" else None
        if isinstance(msg, dict) and msg.get("id") and msg.get("usage"):
            usd = estimate_cost(msg["usage"], msg.get("model") or "", prices)
            if usage_by_msg.get(msg["id"]) != usd:
                usage_by_msg[msg["id"]] = usd
                on_usage(sum(usage_by_msg.values()))  # to disk: billable even if we die
        if ev.get("type") == "result":
            result = ev

    code, err = run_jsonl(cmd, stdin_text, cwd, env, timeout_s, on_event, stop)
    if not result:
        raise Retryable(f"the CLI exited (code {code}) without a result event: {err}")
    return result
