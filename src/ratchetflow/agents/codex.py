"""A step that is a headless Codex CLI session (`codex exec --json`).

It works like `claude()`:

- Stores the thread id in the step's scratch **as soon as the CLI announces it**. If the process
  dies halfway, the retry runs `codex exec resume <thread_id>` instead of starting over.
- Usage limit reached ("You've hit your usage limit ... Try again at 3:45 PM") -> `Pause` until
  that time, or every 15 minutes when no time is given.
- Any other failure -> `Retryable`. Timeout -> kills the whole process group.
- Structured output: extracts the JSON from the answer and passes it through `validate`.
  `output_schema` (a JSON Schema) makes Codex itself answer in that shape.

Codex reports tokens, not dollars, and with a ChatGPT sign-in the session is paid for by the
subscription. So by default a step records its tokens and no cost, and the run's budget does not
see it. Pass `prices=` (USD per million tokens: input, output, cached input, by model prefix) to
bill an API key's usage against the budget. Tokens arrive when a turn completes, so an attempt
that is cut off bills nothing.

Usage:

    out = ctx.step("tests", codex, ctx, "Add tests for the parser and return a JSON summary",
                   cwd=repo, sandbox="workspace-write", validate=check_summary)
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from ..errors import Pause, Retryable
from ._common import (
    DEFAULT_RESUME_PROMPT,
    Prices,
    add_usage,
    price,
    reset_time,
    run_jsonl,
    validated,
)

_LIMIT = re.compile(r"usage limit", re.I)
_TRY_AGAIN = re.compile(r"try again at[^\n]*?(\d{1,2}):(\d{2})\s*(am|pm)?", re.I)
# "try again at Nov 27th, 2026 8:31 AM": a reset days away comes with its date.
_TRY_AGAIN_DATE = re.compile(r"try again at\s+([a-z]{3})[a-z]*\.?\s+(\d{1,2})(?:st|nd|rd|th)?,?"
                             r"\s+(\d{4}),?\s+(\d{1,2}):(\d{2})\s*(am|pm)?", re.I)
_MONTHS = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]


def codex(ctx: Any, prompt: str, *, cwd: str | Path | None = None,
          model: str | None = None, sandbox: str | None = None,
          output_schema: dict | None = None, timeout_s: float = 1800,
          validate: Callable[[Any], Any] | None = None, expect_json: bool = True,
          extra_args: Sequence[str] = (), bin: str = "codex",
          resume_prompt: str = DEFAULT_RESUME_PROMPT, env: dict | None = None,
          now: Callable[[], datetime] = datetime.now,
          prices: Prices | None = None) -> dict:
    """Runs one `codex exec` session as a step and returns its text, data, thread and tokens.

    `sandbox` is passed as `--sandbox` (`read-only`, `workspace-write`, `danger-full-access`);
    without it, Codex's own configuration decides. `validate` receives the JSON extracted from
    the answer, or with `expect_json=False` the raw text. Whatever it returns becomes `data`.
    """
    scratch = ctx.scratch
    thread_id = scratch.get("session_id")
    cmd = [bin, "exec", "--json"]
    if model:
        cmd += ["--model", model]
    if sandbox:
        cmd += ["--sandbox", sandbox]
    if cwd:
        cmd += ["--cd", str(cwd)]
    schema_file = None
    if output_schema is not None:
        fd, schema_file = tempfile.mkstemp(prefix="ratchet-schema-", suffix=".json")
        with os.fdopen(fd, "w") as f:
            json.dump(output_schema, f)
        cmd += ["--output-schema", schema_file]
        if validate is None:
            validate = _same
    cmd += list(extra_args)
    if thread_id:
        cmd += ["resume", thread_id, "-"]
        stdin_text = resume_prompt.format(error=scratch.get("last_error") or "none")
    else:
        cmd += ["-"]
        stdin_text = prompt

    seen: dict = {"text": None, "usage": None, "error": None}

    def on_event(ev: dict) -> None:
        kind = ev.get("type")
        if kind == "thread.started" and ev.get("thread_id"):
            ctx.save_scratch(session_id=ev["thread_id"])  # to disk now: a retry resumes it
        elif kind == "item.completed":
            item = ev.get("item") or {}
            if item.get("type") == "agent_message":
                seen["text"] = item.get("text") or ""
        elif kind == "turn.completed":
            seen["usage"] = ev.get("usage") or {}
        elif kind == "turn.failed":
            seen["error"] = (ev.get("error") or {}).get("message") or "the turn failed"
        elif kind == "error":
            seen["error"] = ev.get("message") or "error without text"

    try:
        code, err = run_jsonl(cmd, stdin_text, cwd, env, timeout_s, on_event,
                              getattr(ctx, "cancelled", None) or (lambda: False))
    finally:
        if schema_file:
            os.unlink(schema_file)

    tokens = _tokens(seen["usage"] or {})
    usage = add_usage(ctx.scratch.get("usage"), tokens)
    cost = _cost(tokens, model or "", prices) if prices and seen["usage"] else None
    if cost:
        ctx.record_cost(cost)
    ctx.save_scratch(usage=usage)

    if seen["error"] or seen["usage"] is None:
        text = seen["error"] or f"the CLI exited (code {code}) without finishing the turn: {err}"
        ctx.save_scratch(last_error=text[:400])
        if _LIMIT.search(text):
            raise Pause(_reset_time(text, now()),
                        reason=f"usage limit reached: {text[:120]}")
        raise Retryable(text[:200])

    text = seen["text"] or ""
    data = validated(ctx, text, validate, expect_json)
    return {"text": text, "data": data, "session_id": ctx.scratch.get("session_id"),
            "cost_usd": cost, "usage": usage, "models": [model] if model else []}


def _reset_time(text: str, now: datetime, margin: timedelta = timedelta(minutes=2)) -> datetime:
    """When the usage limit resets: on the announced date when there is one, else by the time."""
    found = _TRY_AGAIN_DATE.search(text)
    if found and found.group(1).lower() in _MONTHS:
        hour, minute = int(found.group(4)), int(found.group(5))
        ampm = (found.group(6) or "").lower()
        if ampm == "pm" and hour < 12:
            hour += 12
        if ampm == "am" and hour == 12:
            hour = 0
        try:
            at = datetime(int(found.group(3)), _MONTHS.index(found.group(1).lower()) + 1,
                          int(found.group(2)), hour % 24, minute)
        except ValueError:
            at = None
        if at is not None and at > now:
            return at + margin
    return reset_time(_TRY_AGAIN.search(text), now)


def _same(data: Any) -> Any:
    return data


def _tokens(usage: dict) -> dict:
    """Codex counts cached input inside `input_tokens`; the step keeps them apart."""
    cached = int(usage.get("cached_input_tokens") or 0)
    return {"input": int(usage.get("input_tokens") or 0) - cached, "cache_read": cached,
            "output": int(usage.get("output_tokens") or 0), "cache_write": 0}


def _cost(tokens: dict, model: str, prices: Prices) -> float:
    inp, out, read = price(model, prices)
    return (tokens["input"] * inp + tokens["output"] * out + tokens["cache_read"] * read) / 1e6
