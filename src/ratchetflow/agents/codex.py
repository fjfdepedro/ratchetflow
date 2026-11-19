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
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

from ..errors import Pause, Retryable
from ._common import (DEFAULT_RESUME_PROMPT, Prices, add_usage, price, reset_time, run_jsonl,
                      validated)

_LIMIT = re.compile(r"usage limit", re.I)
_TRY_AGAIN = re.compile(r"try again at[^\n]*?(\d{1,2}):(\d{2})\s*(am|pm)?", re.I)


def codex(ctx: Any, prompt: str, *, cwd: Optional[str | Path] = None,
          model: Optional[str] = None, sandbox: Optional[str] = None,
          output_schema: Optional[dict] = None, timeout_s: float = 1800,
          validate: Optional[Callable[[Any], Any]] = None, expect_json: bool = True,
          extra_args: Sequence[str] = (), bin: str = "codex",
          resume_prompt: str = DEFAULT_RESUME_PROMPT, env: Optional[dict] = None,
          now: Callable[[], datetime] = datetime.now,
          prices: Optional[Prices] = None) -> dict:
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
            raise Pause(reset_time(_TRY_AGAIN.search(text), now()),
                        reason=f"usage limit reached: {text[:120]}")
        raise Retryable(text[:200])

    text = seen["text"] or ""
    data = validated(ctx, text, validate, expect_json)
    return {"text": text, "data": data, "session_id": ctx.scratch.get("session_id"),
            "cost_usd": cost, "usage": usage, "models": [model] if model else []}


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
