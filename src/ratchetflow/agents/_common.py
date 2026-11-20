"""What every agent adapter shares: the child process, its JSON lines, cost and validation."""

from __future__ import annotations

import collections
import json
import os
import queue
import re
import signal
import subprocess
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable, Optional, Sequence

from ..errors import Retryable

DEFAULT_RESUME_PROMPT = (
    "The previous session was cut off or its output was not valid. Before going on, check the "
    "actual state (files, git) and do not redo what is already done. Previous error: {error}. "
    "Finish with the output the original prompt asked for."
)

# Prices are USD per million tokens: (input, output, cache read).
Prices = dict[str, tuple[float, float, float]]

def price(model: str, prices: Prices) -> tuple[float, float, float]:
    """Longest matching prefix; an unknown model gets the highest price of each kind."""
    match = max((k for k in prices if model.startswith(k)), key=len, default=None)
    if match:
        return prices[match]
    return tuple(max(p[i] for p in prices.values()) for i in range(3))  # type: ignore[return-value]


def reset_time(found: Optional[re.Match], now: datetime,
               margin: timedelta = timedelta(minutes=2),
               fallback: timedelta = timedelta(minutes=15)) -> datetime:
    """When a quota resets, from a "1:30pm" / "23:15" match whose groups are (hour, minute, am/pm).

    The announced time is approximate, so a margin is added. With no time, poll every 15 min.
    """
    if not found:
        return now + fallback
    hour, minute, ampm = int(found.group(1)), int(found.group(2) or 0), (found.group(3) or "").lower()
    if ampm == "pm" and hour < 12:
        hour += 12
    if ampm == "am" and hour == 12:
        hour = 0
    at = now.replace(hour=hour % 24, minute=minute, second=0, microsecond=0)
    if at <= now:
        at += timedelta(days=1)
    return at + margin


def bill_unbilled(ctx: Any, usd: Optional[float]) -> None:
    """Bills the estimate an attempt left in the scratch, once."""
    if usd:
        ctx.record_cost(float(usd))
        ctx.save_scratch(unbilled_usd=0)


def add_usage(total: Optional[dict], usage: dict) -> dict:
    """Tokens of every attempt of the step so far: input, output, cache_read, cache_write."""
    total = dict(total or {})
    for key in ("input", "output", "cache_read", "cache_write"):
        total[key] = total.get(key, 0) + int(usage.get(key) or 0)
    return total


def sum_tokens(per_message: Iterable[dict]) -> dict:
    """The tokens of several messages, each already in add_usage's short form."""
    total: dict = {}
    for tokens in per_message:
        total = add_usage(total, tokens)
    return total


def extract_json(text: str) -> Any:
    """The last ```json``` block of the answer or, if there is none, the whole answer."""
    blocks = re.findall(r"```(?:json)?\s*\n(.*?)```", text, re.S)
    return json.loads(blocks[-1] if blocks else text)


def validated(ctx: Any, text: str, validate: Optional[Callable[[Any], Any]],
              expect_json: bool) -> Any:
    """Runs `validate` on the answer; an invalid answer is retried, telling the agent why."""
    if not validate:
        return None
    try:
        data = validate(extract_json(text) if expect_json else text)
        json.dumps(data)  # whatever validates must be storable
    except Exception as e:  # noqa: BLE001
        ctx.save_scratch(last_error=f"invalid output: {e}"[:400])
        raise Retryable(f"invalid output: {e}") from e
    return data


def run_jsonl(cmd: Sequence[str], stdin_text: str, cwd: Optional[str | Path],
              env: Optional[dict], timeout_s: float, on_event: Callable[[dict], None],
              stop: Callable[[], bool] = lambda: False) -> tuple[int, str]:
    """Runs an agent CLI that prints one JSON event per line and hands each event to `on_event`.

    Returns the exit code and the tail of stderr. On timeout, on `stop()` (the run is being
    stopped from another thread) or on any error, the whole process group is killed: agent CLIs
    spawn children (MCP servers, subagents, shells) that would otherwise outlive the step.
    """
    proc = subprocess.Popen(list(cmd), cwd=cwd, env={**os.environ, **(env or {})},
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, encoding="utf-8", errors="replace",
                            start_new_session=True)
    assert proc.stdin and proc.stdout and proc.stderr
    lines: "queue.Queue[Optional[str]]" = queue.Queue()
    stderr_tail: collections.deque[str] = collections.deque(maxlen=50)

    def read_stdout() -> None:
        assert proc.stdout
        for line in proc.stdout:
            lines.put(line)
        lines.put(None)

    def read_stderr() -> None:
        # Always drained: a full stderr pipe would block the CLI until the timeout.
        assert proc.stderr
        for line in proc.stderr:
            stderr_tail.append(line)

    threading.Thread(target=read_stdout, daemon=True).start()
    err_reader = threading.Thread(target=read_stderr, daemon=True)
    err_reader.start()
    deadline = time.monotonic() + timeout_s
    try:
        proc.stdin.write(stdin_text)
        proc.stdin.close()
        while True:
            left = deadline - time.monotonic()
            if left <= 0:
                raise TimeoutError(f"the agent went over {timeout_s:.0f}s")
            if stop():
                raise KeyboardInterrupt("run stopped")
            try:
                line = lines.get(timeout=min(left, 0.5))
            except queue.Empty:
                continue
            if line is None:
                break
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(ev, dict):
                on_event(ev)
        proc.wait(timeout=30)
    except BaseException:
        kill_group(proc)
        raise
    err_reader.join(timeout=1)
    return proc.returncode, "".join(stderr_tail)[-400:]


def kill_group(proc: subprocess.Popen, grace_s: float = 10) -> None:
    """SIGTERM to the whole group and SIGKILL if it does not go: no zombie agents."""
    pgid = proc.pid  # start_new_session: the CLI leads its own group, even after it exits
    for sig, wait in ((signal.SIGTERM, grace_s), (signal.SIGKILL, 5)):
        try:
            os.killpg(pgid, sig)
        except ProcessLookupError:
            return
        try:
            proc.wait(timeout=wait)
            return
        except subprocess.TimeoutExpired:
            continue
