"""CLI: `ratchet runs | show | inbox | signal | export | retry | tick | start`.

`tick` and `start` need the workflows: `--app module:engine` or `--app file.py:engine` (or the
RATCHET_APP variable). The other commands only read or write the database: the one of `--app`
if given, otherwise `--db` (or RATCHET_DB, or ./ratchet.db).
"""

from __future__ import annotations

import argparse
import importlib
import importlib.util
import json
import os
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from .engine import BUDGET_EVENT, Engine
from .store import Store


def _load_engine(spec: str) -> Engine:
    target, _, attr = spec.partition(":")
    attr = attr or "engine"
    if target.endswith(".py"):
        path = Path(target).resolve()
        sys.path.insert(0, str(path.parent))
        mod_spec = importlib.util.spec_from_file_location(path.stem, path)
        assert mod_spec and mod_spec.loader
        mod = importlib.util.module_from_spec(mod_spec)
        mod_spec.loader.exec_module(mod)
    else:
        sys.path.insert(0, os.getcwd())
        mod = importlib.import_module(target)
    engine = getattr(mod, attr)
    if not isinstance(engine, Engine):
        raise SystemExit(f"{spec} is not a ratchetflow.Engine")
    return engine


def _stop_on_sigterm() -> None:
    """SIGTERM/SIGHUP behave like Ctrl-C: the agent's process group dies and the lease is freed."""
    def handler(signum: int, _frame: object) -> None:
        raise KeyboardInterrupt(signal.Signals(signum).name)
    for sig in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, handler)


def _span(seconds: float) -> str:
    if seconds < 120:
        return f"{seconds:.0f}s"
    if seconds < 7200:
        return f"{seconds / 60:.0f}m"
    return f"{seconds / 3600:.1f}h"


def _duration(step) -> str:
    """Working time, plus how long the step waited (for a person, a pause or a retry)."""
    if not step.finished_at:
        return "..."
    wall = step.finished_at - step.started_at
    if step.busy_s is None:   # written before busy time was recorded
        return _span(wall)
    waited = wall - step.busy_s
    return _span(step.busy_s) + (f" (+{_span(waited)} waiting)" if waited >= 1 else "")


def _iso(ts: Optional[float]) -> Optional[str]:
    return datetime.fromtimestamp(ts).astimezone().isoformat(timespec="seconds") if ts else None


def _export_row(run, step) -> dict:
    """One step, flat enough for pandas or DuckDB. Agent steps add what the session used."""
    wall = step.finished_at - step.started_at if step.finished_at else None
    row = {"run_id": run.id, "workflow": run.workflow, "run_status": run.status,
           "seq": step.seq, "step": step.name, "status": step.status,
           "attempts": step.attempts, "cost_usd": round(step.cost_usd, 6),
           "busy_s": step.busy_s, "waited_s": (round(wall - step.busy_s, 3)
                                               if wall is not None and step.busy_s is not None
                                               else None),
           "started_at": _iso(step.started_at), "finished_at": _iso(step.finished_at),
           "error": step.error.splitlines()[0] if step.error else None}
    out = step.output
    if isinstance(out, dict) and "session_id" in out and "turns" in out:
        row["agent"] = {k: out.get(k) for k in ("models", "turns", "usage", "session_id")}
    return row


def _fmt_t(ts: Optional[float]) -> str:
    return datetime.fromtimestamp(ts).strftime("%m-%d %H:%M:%S") if ts else "-"


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(prog="ratchet", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db", default=os.environ.get("RATCHET_DB"))
    p.add_argument("--app", default=os.environ.get("RATCHET_APP"))
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("runs", help="list runs")
    r.add_argument("--status")
    r.add_argument("-n", type=int, default=20)

    s = sub.add_parser("show", help="one run and its steps")
    s.add_argument("run_id")
    s.add_argument("--output", action="store_true", help="include each step's output")

    i = sub.add_parser("inbox", help="runs waiting for a person, with what to answer")
    i.add_argument("--json", action="store_true", help="machine-readable output")

    g = sub.add_parser("signal", help="send an event to a waiting run")
    g.add_argument("run_id")
    g.add_argument("event")
    g.add_argument("payload", nargs="?", default="null", help="JSON")

    x = sub.add_parser("export", help="steps as JSON Lines (cost, time, tokens), for metrics")
    x.add_argument("run_ids", nargs="*", help="default: every run")
    x.add_argument("--status", help="only runs in this status")

    t = sub.add_parser("retry", help="reopen a failed run")
    t.add_argument("run_id")

    k = sub.add_parser("tick", help="resume whatever is due (for cron/systemd)")
    k.add_argument("--loop", type=float, metavar="SEC", help="repeat every SEC seconds")

    st = sub.add_parser("start", help="start a workflow")
    st.add_argument("workflow")
    st.add_argument("input", nargs="?", default="null", help="JSON")
    st.add_argument("--id", dest="run_id")
    st.add_argument("--budget", type=float, help="cap in USD")

    a = p.parse_args(argv)

    if a.cmd in ("tick", "start"):
        if not a.app:
            p.error(f"`{a.cmd}` needs --app module:engine (or RATCHET_APP)")
        engine = _load_engine(a.app)
        if argv is None:  # only when running as the real CLI, not from tests
            _stop_on_sigterm()
        if a.cmd == "start":
            run_id = engine.start(a.workflow, json.loads(a.input), run_id=a.run_id,
                                  budget_usd=a.budget)
            run = engine.store.get_run(run_id)
            print(f"{run_id} {run.status if run else '?'}")
            return 0
        while True:
            done = engine.tick()
            if done:
                print("resumed:", " ".join(done))
            if not a.loop:
                return 0
            time.sleep(a.loop)

    if a.db:
        store = Store(a.db)
    elif a.app:
        store = _load_engine(a.app).store
    else:
        store = Store("ratchet.db")

    if a.cmd == "runs":
        for run in store.list_runs(a.status, a.n):
            waiting = run.wait_event or (_fmt_t(run.wake_at) if run.wake_at else "")
            cost = store.run_cost(run.id)
            print(f"{run.id}  {run.workflow:<20} {run.status:<10} {cost:>7.2f}$"
                  f"  {_fmt_t(run.updated_at)}  {waiting}")
        return 0
    if a.cmd == "show":
        run = store.get_run(a.run_id)
        if not run:
            raise SystemExit(f"no run {a.run_id}")
        budget = f" / {run.budget_usd:.2f}" if run.budget_usd is not None else ""
        print(f"{run.id}  {run.workflow}  {run.status}  cost {store.run_cost(run.id):.2f}{budget} USD")
        if run.error:
            # A waiting run's reason is a question for someone: show it whole. A failure's is a
            # traceback: its first line says enough.
            reason = run.error if run.status == "waiting" else run.error.splitlines()[0]
            print("  reason: " + reason.replace("\n", "\n          "))
        for step in store.list_steps(run.id):
            dur = _duration(step)
            print(f"  {step.seq:>3} {step.name:<28} {step.status:<8} x{step.attempts}"
                  f" {step.cost_usd:>6.2f}$ {dur:>7}  {step.error or ''}")
            if a.output and step.output is not None:
                print("      " + json.dumps(step.output, ensure_ascii=False)[:300])
        return 0
    if a.cmd == "inbox":
        items = store.inbox(BUDGET_EVENT)
        if a.json:
            print(json.dumps(items, ensure_ascii=False, indent=2))
            return 0
        if not items:
            print("nobody is waiting for you")
        for item in items:
            print(f"{item['run_id']}  {item['workflow']}  waiting since {_fmt_t(item['since'])}"
                  f"  step: {item['step'] or '-'}")
            for line in item["question"].splitlines() or [""]:
                print(f"  {line}")
            if item["questions"]:
                print("  open questions:")
                for n, q in enumerate(item["questions"], 1):
                    print(f"    {n}. " + q.replace("\n", "\n       "))
            if item["details"] is not None:
                print("  details:")
                for line in json.dumps(item["details"], ensure_ascii=False, indent=2).splitlines():
                    print(f"    {line}")
            payload = '{"add_usd": <USD>}' if item["event"] == BUDGET_EVENT else "<json>"
            print(f"  answer: ratchet signal {item['run_id']} {item['event']} '{payload}'\n")
        return 0
    if a.cmd == "export":
        runs = ([store.get_run(r) for r in a.run_ids] if a.run_ids
                else list(reversed(store.list_runs(a.status, 1_000_000))))
        for run in runs:
            if run is None or (a.status and run.status != a.status):
                continue
            for step in store.list_steps(run.id):
                print(json.dumps(_export_row(run, step), ensure_ascii=False))
        return 0
    if a.cmd == "signal":
        if not store.get_run(a.run_id):
            raise SystemExit(f"no run {a.run_id}")
        store.add_event(a.run_id, a.event, json.loads(a.payload), time.time())
        print(f"event {a.event!r} sent; the next `ratchet tick` picks it up")
        return 0
    if a.cmd == "retry":
        print("reopened" if store.reopen_failed(a.run_id) else "it was not failed")
        return 0
    return 1


def entry() -> int:
    try:
        return main()
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(entry())
