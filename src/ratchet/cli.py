"""CLI: `ratchet runs | show | signal | retry | tick | start`.

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

from .engine import Engine
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
        raise SystemExit(f"{spec} is not a ratchet.Engine")
    return engine


def _stop_on_sigterm() -> None:
    """SIGTERM/SIGHUP behave like Ctrl-C: the agent's process group dies and the lease is freed."""
    def handler(signum: int, _frame: object) -> None:
        raise KeyboardInterrupt(signal.Signals(signum).name)
    for sig in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, handler)


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

    g = sub.add_parser("signal", help="send an event to a waiting run")
    g.add_argument("run_id")
    g.add_argument("event")
    g.add_argument("payload", nargs="?", default="null", help="JSON")

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
            print(f"  reason: {run.error.splitlines()[0]}")
        for step in store.list_steps(run.id):
            dur = f"{step.finished_at - step.started_at:.0f}s" if step.finished_at else "..."
            print(f"  {step.seq:>3} {step.name:<28} {step.status:<8} x{step.attempts}"
                  f" {step.cost_usd:>6.2f}$ {dur:>7}  {step.error or ''}")
            if a.output and step.output is not None:
                print("      " + json.dumps(step.output, ensure_ascii=False)[:300])
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
