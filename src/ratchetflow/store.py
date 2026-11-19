"""SQLite persistence: runs, steps and events.

One source of truth on disk. Nothing in /tmp: a reboot must not take away the state of a run
that has been going for hours.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id           TEXT PRIMARY KEY,
    workflow     TEXT NOT NULL,
    input_json   TEXT NOT NULL,
    status       TEXT NOT NULL,          -- pending | running | waiting | completed | failed
    wake_at      REAL,                   -- waiting for a time
    wait_event   TEXT,                   -- waiting for an event
    owner        TEXT,
    lease_until  REAL,
    code_hash    TEXT,
    budget_usd   REAL,
    output_json  TEXT,
    error        TEXT,
    created_at   REAL NOT NULL,
    updated_at   REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS steps (
    run_id       TEXT NOT NULL REFERENCES runs(id),
    seq          INTEGER NOT NULL,
    name         TEXT NOT NULL,
    status       TEXT NOT NULL,          -- running | waiting | done | failed
    attempts     INTEGER NOT NULL DEFAULT 0,
    output_json  TEXT,
    error        TEXT,
    scratch_json TEXT NOT NULL DEFAULT '{}',
    cost_usd     REAL NOT NULL DEFAULT 0,
    started_at   REAL NOT NULL,
    finished_at  REAL,
    busy_s       REAL,                   -- time spent running the step, waits left out
    PRIMARY KEY (run_id, seq),
    UNIQUE (run_id, name)
);
CREATE TABLE IF NOT EXISTS events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id       TEXT NOT NULL REFERENCES runs(id),
    name         TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    consumed_seq INTEGER,
    created_at   REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS runs_status ON runs(status, wake_at);
CREATE INDEX IF NOT EXISTS events_pending ON events(run_id, name, consumed_seq);
"""

# A run is due when its wait time has come, its event has arrived or its worker died.
_RUNNABLE = (
    "((r.status = 'waiting' AND r.wake_at IS NOT NULL AND r.wake_at <= :now)"
    " OR (r.status = 'waiting' AND r.wait_event IS NOT NULL AND EXISTS ("
    "     SELECT 1 FROM events e WHERE e.run_id = r.id AND e.name = r.wait_event"
    "     AND e.consumed_seq IS NULL))"
    " OR (r.status IN ('pending', 'running') AND (r.lease_until IS NULL OR r.lease_until < :now)))"
)

# Step writes only land while the writer still holds the run's lease.
_OWNS = "EXISTS (SELECT 1 FROM runs WHERE id = :run_id AND owner = :owner)"


@dataclass
class RunRow:
    id: str
    workflow: str
    input: Any
    status: str
    wake_at: Optional[float]
    wait_event: Optional[str]
    owner: Optional[str]
    lease_until: Optional[float]
    code_hash: Optional[str]
    budget_usd: Optional[float]
    output: Any
    error: Optional[str]
    created_at: float
    updated_at: float


@dataclass
class StepRow:
    run_id: str
    seq: int
    name: str
    status: str
    attempts: int
    output: Any
    error: Optional[str]
    scratch: dict
    cost_usd: float
    started_at: float
    finished_at: Optional[float]
    busy_s: Optional[float] = None   # None: written before 0.2.0


def _loads(s: Optional[str]) -> Any:
    return None if s is None else json.loads(s)


class Store:
    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._db = sqlite3.connect(str(self.path), isolation_level=None, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA busy_timeout=5000")
        self._db.execute("PRAGMA foreign_keys=ON")
        self._db.executescript(SCHEMA)
        self._migrate()

    def _migrate(self) -> None:
        """Columns added after 0.1.0. Old rows keep NULL: their working time is unknown."""
        cols = {r["name"] for r in self._db.execute("PRAGMA table_info(steps)")}
        if "busy_s" not in cols:
            try:
                self._db.execute("ALTER TABLE steps ADD COLUMN busy_s REAL")
            except sqlite3.OperationalError:   # another process added it first
                pass

    def close(self) -> None:
        self._db.close()

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        """IMMEDIATE transaction: takes the write lock up front, not at the first UPDATE."""
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                yield self._db
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise

    def q(self, sql: str, params: tuple | dict = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._db.execute(sql, params).fetchall()

    def x(self, sql: str, params: tuple | dict = ()) -> int:
        with self._lock:
            return self._db.execute(sql, params).rowcount

    # --- runs -------------------------------------------------------------

    def insert_run(self, run_id: str, workflow: str, input_: Any, code_hash: str,
                   budget_usd: Optional[float], now: float) -> None:
        self.x(
            "INSERT INTO runs (id, workflow, input_json, status, code_hash, budget_usd,"
            " created_at, updated_at) VALUES (?, ?, ?, 'pending', ?, ?, ?, ?)",
            (run_id, workflow, json.dumps(input_), code_hash, budget_usd, now, now),
        )

    def get_run(self, run_id: str) -> Optional[RunRow]:
        rows = self.q("SELECT * FROM runs WHERE id = ?", (run_id,))
        return self._run(rows[0]) if rows else None

    def list_runs(self, status: Optional[str] = None, limit: int = 50) -> list[RunRow]:
        if status:
            rows = self.q("SELECT * FROM runs WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                          (status, limit))
        else:
            rows = self.q("SELECT * FROM runs ORDER BY created_at DESC LIMIT ?", (limit,))
        return [self._run(r) for r in rows]

    def acquire_lease(self, run_id: str, owner: str, now: float, ttl: float) -> bool:
        """Only one process runs a run at a time. An expired lease means a dead worker."""
        with self.tx() as db:
            n = db.execute(
                "UPDATE runs SET owner = ?, lease_until = ?, status = 'running', updated_at = ?"
                " WHERE id = ? AND status IN ('pending', 'running', 'waiting')"
                " AND (lease_until IS NULL OR lease_until < ? OR owner = ?)",
                (owner, now + ttl, now, run_id, now, owner),
            ).rowcount
        return n == 1

    def renew_lease(self, run_id: str, owner: str, now: float, ttl: float) -> bool:
        return self.x("UPDATE runs SET lease_until = ? WHERE id = ? AND owner = ?",
                      (now + ttl, run_id, owner)) == 1

    def release_lease(self, run_id: str, owner: str) -> None:
        self.x("UPDATE runs SET owner = NULL, lease_until = NULL WHERE id = ? AND owner = ?",
               (run_id, owner))

    def finish_run(self, run_id: str, owner: str, status: str, now: float, *,
                   output: Any = None, error: Optional[str] = None,
                   wake_at: Optional[float] = None, wait_event: Optional[str] = None) -> None:
        self.x(
            "UPDATE runs SET status = ?, output_json = ?, error = ?, wake_at = ?, wait_event = ?,"
            " owner = NULL, lease_until = NULL, updated_at = ? WHERE id = ? AND owner = ?",
            (status, None if output is None else json.dumps(output), error, wake_at, wait_event,
             now, run_id, owner),
        )

    def reopen_failed(self, run_id: str) -> bool:
        """A `failed` run goes back to `pending` and its failed steps get all their attempts back."""
        with self.tx() as db:
            n = db.execute("UPDATE runs SET status = 'pending', error = NULL, owner = NULL,"
                           " lease_until = NULL WHERE id = ? AND status = 'failed'",
                           (run_id,)).rowcount
            if n:
                db.execute("UPDATE steps SET status = 'running', attempts = 0, error = NULL"
                           " WHERE run_id = ? AND status = 'failed'", (run_id,))
        return n == 1

    def runnable_ids(self, now: float) -> list[str]:
        """Runs `tick` must resume: wait time reached, event arrived or worker dead."""
        rows = self.q(f"SELECT r.id FROM runs r WHERE {_RUNNABLE} ORDER BY r.created_at",
                      {"now": now})
        return [r["id"] for r in rows]

    def is_runnable(self, run_id: str, now: float) -> bool:
        return bool(self.q(f"SELECT 1 FROM runs r WHERE r.id = :id AND {_RUNNABLE}",
                           {"id": run_id, "now": now}))

    def inbox(self, budget_event: str) -> list[dict]:
        """Runs waiting for a person: a `NeedsHuman` question or a budget top-up, oldest first.

        Runs waiting for an external event (`wait_for_event`, e.g. CI) are not listed.
        """
        items = []
        for run in reversed(self.list_runs("waiting", limit=100_000)):
            waiting = [s for s in self.list_steps(run.id) if s.status == "waiting"]
            step = waiting[-1] if waiting else None
            if run.wait_event == budget_event:
                question, details, questions = run.error or "budget used up", {
                    "spent_usd": round(self.run_cost(run.id), 4), "budget_usd": run.budget_usd}, []
            elif step is not None and step.scratch.get("waiting_event") == run.wait_event:
                question = step.scratch.get("question") or run.error or ""
                details = step.scratch.get("details")
                questions = step.scratch.get("questions") or []
            else:
                continue
            items.append({"run_id": run.id, "workflow": run.workflow, "event": run.wait_event,
                          "step": step.name if step else None, "question": question,
                          "questions": questions, "details": details, "since": run.updated_at})
        return items

    # --- steps ------------------------------------------------------------

    def get_step(self, run_id: str, seq: int) -> Optional[StepRow]:
        rows = self.q("SELECT * FROM steps WHERE run_id = ? AND seq = ?", (run_id, seq))
        return self._step(rows[0]) if rows else None

    def list_steps(self, run_id: str) -> list[StepRow]:
        return [self._step(r) for r in
                self.q("SELECT * FROM steps WHERE run_id = ? ORDER BY seq", (run_id,))]

    def start_step(self, run_id: str, seq: int, name: str, now: float,
                   owner: Optional[str] = None) -> Optional[StepRow]:
        """Creates the step (or marks it running again). With `owner`, None if the lease is gone."""
        with self.tx() as db:
            if owner is not None and not db.execute(
                    f"SELECT {_OWNS}", {"run_id": run_id, "owner": owner}).fetchone()[0]:
                return None
            db.execute("INSERT OR IGNORE INTO steps (run_id, seq, name, status, started_at, busy_s)"
                       " VALUES (?, ?, ?, 'running', ?, 0)", (run_id, seq, name, now))
            db.execute("UPDATE steps SET status = 'running'"
                       " WHERE run_id = ? AND seq = ? AND status NOT IN ('done', 'failed')",
                       (run_id, seq))
        return self.get_step(run_id, seq)

    def update_step(self, run_id: str, seq: int, *, owner: Optional[str] = None,
                    **fields: Any) -> bool:
        """With `owner`, only writes while that owner holds the lease. Returns whether it wrote."""
        sets, params = [], {"run_id": run_id, "seq": seq, "owner": owner}
        for i, (k, v) in enumerate(fields.items()):
            col = f"{k}_json" if k in ("output", "scratch") else k
            sets.append(f"{col} = :v{i}")
            params[f"v{i}"] = json.dumps(v) if k in ("output", "scratch") else v
        fence = f" AND {_OWNS}" if owner is not None else ""
        return self.x(f"UPDATE steps SET {', '.join(sets)} WHERE run_id = :run_id AND seq = :seq"
                      f"{fence}", params) == 1

    def add_cost(self, run_id: str, seq: int, usd: float) -> None:
        self.x("UPDATE steps SET cost_usd = cost_usd + ? WHERE run_id = ? AND seq = ?",
               (usd, run_id, seq))

    def add_busy(self, run_id: str, seq: int, seconds: float) -> None:
        self.x("UPDATE steps SET busy_s = COALESCE(busy_s, 0) + ? WHERE run_id = ? AND seq = ?",
               (max(seconds, 0.0), run_id, seq))

    def run_cost(self, run_id: str) -> float:
        return self.q("SELECT COALESCE(SUM(cost_usd), 0) AS c FROM steps WHERE run_id = ?",
                      (run_id,))[0]["c"]

    # --- events -----------------------------------------------------------

    def add_event(self, run_id: str, name: str, payload: Any, now: float) -> None:
        self.x("INSERT INTO events (run_id, name, payload_json, created_at) VALUES (?, ?, ?, ?)",
               (run_id, name, json.dumps(payload), now))

    def consume_event(self, run_id: str, name: str, seq: int,
                      reuse: bool = True) -> tuple[bool, Any]:
        """Assigns the oldest unconsumed event to step `seq`.

        With `reuse`, if the step already had one assigned it returns that one (idempotent on
        replay).
        """
        with self.tx() as db:
            if reuse:
                mine = db.execute("SELECT payload_json FROM events WHERE run_id = ? AND name = ?"
                                  " AND consumed_seq = ?", (run_id, name, seq)).fetchone()
                if mine:
                    return True, _loads(mine["payload_json"])
            row = db.execute("SELECT id, payload_json FROM events WHERE run_id = ? AND name = ?"
                             " AND consumed_seq IS NULL ORDER BY id LIMIT 1",
                             (run_id, name)).fetchone()
            if not row:
                return False, None
            db.execute("UPDATE events SET consumed_seq = ? WHERE id = ?", (seq, row["id"]))
            return True, _loads(row["payload_json"])

    def absorb_budget_events(self, run_id: str, name: str) -> float:
        """Adds pending budget top-ups to the run's budget, in the same transaction."""
        with self.tx() as db:
            rows = db.execute("SELECT id, payload_json FROM events WHERE run_id = ? AND name = ?"
                              " AND consumed_seq IS NULL", (run_id, name)).fetchall()
            extra = sum(float((_loads(r["payload_json"]) or {}).get("add_usd", 0)) for r in rows)
            if rows:
                db.execute(f"UPDATE events SET consumed_seq = -1 WHERE id IN"
                           f" ({','.join('?' * len(rows))})", tuple(r["id"] for r in rows))
                db.execute("UPDATE runs SET budget_usd = budget_usd + ? WHERE id = ?",
                           (extra, run_id))
            return db.execute("SELECT budget_usd FROM runs WHERE id = ?",
                              (run_id,)).fetchone()["budget_usd"]

    # --- mapping ----------------------------------------------------------

    @staticmethod
    def _run(r: sqlite3.Row) -> RunRow:
        return RunRow(r["id"], r["workflow"], _loads(r["input_json"]), r["status"], r["wake_at"],
                      r["wait_event"], r["owner"], r["lease_until"], r["code_hash"],
                      r["budget_usd"], _loads(r["output_json"]), r["error"], r["created_at"],
                      r["updated_at"])

    @staticmethod
    def _step(r: sqlite3.Row) -> StepRow:
        return StepRow(r["run_id"], r["seq"], r["name"], r["status"], r["attempts"],
                       _loads(r["output_json"]), r["error"], _loads(r["scratch_json"]) or {},
                       r["cost_usd"], r["started_at"], r["finished_at"], r["busy_s"])
