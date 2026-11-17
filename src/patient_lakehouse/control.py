"""Transactional SQLite control plane for runs and committed watermarks."""
from __future__ import annotations

import sqlite3
from pathlib import Path


class ControlStore:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("CREATE TABLE IF NOT EXISTS watermarks (dataset TEXT PRIMARY KEY, value TEXT NOT NULL)")
            db.execute("CREATE TABLE IF NOT EXISTS event_ledger (dataset TEXT NOT NULL, event_id TEXT NOT NULL, first_run_id TEXT NOT NULL, PRIMARY KEY(dataset,event_id))")
            db.execute("""CREATE TABLE IF NOT EXISTS ingestion_runs (
                run_id TEXT PRIMARY KEY, dataset TEXT NOT NULL, status TEXT NOT NULL,
                started_at TEXT NOT NULL, completed_at TEXT, source_count INTEGER DEFAULT 0,
                accepted_count INTEGER DEFAULT 0, rejected_count INTEGER DEFAULT 0,
                deduplicated_count INTEGER DEFAULT 0, silver_count INTEGER DEFAULT 0,
                duration_seconds REAL, watermark TEXT, error TEXT)""")
            columns = {row["name"] for row in db.execute("PRAGMA table_info(ingestion_runs)")}
            if "deduplicated_count" not in columns:
                db.execute("ALTER TABLE ingestion_runs ADD COLUMN deduplicated_count INTEGER DEFAULT 0")

    def _connect(self):
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        return db

    def get_watermark(self, dataset: str) -> str | None:
        with self._connect() as db:
            row = db.execute("SELECT value FROM watermarks WHERE dataset=?", (dataset,)).fetchone()
            return row["value"] if row else None

    def seen_event_ids(self, dataset: str, event_ids: list[str]) -> set[str]:
        if not event_ids:
            return set()
        found: set[str] = set()
        with self._connect() as db:
            for offset in range(0, len(event_ids), 500):
                chunk = event_ids[offset:offset + 500]
                placeholders = ",".join("?" for _ in chunk)
                rows = db.execute(f"SELECT event_id FROM event_ledger WHERE dataset=? AND event_id IN ({placeholders})",
                                  [dataset, *chunk]).fetchall()
                found.update(row["event_id"] for row in rows)
        return found

    def start_run(self, run_id: str, dataset: str, started_at: str) -> None:
        with self._connect() as db:
            db.execute("INSERT INTO ingestion_runs(run_id,dataset,status,started_at) VALUES(?,?,?,?)",
                       (run_id, dataset, "RUNNING", started_at))

    def finish_run(self, run_id: str, *, status: str, completed_at: str, source_count: int,
                   accepted_count: int, rejected_count: int, deduplicated_count: int, silver_count: int,
                   duration_seconds: float, watermark: str | None, error: str | None = None,
                   event_ids: list[str] | None = None) -> None:
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT dataset FROM ingestion_runs WHERE run_id=?", (run_id,)).fetchone()
            if not row:
                raise KeyError(f"Unknown run_id {run_id}")
            db.execute("""UPDATE ingestion_runs SET status=?,completed_at=?,source_count=?,accepted_count=?,
                       rejected_count=?,deduplicated_count=?,silver_count=?,duration_seconds=?,watermark=?,error=? WHERE run_id=?""",
                       (status, completed_at, source_count, accepted_count, rejected_count, deduplicated_count, silver_count,
                        duration_seconds, watermark, error, run_id))
            # Advance control watermark only after validation, merge, and downstream handoff succeed.
            if status == "SUCCEEDED" and watermark is not None:
                db.execute("INSERT INTO watermarks(dataset,value) VALUES(?,?) ON CONFLICT(dataset) DO UPDATE SET value=excluded.value",
                           (row["dataset"], watermark))
            if status == "SUCCEEDED" and event_ids:
                db.executemany("INSERT OR IGNORE INTO event_ledger(dataset,event_id,first_run_id) VALUES(?,?,?)",
                               [(row["dataset"], event_id, run_id) for event_id in event_ids])
            db.commit()

