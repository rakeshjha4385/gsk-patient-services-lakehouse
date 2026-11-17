"""Create a deterministic synthetic patient-services JDBC source for pipeline runs."""
from __future__ import annotations

import random
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .synthetic import PROGRAMS, STATUSES


def seed_synthetic_jdbc(database_path: Path, patients: int = 250, seed: int = 42) -> int:
    if patients < 1:
        raise ValueError("patients must be positive")
    database_path.parent.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    rows = []
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for index in range(patients):
        updated = start + timedelta(minutes=index * 7)
        rows.append((f"SYN-{index + 1:06d}", rng.choice(PROGRAMS),
                     "INVALID_STATUS" if index == 0 else rng.choice(STATUSES),
                     int(index % 10 != 0), updated.isoformat(), 1))
    with sqlite3.connect(database_path) as db:
        db.execute("DROP TABLE IF EXISTS patient_services")
        db.execute("""CREATE TABLE patient_services (
            patient_id TEXT PRIMARY KEY, program TEXT NOT NULL, status TEXT NOT NULL,
            consent INTEGER NOT NULL, SystemModstamp TEXT NOT NULL, source_version INTEGER NOT NULL)""")
        db.executemany("INSERT INTO patient_services VALUES(?,?,?,?,?,?)", rows)
    return len(rows)
