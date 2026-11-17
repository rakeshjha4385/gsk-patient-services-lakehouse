"""Generate deterministic, fictional patient-services CDC events."""
from __future__ import annotations

import json
import random
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

PROGRAMS = ("RESPIRA", "IMMUNO", "ONCOLOGY")
STATUSES = ("ENROLLED", "CONTACTED", "SERVICE_ACTIVE", "COMPLETED")


def generate(output: Path, patients: int = 100, days: int = 7, seed: int = 42) -> int:
    if patients < 1 or days < 1:
        raise ValueError("patients and days must be positive")
    rng = random.Random(seed)
    output.mkdir(parents=True, exist_ok=True)
    path = output / "pmp_cdc.jsonl"
    start = date(2026, 1, 1)
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with path.open("w", encoding="utf-8") as f:
        for i in range(patients):
            pid = f"SYN-{i + 1:06d}"
            n_events = rng.randint(1, min(days, 4))
            for seq in range(n_events):
                event_day = start + timedelta(days=rng.randrange(days))
                timestamp = now + timedelta(days=(event_day - start).days, minutes=i * 5 + seq)
                consent = rng.random() > 0.08
                row = {
                    "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"gsk-demo:{seed}:{pid}:{seq}")),
                    "patient_id": pid,
                    "program": rng.choice(PROGRAMS),
                    "status": rng.choice(STATUSES),
                    "event_type": "UPSERT",
                    "consent": consent,
                    "event_ts": timestamp.isoformat(),
                    "source_version": seq + 1,
                    "synthetic": True,
                }
                # Inject a small number of intentional contract failures.
                if i % 97 == 0 and seq == 0:
                    row["status"] = "UNKNOWN_STATUS"
                f.write(json.dumps(row, sort_keys=True) + "\n")
    return patients
