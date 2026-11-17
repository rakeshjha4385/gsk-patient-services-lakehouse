"""Metadata-driven local pipeline with idempotency and auditable controls."""
from __future__ import annotations

import csv
import hashlib
import hmac
import json
import secrets
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def _write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def run(input_dir: Path, output_dir: Path, config_path: Path) -> dict[str, Any]:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    source = input_dir / config["source_file"]
    raw = _read_jsonl(source)
    output_dir.mkdir(parents=True, exist_ok=True)
    secret = secrets.token_bytes(32)  # demonstration only; use Key Vault in production
    seen: set[str] = set()
    accepted: list[dict[str, Any]] = []
    rejects: list[dict[str, Any]] = []
    duplicates = 0
    audit: list[dict[str, Any]] = []
    required = set(config["required_fields"])
    for row in raw:
        eid = str(row.get("event_id", ""))
        if not eid or eid in seen:
            duplicates += int(bool(eid and eid in seen))
            rejects.append({"event_id": eid or None, "reason": "MISSING_EVENT_ID" if not eid else "DUPLICATE_EVENT_ID"})
            continue
        seen.add(eid)
        missing = required - row.keys()
        if missing:
            rejects.append({"event_id": eid, "reason": "MISSING_FIELDS:" + ",".join(sorted(missing))})
            continue
        if row["status"] not in config["allowed_statuses"] or row["program"] not in config["allowed_programs"]:
            rejects.append({"event_id": eid, "reason": "CONTRACT_VIOLATION"})
            continue
        try:
            datetime.fromisoformat(row["event_ts"].replace("Z", "+00:00"))
        except (TypeError, ValueError):
            rejects.append({"event_id": eid, "reason": "INVALID_EVENT_TIMESTAMP"})
            continue
        accepted.append(row)
        audit.append({"event_id": eid, "decision": "ACCEPTED", "event_ts": row["event_ts"]})

    # Latest source version wins deterministically; never emit non-consented records.
    latest: dict[str, dict[str, Any]] = {}
    for row in accepted:
        old = latest.get(row["patient_id"])
        key = (int(row["source_version"]), row["event_ts"], row["event_id"])
        if old is None or key > (int(old["source_version"]), old["event_ts"], old["event_id"]):
            latest[row["patient_id"]] = row

    silver: list[dict[str, Any]] = []
    for pid, row in sorted(latest.items()):
        token = hmac.new(secret, pid.encode(), hashlib.sha256).hexdigest()[:24]
        silver.append({"patient_key": token, "program": row["program"], "status": row["status"],
                       "consent": str(bool(row["consent"])).lower(), "event_ts": row["event_ts"],
                       "source_version": row["source_version"]})
        audit.append({"patient_key": token, "decision": "CONSENTED" if row["consent"] else "SUPPRESSED_NO_CONSENT",
                      "event_ts": row["event_ts"]})
    gold_counts = Counter((r["program"], r["status"]) for r in silver if r["consent"] == "true")
    gold = [{"program": p, "status": s, "patient_count": count} for (p, s), count in sorted(gold_counts.items())]
    outbound = [{"idempotency_key": hashlib.sha256((r["patient_key"] + r["event_ts"]).encode()).hexdigest(),
                 "patient_key": r["patient_key"], "program": r["program"], "status": r["status"],
                 "event_ts": r["event_ts"]} for r in silver if r["consent"] == "true"]

    accepted_rows = len(accepted)
    reject_rate = len(rejects) / max(len(raw), 1)
    quality = {"source_rows": len(raw), "accepted_events": accepted_rows, "rejected_events": len(rejects),
               "duplicate_events": duplicates, "latest_patients": len(silver),
               "consented_patients": len(outbound), "reject_rate": round(reject_rate, 4),
               "passed": reject_rate <= config["max_reject_rate"]}
    expected = len({r["patient_id"] for r in accepted})
    reconciliation = {"accepted_distinct_patients": expected, "silver_patients": len(silver),
                      "delta": len(silver) - expected, "passed": expected == len(silver)}
    _write_csv(output_dir / "bronze" / "events.csv", raw,
               ["event_id", "patient_id", "program", "status", "event_type", "consent", "event_ts", "source_version", "synthetic"])
    _write_csv(output_dir / "silver" / "patients.csv", silver,
               ["patient_key", "program", "status", "consent", "event_ts", "source_version"])
    _write_csv(output_dir / "gold" / "program_status.csv", gold, ["program", "status", "patient_count"])
    _write_csv(output_dir / "outbound" / "salesforce_change_feed.csv", outbound,
               ["idempotency_key", "patient_key", "program", "status", "event_ts"])
    _write_csv(output_dir / "quarantine" / "rejected.csv", rejects, ["event_id", "reason"])
    (output_dir / "quality.json").write_text(json.dumps(quality, indent=2) + "\n", encoding="utf-8")
    (output_dir / "reconciliation.json").write_text(json.dumps(reconciliation, indent=2) + "\n", encoding="utf-8")
    metrics = {"run_id": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"), "source_rows": len(raw),
               "accepted_events": accepted_rows, "rejected_events": len(rejects), "silver_patients": len(silver),
               "gold_groups": len(gold), "outbound_records": len(outbound), "source_file": source.name}
    (output_dir / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")
    with (output_dir / "audit.jsonl").open("w", encoding="utf-8") as f:
        for entry in audit:
            f.write(json.dumps(entry, sort_keys=True) + "\n")
    return {"quality": quality, "reconciliation": reconciliation, "metrics": metrics}
