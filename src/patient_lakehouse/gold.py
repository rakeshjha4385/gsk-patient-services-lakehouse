"""Local reference publisher for a consent-filtered patient-services product."""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from .model import TableMetadata


class GoldProductPublisher:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir

    def __call__(self, metadata: TableMetadata, run_id: str) -> None:
        silver_path = Path(metadata.silver_path) / "records.jsonl"
        if not silver_path.exists():
            raise FileNotFoundError(f"Silver product not found: {silver_path}")
        counts: Counter[tuple[str, str]] = Counter()
        with silver_path.open(encoding="utf-8") as handle:
            for line in handle:
                row = json.loads(line)
                if metadata.outbound_requires_consent and not _consented(row.get("consent")):
                    continue
                counts[(str(row.get("program", "UNKNOWN")), str(row.get("status", "UNKNOWN")))] += 1
        target = self.output_dir / "program_status.jsonl"
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w", encoding="utf-8") as handle:
            for (program, status), count in sorted(counts.items()):
                handle.write(json.dumps({"program": program, "status": status, "patient_count": count,
                                         "_run_id": run_id}, sort_keys=True) + "\n")


def _consented(value) -> bool:
    return value is True or str(value).strip().lower() in {"true", "1", "yes"}
