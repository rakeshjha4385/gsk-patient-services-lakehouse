"""Structured quality, reconciliation, metrics, and audit artifact emission."""
from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from .model import RunResult


class RunMonitor:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        output_dir.mkdir(parents=True, exist_ok=True)

    def emit(self, result: RunResult, quality: dict, reconciliation: dict, audit: list[dict]) -> None:
        self._json("run.json", asdict(result))
        self._json("quality.json", quality)
        self._json("reconciliation.json", reconciliation)
        with (self.output_dir / "audit.jsonl").open("a", encoding="utf-8") as handle:
            for entry in audit:
                handle.write(json.dumps(entry, sort_keys=True, default=str) + "\n")

    def _json(self, name: str, payload: dict) -> None:
        (self.output_dir / name).write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
