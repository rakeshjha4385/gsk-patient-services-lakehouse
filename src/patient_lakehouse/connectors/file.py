"""CSV and JSONL batch-file connector."""
from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timezone
from datetime import timedelta
from pathlib import Path

from ..model import IngestionBatch, TableMetadata


class FileConnector:
    def extract(self, metadata: TableMetadata, watermark: str | None) -> IngestionBatch:
        raw_path = str(metadata.source_options["path"])
        if raw_path.startswith("env:"):
            env_name = raw_path[4:]
            if not os.environ.get(env_name):
                raise RuntimeError(f"Required source path environment variable {env_name!r} is not set")
            raw_path = os.environ[env_name]
        path = Path(raw_path)
        fmt = metadata.source_options.get("format", path.suffix.lstrip(".")).lower()
        if fmt in {"jsonl", "ndjson"}:
            with path.open(encoding="utf-8") as handle:
                rows = [json.loads(line) for line in handle if line.strip()]
        elif fmt == "csv":
            with path.open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
        elif fmt == "json":
            payload = json.loads(path.read_text(encoding="utf-8"))
            rows = payload if isinstance(payload, list) else payload[metadata.source_options.get("records_key", "records")]
        else:
            raise ValueError(f"Unsupported file format {fmt!r}")
        rows = _filter_watermark(rows, metadata, watermark)
        next_watermark = _max_watermark(rows, metadata)
        return IngestionBatch(rows, len(rows), datetime.now(timezone.utc), next_watermark)


def _filter_watermark(rows: list[dict], metadata: TableMetadata, watermark: str | None) -> list[dict]:
    column = metadata.watermark_column
    if not column or not watermark:
        return rows
    lower_bound = watermark_lower_bound(watermark, int(metadata.source_options.get("watermark_lookback_seconds", 0)))
    try:
        bound = datetime.fromisoformat(lower_bound.replace("Z", "+00:00"))
        return [row for row in rows if _after(row.get(column), bound)]
    except (TypeError, ValueError):
        try:
            return [row for row in rows if float(row.get(column)) > float(lower_bound)]
        except (TypeError, ValueError):
            return [row for row in rows if str(row.get(column, "")) > str(lower_bound)]


def _after(value, bound: datetime) -> bool:
    try:
        candidate = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return candidate > bound
    except (TypeError, ValueError):
        return False


def watermark_lower_bound(watermark: str, lookback_seconds: int) -> str:
    if not lookback_seconds:
        return watermark
    try:
        return (datetime.fromisoformat(watermark.replace("Z", "+00:00")) - timedelta(seconds=lookback_seconds)).isoformat()
    except ValueError:
        try:
            return str(float(watermark) - lookback_seconds)
        except ValueError:
            return watermark


def _max_watermark(rows: list[dict], metadata: TableMetadata) -> str | None:
    column = metadata.watermark_column
    values = [str(row[column]) for row in rows if column and row.get(column) is not None]
    return max(values) if values else None
