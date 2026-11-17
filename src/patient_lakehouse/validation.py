"""Configuration-driven contract checks with quarantined row-level errors."""
from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any

from .model import TableMetadata, ValidationResult


def validate(records: list[dict[str, Any]], metadata: TableMetadata) -> ValidationResult:
    rejected, accepted = [], []
    counts: Counter[str] = Counter()
    seen: set[tuple[str, ...]] = set()
    for record in records:
        errors = []
        for column in metadata.required_columns:
            if column not in record:
                errors.append(f"MISSING_COLUMN:{column}")
            elif record[column] is None or record[column] == "":
                errors.append(f"NULL_VALUE:{column}")
        identity_columns = ((metadata.event_id_column,) if metadata.ingestion_mode == "CDC" and metadata.event_id_column
                            else metadata.primary_key)
        key_values = tuple(str(record.get(key, "")) for key in identity_columns)
        if any(not v for v in key_values):
            errors.append("NULL_PRIMARY_KEY")
        elif key_values in seen:
            errors.append("DUPLICATE_PRIMARY_KEY")
        else:
            seen.add(key_values)
        for column, allowed in metadata.allowed_values.items():
            if record.get(column) not in allowed:
                errors.append(f"INVALID_VALUE:{column}")
        for column, expected in metadata.source_options.get("data_types", {}).items():
            if column in record and not _valid_type(record[column], expected):
                errors.append(f"INVALID_TYPE:{column}:{expected}")
        event_ts = record.get(metadata.watermark_column or "")
        if event_ts is not None and metadata.source_options.get("validate_timestamp", False):
            try:
                datetime.fromisoformat(str(event_ts).replace("Z", "+00:00"))
            except ValueError:
                errors.append("INVALID_TIMESTAMP")
        if errors:
            counts.update(errors)
            rejected.append({"record": record, "errors": errors})
        else:
            accepted.append(record)
    return ValidationResult(accepted, rejected, dict(counts))


def _valid_type(value: Any, expected: str) -> bool:
    if expected == "string":
        return isinstance(value, str)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "timestamp":
        try:
            datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return True
        except ValueError:
            return False
    raise ValueError(f"Unsupported metadata data type {expected!r}")
