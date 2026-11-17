"""Contracts shared by metadata, connectors, storage, and orchestration."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import re
from typing import Any


SUPPORTED_SOURCES = {"REST_API", "SALESFORCE_BULK_API", "JDBC", "FILE", "CDC", "STREAMING"}
SUPPORTED_MODES = {"FULL", "INCREMENTAL", "CDC", "STREAMING"}


@dataclass(frozen=True)
class TableMetadata:
    dataset: str
    source_name: str
    source_type: str
    ingestion_mode: str
    bronze_path: str
    silver_path: str
    primary_key: tuple[str, ...]
    required_columns: tuple[str, ...]
    source_options: dict[str, Any] = field(default_factory=dict)
    watermark_column: str | None = None
    event_id_column: str | None = None
    operation_column: str | None = None
    allowed_values: dict[str, tuple[str, ...]] = field(default_factory=dict)
    pii_columns: tuple[str, ...] = ()
    outbound_requires_consent: bool = False
    max_reject_rate: float = 0.02
    retries: int = 3

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "TableMetadata":
        data = dict(raw)
        for name in ("primary_key", "required_columns", "pii_columns"):
            data[name] = tuple(data.get(name, ()))
        data["allowed_values"] = {k: tuple(v) for k, v in data.get("allowed_values", {}).items()}
        metadata = cls(**data)
        metadata.validate()
        return metadata

    def validate(self) -> None:
        if self.source_type not in SUPPORTED_SOURCES:
            raise ValueError(f"Unsupported source_type {self.source_type!r}; expected one of {sorted(SUPPORTED_SOURCES)}")
        if self.ingestion_mode not in SUPPORTED_MODES:
            raise ValueError(f"Unsupported ingestion_mode {self.ingestion_mode!r}; expected one of {sorted(SUPPORTED_MODES)}")
        if not self.dataset or not self.source_name or not self.primary_key:
            raise ValueError("dataset, source_name, and primary_key are required")
        for name in (*self.primary_key, *self.required_columns, *self.pii_columns,
                     *((self.watermark_column,) if self.watermark_column else ())):
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
                raise ValueError(f"Invalid metadata column identifier {name!r}")
        if self.ingestion_mode == "INCREMENTAL" and not self.watermark_column:
            raise ValueError("INCREMENTAL mode requires watermark_column")
        if self.ingestion_mode == "CDC" and not (self.event_id_column and self.operation_column):
            raise ValueError("CDC mode requires event_id_column and operation_column")
        if self.ingestion_mode == "CDC" and not self.source_options.get("silver_order_column"):
            raise ValueError("CDC mode requires source_options.silver_order_column for deterministic Silver merges")
        if not 0 <= self.max_reject_rate <= 1:
            raise ValueError("max_reject_rate must be between 0 and 1")
        if self.retries < 0 or self.retries > 10:
            raise ValueError("retries must be between 0 and 10")


@dataclass
class IngestionBatch:
    records: list[dict[str, Any]]
    source_count: int
    extracted_at: datetime
    next_watermark: str | None = None
    source_offset: str | None = None


@dataclass
class ValidationResult:
    accepted: list[dict[str, Any]]
    rejected: list[dict[str, Any]]
    rule_counts: dict[str, int]


@dataclass
class RunResult:
    run_id: str
    dataset: str
    status: str
    source_count: int
    accepted_count: int
    rejected_count: int
    deduplicated_count: int
    silver_count: int
    duration_seconds: float
    watermark: str | None
    error: str | None = None

