"""CDC envelope normalization; supports INSERT/UPDATE/DELETE operations."""
from __future__ import annotations

from datetime import datetime, timezone

from .file import FileConnector
from ..model import IngestionBatch, TableMetadata


class CdcConnector:
    """Consumes a CDC envelope file fixture; Kafka/Event Hubs can implement the same protocol."""
    def extract(self, metadata: TableMetadata, watermark: str | None) -> IngestionBatch:
        batch = FileConnector().extract(metadata, watermark)
        operations = {"I": "INSERT", "U": "UPDATE", "D": "DELETE", "INSERT": "INSERT", "UPDATE": "UPDATE", "DELETE": "DELETE"}
        for row in batch.records:
            raw_op = str(row.get(metadata.operation_column, "")).upper()
            if raw_op not in operations:
                raise ValueError(f"Unsupported CDC operation {raw_op!r}")
            row[metadata.operation_column] = operations[raw_op]
        batch.extracted_at = datetime.now(timezone.utc)
        return batch
