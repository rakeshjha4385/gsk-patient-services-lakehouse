"""Bounded micro-batch connector for file-backed stream fixtures."""
from __future__ import annotations

from datetime import datetime, timezone

from .file import FileConnector
from ..model import IngestionBatch, TableMetadata


class StreamingConnector:
    """Reference micro-batch source; production uses Event Hubs/Kafka offsets."""
    def extract(self, metadata: TableMetadata, watermark: str | None) -> IngestionBatch:
        batch = FileConnector().extract(metadata, watermark)
        limit = int(metadata.source_options.get("max_records_per_batch", 1000))
        batch.records = batch.records[:limit]
        batch.source_count = len(batch.records)
        batch.next_watermark = max((str(row[metadata.watermark_column]) for row in batch.records
                                    if metadata.watermark_column and row.get(metadata.watermark_column)), default=None)
        batch.extracted_at = datetime.now(timezone.utc)
        return batch
