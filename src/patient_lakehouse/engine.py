"""Generic metadata-driven ingestion orchestration and operational controls."""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from .connectors.base import ConnectorRegistry
from .control import ControlStore
from .metadata import MetadataRegistry
from .model import RunResult, TableMetadata
from .monitoring import RunMonitor
from .storage import LakehouseSink
from .validation import validate


class MetadataDrivenIngestion:
    """Coordinates extract → bronze → validate → silver → control → downstream.

    Source-specific behavior belongs in SourceConnector implementations; this engine
    owns shared run policy and advances watermarks only after every required step.
    """
    def __init__(self, registry: MetadataRegistry, connectors: ConnectorRegistry,
                 sink: LakehouseSink, control: ControlStore,
                 pii_key: bytes | None = None,
                 downstream: Callable[[TableMetadata, str], None] | None = None):
        self.registry, self.connectors, self.sink, self.control = registry, connectors, sink, control
        configured_key = os.environ.get("PII_HMAC_KEY")
        self.pii_key = pii_key or (configured_key.encode() if configured_key else None)
        self.downstream = downstream or (lambda _metadata, _run_id: None)

    def execute(self, dataset: str) -> RunResult:
        metadata = self.registry.get(dataset)
        started = datetime.now(timezone.utc)
        start_clock = time.monotonic()
        run_id = str(uuid.uuid4())
        started_at = started.isoformat()
        self.control.start_run(run_id, dataset, started_at)
        batch_count = accepted_count = rejected_count = deduplicated_count = silver_count = 0
        watermark = None
        try:
            connector = self.connectors.resolve(metadata.source_type)
            previous_watermark = self.control.get_watermark(dataset)
            batch = connector.extract(metadata, previous_watermark)
            batch_count = len(batch.records)
            enriched = [self._add_metadata(row, metadata, run_id, batch.extracted_at.isoformat()) for row in batch.records]
            # Bronze retains source-aligned values and operational metadata for replay/audit.
            self.sink.write_bronze(enriched, metadata.bronze_path)
            event_column = metadata.event_id_column
            previously_committed = self.control.seen_event_ids(dataset, [str(r.get(event_column, "")) for r in enriched]) if event_column else set()
            replay_filtered = [r for r in enriched if not event_column or str(r.get(event_column, "")) not in previously_committed]
            deduplicated_count = len(enriched) - len(replay_filtered)
            validation = validate(replay_filtered, metadata)
            accepted_count, rejected_count = len(validation.accepted), len(validation.rejected)
            reject_rate = rejected_count / max(batch_count, 1)
            quality_passed = reject_rate <= metadata.max_reject_rate
            rejected_path = Path(metadata.bronze_path).parent.parent / "quarantine" / f"{run_id}.jsonl"
            rejected_path.parent.mkdir(parents=True, exist_ok=True)
            with rejected_path.open("w", encoding="utf-8") as handle:
                for row in validation.rejected:
                    handle.write(json.dumps(row, sort_keys=True, default=str) + "\n")
            if not quality_passed:
                raise QualityGateError(f"Reject rate {reject_rate:.4f} exceeds threshold {metadata.max_reject_rate:.4f}")
            ordered = validation.accepted
            order_column = metadata.source_options.get("silver_order_column")
            if order_column:
                ordered = sorted(ordered, key=lambda row: _ordering_value(row.get(order_column), row.get(metadata.event_id_column or "")))
            curated = [self._protect_pii(row, metadata) for row in ordered]
            # Delta MERGE must receive at most one source row per business key.
            # Keep the newest CDC state deterministically before the sink runs.
            if order_column:
                latest = {}
                for row in curated:
                    latest[tuple(str(row[k]) for k in metadata.primary_key)] = row
                curated = list(latest.values())
            silver_count = self.sink.merge_silver(curated, metadata.silver_path,
                                                  metadata.primary_key, metadata.operation_column)
            self.downstream(metadata, run_id)
            # Never checkpoint past quarantined incremental rows. A replay is safe:
            # the event ledger skips committed events while rejected rows re-enter validation.
            watermark = previous_watermark if rejected_count else batch.next_watermark
            duration = time.monotonic() - start_clock
            completed = datetime.now(timezone.utc)
            result = RunResult(run_id, dataset, "SUCCEEDED", batch_count, accepted_count,
                               rejected_count, deduplicated_count, silver_count, duration, watermark)
            audit = [{"run_id": run_id, "dataset": dataset, "decision": "ACCEPTED", "event_id": row.get(metadata.event_id_column or "")}
                     for row in validation.accepted]
            quality = {"passed": True, "source_count": batch_count, "accepted_count": accepted_count,
                       "rejected_count": rejected_count, "reject_rate": reject_rate,
                       "rule_failures": validation.rule_counts, "threshold": metadata.max_reject_rate}
            reconciliation = {"source_count": batch_count, "accepted_count": accepted_count,
                              "rejected_count": rejected_count, "deduplicated_count": deduplicated_count,
                              "count_balanced": batch_count == accepted_count + rejected_count + deduplicated_count,
                              "silver_state_count": silver_count}
            RunMonitor(Path(metadata.bronze_path).parent.parent / "runs" / run_id).emit(result, quality, reconciliation, audit)
            self.control.finish_run(run_id, status="SUCCEEDED", completed_at=completed.isoformat(),
                                    source_count=batch_count, accepted_count=accepted_count,
                                    rejected_count=rejected_count, deduplicated_count=deduplicated_count,
                                    silver_count=silver_count,
                                    duration_seconds=duration, watermark=watermark,
                                    event_ids=[str(row[metadata.event_id_column]) for row in validation.accepted]
                                    if metadata.event_id_column else [])
            return result
        except Exception as exc:
            duration = time.monotonic() - start_clock
            completed = datetime.now(timezone.utc)
            self.control.finish_run(run_id, status="FAILED", completed_at=completed.isoformat(),
                                    source_count=batch_count, accepted_count=accepted_count,
                                    rejected_count=rejected_count, deduplicated_count=deduplicated_count,
                                    silver_count=silver_count,
                                    duration_seconds=duration, watermark=None, error=f"{type(exc).__name__}: {exc}")
            failed = RunResult(run_id, dataset, "FAILED", batch_count, accepted_count, rejected_count,
                               deduplicated_count, silver_count, duration, None, f"{type(exc).__name__}: {exc}")
            RunMonitor(Path(metadata.bronze_path).parent.parent / "runs" / run_id).emit(
                failed, {"passed": False, "source_count": batch_count, "accepted_count": accepted_count,
                         "rejected_count": rejected_count, "error": failed.error},
                {"passed": False}, [{"run_id": run_id, "dataset": dataset, "decision": "FAILED", "error": failed.error}])
            raise

    def _add_metadata(self, row: dict, metadata: TableMetadata, run_id: str, timestamp: str) -> dict:
        return {**row, "_ingestion_timestamp": timestamp, "_source_system": metadata.source_name,
                "_source_type": metadata.source_type, "_ingestion_mode": metadata.ingestion_mode,
                "_run_id": run_id, "_synthetic": bool(row.get("synthetic", False))}

    def _protect_pii(self, row: dict, metadata: TableMetadata) -> dict:
        protected = dict(row)
        for column in metadata.pii_columns:
            if protected.get(column) is None:
                continue
            if not self.pii_key:
                raise RuntimeError("Metadata declares PII columns but PII_HMAC_KEY is not configured")
            protected[column] = hmac.new(self.pii_key, str(protected[column]).encode(), hashlib.sha256).hexdigest()[:32]
        return protected


def _ordering_value(value, tie_breaker):
    try:
        primary = (0, float(value))
    except (TypeError, ValueError):
        primary = (1, str(value or ""))
    return primary, str(tie_breaker or "")


class QualityGateError(RuntimeError):
    pass
