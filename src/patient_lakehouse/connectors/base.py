from __future__ import annotations

from typing import Protocol

from ..model import IngestionBatch, TableMetadata


class SourceConnector(Protocol):
    def extract(self, metadata: TableMetadata, watermark: str | None) -> IngestionBatch: ...


class ConnectorRegistry:
    def __init__(self):
        self._connectors: dict[str, SourceConnector] = {}

    def register(self, source_type: str, connector: SourceConnector) -> None:
        self._connectors[source_type] = connector

    def resolve(self, source_type: str) -> SourceConnector:
        try:
            return self._connectors[source_type]
        except KeyError as exc:
            raise ValueError(f"No connector registered for source type {source_type!r}") from exc
