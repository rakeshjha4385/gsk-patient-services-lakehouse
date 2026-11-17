"""Metadata registry loading and contract validation."""
from __future__ import annotations

import json
import os
from pathlib import Path

from .model import TableMetadata


class MetadataRegistry:
    def __init__(self, metadata_dir: Path):
        self.metadata_dir = metadata_dir

    def active_tables(self) -> list[TableMetadata]:
        tables = []
        for path in sorted(self.metadata_dir.glob("*.json")):
            raw = json.loads(path.read_text(encoding="utf-8"))
            if raw.get("active", True):
                raw.pop("active", None)
                for key in ("bronze_path", "silver_path"):
                    raw[key] = os.path.expandvars(raw[key])
                tables.append(TableMetadata.from_dict(raw))
        return tables

    def get(self, dataset: str) -> TableMetadata:
        for table in self.active_tables():
            if table.dataset == dataset:
                return table
        raise KeyError(f"No active metadata for dataset {dataset!r}")

