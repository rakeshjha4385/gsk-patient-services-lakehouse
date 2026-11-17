"""DB-API JDBC-equivalent connector boundary with parameterized incremental reads.

Production Databricks deployments can replace this with Spark's JDBC reader while
preserving the same SourceConnector contract and metadata semantics.
"""
from __future__ import annotations

import importlib
import os
from datetime import datetime, timezone

from ..model import IngestionBatch, TableMetadata
from .file import watermark_lower_bound


class JdbcConnector:
    def extract(self, metadata: TableMetadata, watermark: str | None) -> IngestionBatch:
        opts = metadata.source_options
        driver_module = opts["driver_module"]
        driver = importlib.import_module(driver_module)
        dsn_env = opts.get("dsn_env", "JDBC_DSN")
        dsn = os.environ.get(dsn_env)
        if not dsn:
            raise RuntimeError(f"Required JDBC connection environment variable {dsn_env!r} is not set")
        query = opts.get("incremental_query" if watermark else "full_query", opts.get("query"))
        if not query:
            raise ValueError("JDBC source metadata must configure full_query or query")
        params = {}
        if metadata.watermark_column and watermark:
            # Query must contain one named :watermark placeholder; drivers map it safely.
            if ":watermark" not in query:
                raise ValueError("Incremental JDBC query must use a :watermark parameter")
            params["watermark"] = watermark_lower_bound(
                watermark, int(opts.get("watermark_lookback_seconds", 0)))
        elif ":watermark" in query:
            raise ValueError("JDBC full_query must not contain a :watermark placeholder")
        connection = driver.connect(dsn)
        try:
            cursor = connection.cursor()
            cursor.execute(query, params)
            names = [col[0] for col in cursor.description]
            rows = [dict(zip(names, row)) for row in cursor.fetchall()]
            cursor.close()
        finally:
            connection.close()
        next_watermark = max((str(r[metadata.watermark_column]) for r in rows if metadata.watermark_column and r.get(metadata.watermark_column)), default=None)
        return IngestionBatch(rows, len(rows), datetime.now(timezone.utc), next_watermark)
