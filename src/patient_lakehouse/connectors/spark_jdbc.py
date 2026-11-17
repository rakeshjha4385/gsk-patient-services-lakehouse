"""Spark JDBC reader adapter for bounded framework batches on Databricks."""
from __future__ import annotations

import os
from datetime import datetime, timezone

from ..model import IngestionBatch, TableMetadata
from .file import watermark_lower_bound


class SparkJdbcConnector:
    """Reads JDBC with Spark, returning bounded rows to the shared engine.

    This adapter is intended for synthetic/reference-sized jobs. For production
    scale, keep transformations as DataFrames through a distributed engine path.
    """
    def __init__(self, spark):
        self.spark = spark

    def extract(self, metadata: TableMetadata, watermark: str | None) -> IngestionBatch:
        from pyspark.sql import functions as F

        options = metadata.source_options
        if options.get("jdbc_url_env"):
            jdbc_url = os.environ.get(options["jdbc_url_env"], "")
            if not jdbc_url:
                raise RuntimeError(f"JDBC URL environment variable {options['jdbc_url_env']!r} is not set")
        else:
            jdbc_url = _resolve_env(options.get("jdbc_url", ""))
        if not jdbc_url:
            raise RuntimeError("JDBC source metadata must provide jdbc_url or jdbc_url_env")
        reader = (self.spark.read.format("jdbc").option("url", jdbc_url)
                  .option("driver", options["driver"]).option("dbtable", options["table"]))
        for key in ("user", "password", "fetchsize", "queryTimeout"):
            env_key = options.get(f"{key}_env")
            if env_key:
                value = os.environ.get(env_key)
                if not value:
                    raise RuntimeError(f"JDBC option environment variable {env_key!r} is not set")
                reader = reader.option(key, value)
            elif key in options:
                reader = reader.option(key, str(options[key]))
        frame = reader.load()
        if metadata.watermark_column and watermark:
            lower_bound = watermark_lower_bound(watermark, int(options.get("watermark_lookback_seconds", 0)))
            frame = frame.filter(F.col(metadata.watermark_column) > F.lit(lower_bound))
        max_records = int(options.get("max_driver_records", 5000))
        frame = frame.persist()
        try:
            aggregate = frame.agg(F.count(F.lit(1)).alias("n"),
                                  F.max(F.col(metadata.watermark_column)).alias("watermark")
                                  if metadata.watermark_column else F.lit(None).alias("watermark")).first()
            source_count = int(aggregate["n"])
            if source_count > max_records:
                raise RuntimeError(f"JDBC batch has {source_count} rows; max_driver_records={max_records}. Use a distributed Spark ingestion path for larger sources.")
            records = [row.asDict(recursive=True) for row in frame.toLocalIterator()]
        finally:
            frame.unpersist()
        for row in records:
            _coerce_record(row, options.get("data_types", {}))
        next_watermark = str(aggregate["watermark"]) if aggregate["watermark"] is not None else None
        return IngestionBatch(records, source_count, datetime.now(timezone.utc), next_watermark)


def _resolve_env(value: str) -> str:
    if value.startswith("env:"):
        name = value[4:]
        return os.environ.get(name, "")
    return value


def _coerce_record(row: dict, type_map: dict) -> None:
    for column, kind in type_map.items():
        value = row.get(column)
        if value is None:
            continue
        if kind == "integer":
            row[column] = int(value)
        elif kind == "number":
            row[column] = float(value)
        elif kind == "boolean":
            if isinstance(value, bool):
                continue
            normalized = str(value).strip().lower()
            if normalized not in {"true", "false", "1", "0"}:
                raise ValueError(f"Invalid JDBC boolean value for {column!r}")
            row[column] = normalized in {"true", "1"}
