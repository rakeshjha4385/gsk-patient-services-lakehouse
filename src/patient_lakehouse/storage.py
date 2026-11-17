"""Storage sinks: portable local reference and optional Delta Lake implementation."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Protocol


class LakehouseSink(Protocol):
    def write_bronze(self, rows: list[dict[str, Any]], path: str) -> None: ...
    def merge_silver(self, rows: list[dict[str, Any]], path: str, keys: tuple[str, ...], operation_column: str | None) -> int: ...


class LocalJsonSink:
    """Local append-only JSONL layers for repeatable development and CI."""
    def write_bronze(self, rows: list[dict[str, Any]], path: str) -> None:
        target = Path(path) / "records.jsonl"
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("a", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, sort_keys=True, default=str) + "\n")

    def merge_silver(self, rows: list[dict[str, Any]], path: str, keys: tuple[str, ...], operation_column: str | None) -> int:
        target = Path(path) / "records.jsonl"
        target.parent.mkdir(parents=True, exist_ok=True)
        current = {}
        if target.exists():
            with target.open(encoding="utf-8") as handle:
                for line in handle:
                    row = json.loads(line)
                    current[tuple(str(row[k]) for k in keys)] = row
        for row in rows:
            key = tuple(str(row[k]) for k in keys)
            if operation_column and str(row.get(operation_column, "")).upper() == "DELETE":
                current.pop(key, None)
            else:
                current[key] = row
        with target.open("w", encoding="utf-8") as handle:
            for _, row in sorted(current.items()):
                handle.write(json.dumps(row, sort_keys=True, default=str) + "\n")
        return len(current)


class DeltaLakeSink:
    """Delta MERGE sink for Spark runtimes with delta-spark installed."""
    def __init__(self, spark):
        self.spark = spark

    def write_bronze(self, rows: list[dict[str, Any]], path: str) -> None:
        if not rows:
            return
        self.spark.createDataFrame(rows).write.format("delta").mode("append").option("mergeSchema", "true").save(path)

    def merge_silver(self, rows: list[dict[str, Any]], path: str, keys: tuple[str, ...], operation_column: str | None) -> int:
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F
        if not rows:
            return self.spark.read.format("delta").load(path).count() if DeltaTable.isDeltaTable(self.spark, path) else 0
        source = self.spark.createDataFrame(rows).dropDuplicates(list(keys))
        if not DeltaTable.isDeltaTable(self.spark, path):
            if operation_column:
                source = source.filter(F.upper(F.col(operation_column)) != "DELETE")
            source.write.format("delta").mode("overwrite").save(path)
            return self.spark.read.format("delta").load(path).count()
        target = DeltaTable.forPath(self.spark, path)
        condition = " AND ".join(f"t.`{key}` = s.`{key}`" for key in keys)
        merge = target.alias("t").merge(source.alias("s"), condition)
        if operation_column:
            merge = merge.whenMatchedDelete(condition=f"upper(s.`{operation_column}`) = 'DELETE'")
            merge = merge.whenMatchedUpdateAll(condition=f"upper(s.`{operation_column}`) <> 'DELETE'")
            merge = merge.whenNotMatchedInsertAll(condition=f"upper(s.`{operation_column}`) <> 'DELETE'")
        else:
            merge = merge.whenMatchedUpdateAll().whenNotMatchedInsertAll()
        merge.execute()
        return self.spark.read.format("delta").load(path).count()
