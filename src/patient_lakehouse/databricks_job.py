"""Databricks wheel-task entry point for the synthetic JDBC workflow."""
from __future__ import annotations

import json
import os
import re
import sqlite3
from pathlib import Path

from .connectors import default_connectors
from .connectors.spark_jdbc import SparkJdbcConnector
from .control import ControlStore
from .engine import MetadataDrivenIngestion
from .metadata import MetadataRegistry
from .model import TableMetadata
from .storage import DeltaLakeSink
from .synthetic_jdbc import seed_synthetic_jdbc


def main(catalog: str = "main", schema: str = "default", volume: str = "synthetic_ingestion",
         patients: str = "250", seed: str = "42") -> None:
    """Seed a synthetic SQLite JDBC source, then run the configured Delta pipeline."""
    for name, value in (("catalog", catalog), ("schema", schema), ("volume", volume)):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            raise ValueError(f"Invalid Unity Catalog {name}: {value!r}")
    try:
        patient_count, seed_value = int(patients), int(seed)
    except ValueError as exc:
        raise ValueError("patients and seed must be integers") from exc
    volume_root = Path("/Volumes") / catalog / schema / volume / "patient-lakehouse"
    if not (Path("/Volumes") / catalog / schema / volume).is_dir():
        raise FileNotFoundError(f"Unity Catalog volume does not exist or is not mounted: /Volumes/{catalog}/{schema}/{volume}")
    volume_root.mkdir(parents=True, exist_ok=True)
    state_root = volume_root / "lakehouse"
    source_db = volume_root / "source" / "patients.sqlite"
    seed_synthetic_jdbc(source_db, patient_count, seed_value)
    os.environ["LAKEHOUSE_ROOT"] = str(state_root)
    os.environ["SYNTHETIC_JDBC_URL"] = f"jdbc:sqlite:{source_db}"
    os.environ["PII_HMAC_KEY"] = "synthetic-records-only-key-set-via-workspace-secret-in-production"

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    metadata_dir = Path(__file__).resolve().parent / "resources" / "metadata"
    registry = MetadataRegistry(metadata_dir)
    connectors = default_connectors()
    connectors.register("JDBC", SparkJdbcConnector(spark))
    metadata = registry.get("jdbc_synthetic_patient_services")
    control = ControlStore(state_root / "control.sqlite")

    def publish_gold(table_metadata: TableMetadata, run_id: str) -> None:
        from pyspark.sql import functions as F

        silver = spark.read.format("delta").load(table_metadata.silver_path)
        if table_metadata.outbound_requires_consent:
            silver = silver.filter(F.col("consent").cast("boolean"))
        gold = (silver.groupBy("program", "status").count()
                .withColumnRenamed("count", "patient_count")
                .withColumn("_run_id", F.lit(run_id)))
        (gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
         .save(str(state_root / "gold" / table_metadata.dataset / "program_status")))

    engine = MetadataDrivenIngestion(registry, connectors, DeltaLakeSink(spark), control,
                                     pii_key=b"synthetic-records-only-local-key", downstream=publish_gold)
    result = engine.execute("jdbc_synthetic_patient_services")
    print(json.dumps({**result.__dict__, "source_database": str(source_db),
                      "lakehouse_path": str(state_root)}, indent=2))
    if result.status != "SUCCEEDED":
        raise RuntimeError(f"JDBC ingestion run failed: {result.error}")
