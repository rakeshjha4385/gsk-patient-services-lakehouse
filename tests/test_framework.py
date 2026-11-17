from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

from patient_lakehouse.connectors import default_connectors
from patient_lakehouse.connectors.salesforce_bulk import SalesforceBulkApiConnector
from patient_lakehouse.control import ControlStore
from patient_lakehouse.engine import MetadataDrivenIngestion
from patient_lakehouse.gold import GoldProductPublisher
from patient_lakehouse.metadata import MetadataRegistry
from patient_lakehouse.model import TableMetadata
from patient_lakehouse.storage import LocalJsonSink
from patient_lakehouse.synthetic import generate
from patient_lakehouse.synthetic_jdbc import seed_synthetic_jdbc

ROOT = Path(__file__).parents[1]


def metadata(source_type: str, mode: str, options: dict, **overrides) -> TableMetadata:
    raw = {"dataset": "test", "source_name": "synthetic-test", "source_type": source_type,
           "ingestion_mode": mode, "bronze_path": "/tmp/bronze", "silver_path": "/tmp/silver",
           "primary_key": ["id"], "required_columns": ["id"], "source_options": options,
           **overrides}
    return TableMetadata.from_dict(raw)


class FrameworkTests(unittest.TestCase):
    def test_connector_registry_covers_api_jdbc_file_cdc_streaming(self):
        registry = default_connectors()
        for source_type in ("REST_API", "SALESFORCE_BULK_API", "JDBC", "FILE", "CDC", "STREAMING"):
            self.assertIsNotNone(registry.resolve(source_type))

    def test_rest_api_connector_contract(self):
        class Response:
            def __enter__(self): return self
            def __exit__(self, *_args): return False
            def read(self): return json.dumps({"records": [{"id": "api-1", "updated_at": "2026-01-02"}], "has_more": False}).encode()
        table = metadata("REST_API", "INCREMENTAL", {"url": "https://api.example.invalid/records"},
                         watermark_column="updated_at")
        with patch("patient_lakehouse.connectors.rest_api.urlopen", return_value=Response()):
            result = default_connectors().resolve("REST_API").extract(table, "2026-01-01")
        self.assertEqual(result.records[0]["id"], "api-1")
        self.assertEqual(result.next_watermark, "2026-01-02")

    def test_salesforce_bulk_api_job_and_locator_flow(self):
        class FakeSalesforce(SalesforceBulkApiConnector):
            def __init__(self): self.calls, self.pages = [], 0
            def _json_request(self, method, url, headers, payload, options):
                self.calls.append((method, url, payload))
                return {"id": "750xx", "state": "JobComplete"} if method == "POST" else {"state": "JobComplete"}
            def _csv_page(self, url, headers, options):
                self.pages += 1
                if self.pages == 1:
                    return ([{"Id": "p1", "SystemModstamp": "2026-01-03T00:00:00Z", "Consent__c": "true"}], "next page")
                return ([{"Id": "p2", "SystemModstamp": "2026-01-04T00:00:00Z", "Consent__c": "false"}], None)

        os.environ["TEST_SF_TOKEN"] = "synthetic-test-token"
        try:
            options = {"instance_url": "https://example.my.salesforce.com", "token_env": "TEST_SF_TOKEN",
                       "object": "Patient__c", "fields": ["Id", "SystemModstamp", "Consent__c"],
                       "data_types": {"Consent__c": "boolean"}, "watermark_lookback_seconds": 60}
            table = metadata("SALESFORCE_BULK_API", "INCREMENTAL", options,
                             primary_key=["Id"], required_columns=["Id", "SystemModstamp"],
                             watermark_column="SystemModstamp")
            connector = FakeSalesforce()
            result = connector.extract(table, "2026-01-02T00:00:00Z")
            soql = connector.calls[0][2]["query"]
            self.assertIn("FROM Patient__c", soql)
            self.assertIn("SystemModstamp > '2026-01-01T23:59:00+00:00'", soql)
            self.assertEqual(len(result.records), 2)
            self.assertIs(result.records[0]["Consent__c"], True)
            self.assertIs(result.records[1]["Consent__c"], False)
        finally:
            os.environ.pop("TEST_SF_TOKEN", None)

    def test_jdbc_connector_uses_parameterized_incremental_query(self):
        with tempfile.TemporaryDirectory() as temp:
            db_path = str(Path(temp) / "source.db")
            with sqlite3.connect(db_path) as db:
                db.execute("create table source(id text, modified_at text)")
                db.execute("insert into source values(?,?)", ("db-1", "2026-01-02"))
            os.environ["TEST_JDBC_DSN"] = db_path
            try:
                table = metadata("JDBC", "INCREMENTAL", {
                    "driver_module": "sqlite3", "dsn_env": "TEST_JDBC_DSN",
                    "query": "select id, modified_at from source where modified_at > :watermark"},
                    watermark_column="modified_at")
                result = default_connectors().resolve("JDBC").extract(table, "2026-01-01")
                self.assertEqual([row["id"] for row in result.records], ["db-1"])
            finally:
                os.environ.pop("TEST_JDBC_DSN", None)

    def test_file_cdc_and_streaming_connectors(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "events.jsonl"
            rows = [{"id": "one", "event_id": "e1", "op": "I", "event_time": "2026-01-01"},
                    {"id": "two", "event_id": "e2", "op": "U", "event_time": "2026-01-02"}]
            path.write_text("".join(json.dumps(row) + "\n" for row in rows))
            options = {"path": str(path), "format": "jsonl", "max_records_per_batch": 1}
            file_table = metadata("FILE", "FULL", options)
            self.assertEqual(len(default_connectors().resolve("FILE").extract(file_table, None).records), 2)
            cdc_table = metadata("CDC", "CDC", {**options, "silver_order_column": "event_time"},
                                 event_id_column="event_id", operation_column="op")
            cdc = default_connectors().resolve("CDC").extract(cdc_table, None)
            self.assertEqual([row["op"] for row in cdc.records], ["INSERT", "UPDATE"])
            stream_table = metadata("STREAMING", "STREAMING", options, watermark_column="event_time")
            stream = default_connectors().resolve("STREAMING").extract(stream_table, None)
            self.assertEqual(len(stream.records), 1)

    def test_synthetic_cdc_framework_replay_and_operational_state(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            input_path = base / "input/pmp_cdc.jsonl"
            generate(input_path.parent, patients=120, days=9, seed=8)
            os.environ["PMP_CDC_FIXTURE"] = str(input_path)
            os.environ["LAKEHOUSE_ROOT"] = str(base / "lake")
            try:
                engine = MetadataDrivenIngestion(
                    MetadataRegistry(ROOT / "config/metadata"), default_connectors(), LocalJsonSink(),
                    ControlStore(base / "control.sqlite"), pii_key=b"unit-test-only",
                    downstream=lambda _metadata, _run_id: None)
                first = engine.execute("pmp_patient_services")
                self.assertEqual(first.status, "SUCCEEDED")
                self.assertGreater(first.source_count, first.accepted_count)
                self.assertEqual(first.accepted_count + first.rejected_count, first.source_count)
                self.assertIsNone(first.watermark)
                self.assertIsNone(ControlStore(base / "control.sqlite").get_watermark("pmp_patient_services"))
                self.assertGreater(first.silver_count, 0)
                replay = engine.execute("pmp_patient_services")
                self.assertEqual(replay.accepted_count, 0)
                self.assertEqual(replay.deduplicated_count, first.accepted_count)
                self.assertEqual(replay.silver_count, first.silver_count)
                self.assertTrue((base / "lake/runs" / first.run_id / "quality.json").exists())
            finally:
                os.environ.pop("PMP_CDC_FIXTURE", None)
                os.environ.pop("LAKEHOUSE_ROOT", None)

    def test_synthetic_jdbc_runs_quality_silver_and_gold_pipeline(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            database = base / "source/patients.sqlite"
            seed_synthetic_jdbc(database, patients=150, seed=17)
            os.environ["SYNTHETIC_JDBC_DSN"] = str(database)
            os.environ["LAKEHOUSE_ROOT"] = str(base / "lake")
            try:
                engine = MetadataDrivenIngestion(
                    MetadataRegistry(ROOT / "config/metadata"), default_connectors(), LocalJsonSink(),
                    ControlStore(base / "control.sqlite"), pii_key=b"jdbc-test-only",
                    downstream=GoldProductPublisher(base / "lake/gold"))
                result = engine.execute("jdbc_synthetic_patient_services")
                self.assertEqual(result.status, "SUCCEEDED")
                self.assertEqual((result.source_count, result.accepted_count, result.rejected_count), (150, 149, 1))
                self.assertEqual(result.silver_count, 149)
                self.assertIsNone(result.watermark)  # reject remains replayable until corrected
                gold = (base / "lake/gold/program_status.jsonl").read_text()
                self.assertNotIn("SYN-", gold)
                self.assertTrue((base / "lake/quarantine").is_dir())
            finally:
                os.environ.pop("SYNTHETIC_JDBC_DSN", None)
                os.environ.pop("LAKEHOUSE_ROOT", None)

    def test_metadata_rejects_incremental_without_watermark(self):
        raw = {"dataset": "x", "source_name": "x", "source_type": "FILE", "ingestion_mode": "INCREMENTAL",
               "bronze_path": "b", "silver_path": "s", "primary_key": ["id"], "required_columns": ["id"]}
        with self.assertRaises(ValueError):
            TableMetadata.from_dict(raw)


if __name__ == "__main__":
    unittest.main()
