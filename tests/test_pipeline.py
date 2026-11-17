import json
import tempfile
import unittest
from pathlib import Path

from lakehouse_demo.agent import diagnose
from lakehouse_demo.pipeline import run
from lakehouse_demo.synthetic import generate

CONFIG = Path(__file__).parents[1] / "config" / "pmp_patient_services.json"


class PipelineTests(unittest.TestCase):
    def test_synthetic_pipeline_and_controls(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            generate(base / "input", patients=150, days=10, seed=7)
            result = run(base / "input", base / "run", CONFIG)
            self.assertTrue(result["quality"]["passed"])
            self.assertTrue(result["reconciliation"]["passed"])
            self.assertGreater(result["quality"]["rejected_events"], 0)
            outbound = (base / "run/outbound/salesforce_change_feed.csv").read_text()
            self.assertNotIn("SYN-", outbound)
            report = diagnose(base / "run")
            self.assertFalse(report["autonomous_mutations"])
            self.assertTrue(report["review_required"])

    def test_seed_is_repeatable(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            generate(base / "one", 10, 5, 12)
            generate(base / "two", 10, 5, 12)
            self.assertEqual((base / "one/pmp_cdc.jsonl").read_bytes(), (base / "two/pmp_cdc.jsonl").read_bytes())


if __name__ == "__main__":
    unittest.main()
