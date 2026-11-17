"""Read-only operations assistant with allowlisted tools and deterministic policy."""
from __future__ import annotations

import json
from pathlib import Path


ALLOWED_ARTIFACTS = {"quality.json", "reconciliation.json", "metrics.json"}


def diagnose(run_dir: Path) -> dict:
    report = {}
    for name in ALLOWED_ARTIFACTS:
        path = run_dir / name
        if path.exists():
            report[name.removesuffix(".json")] = json.loads(path.read_text(encoding="utf-8"))
    quality = report.get("quality", {})
    reconciliation = report.get("reconciliation", {})
    issues, actions = [], []
    if not quality.get("passed", False):
        issues.append("QUALITY_GATE_FAILED")
        actions.append("Review quarantine reasons and contract changes; rerun only after data-owner approval.")
    if not reconciliation.get("passed", False):
        issues.append("RECONCILIATION_FAILED")
        actions.append("Hold outbound delivery and compare source, silver, and event-ledger counts.")
    if not issues:
        actions.append("No control breach detected; outbound feed is ready for an authorized integration job.")
    return {"agent": "read-only-pipeline-ops", "issues": issues, "recommendations": actions,
            "autonomous_mutations": False, "review_required": True}
