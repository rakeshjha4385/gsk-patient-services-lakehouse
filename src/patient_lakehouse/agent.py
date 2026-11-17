"""Read-only incident triage over allowlisted run artifacts."""
from __future__ import annotations

import json
from pathlib import Path


def diagnose(run_dir: Path) -> dict:
    report = {}
    for name in ("run.json", "quality.json", "reconciliation.json"):
        path = run_dir / name
        if path.exists():
            report[name.removesuffix(".json")] = json.loads(path.read_text(encoding="utf-8"))
    issues, recommendations = [], []
    quality = report.get("quality", {})
    if quality.get("passed") is False:
        issues.append("QUALITY_GATE_FAILED")
        recommendations.append("Review quarantine reason counts and the source contract with its data owner.")
    if quality.get("rejected_count", 0) > 0:
        issues.append("ROWS_QUARANTINED")
        recommendations.append("Review quarantined rows; the ingestion watermark is held until they are corrected or dispositioned.")
    run = report.get("run", {})
    if run.get("status") == "FAILED":
        issues.append("INGESTION_FAILED")
        recommendations.append("Inspect the run error and audit trail; retry only after checking idempotency and source health.")
    if not issues:
        recommendations.append("No configured gate failure found; downstream delivery still requires its authorized workflow.")
    return {"agent": "read-only-operations-triage", "issues": issues,
            "recommendations": recommendations, "autonomous_mutations": False, "review_required": True}
