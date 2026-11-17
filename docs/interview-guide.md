# Principal data engineer walkthrough

## Five-minute platform explanation

“This repository demonstrates a reusable ingestion platform rather than a one-off pipeline. Dataset metadata declares source and mode, keys, watermarks, validation rules, storage locations, privacy fields, and quality thresholds. A connector registry isolates source protocols; the generic engine owns control state, Bronze capture, validation, Silver merge, checkpoint safety, operational artifacts, and downstream handoff.”

## Walk the implementation

1. Review `config/metadata/pmp_patient_services.json`: Salesforce-style PMP CDC, `SystemModstamp`, patient business key, event ID, consent, domains, and reject threshold.
2. Review `src/patient_lakehouse/model.py` and `metadata.py`: typed, validated metadata contract and active registry.
3. Review `connectors/`: connector contract and adapters for REST API, JDBC, files, CDC, and stream micro-batches.
4. Run `patient-lakehouse demo --output ./build/demo`; inspect Bronze metadata, quarantine reasons, Silver latest state, consent-filtered Gold output, SQLite control state, and per-run audit/quality/reconciliation artifacts.
5. Show `ControlStore.finish_run`: only successful runs advance watermark and event ledger; failed downstream work can safely replay.
6. Compare `LocalJsonSink` with `DeltaLakeSink`: local mode keeps CI dependency-free, while the optional Spark sink uses Delta append and MERGE.

## Senior/principal engineering topics

- **Salesforce PMP:** discuss Bulk API v2 for high volumes, SOQL projection/filtering, SystemModstamp overlap, token bucket/API budget, retry classification, dead-letter handling, schema drift, OAuth via managed identity/Key Vault, and reconciliation before checkpointing.
- **CDC correctness:** define event identity, ordering, tombstones, late arrival policy, exactly-once boundary, replay retention, and idempotent destination semantics before selecting a watermark.
- **Metadata governance:** version contracts, owner approvals, environment overrides, compatibility policy, policy-as-code, rollout cohorts, and deprecation workflow.
- **Spark performance:** tune file sizing/compaction, partitioning to actual predicates, adaptive query execution, Photon, shuffle/skew, Delta layout, cluster policy, and workload isolation using observed query plans and metrics.
- **Privacy and quality:** classify fields, enforce consent purpose, pseudonymize with governed key lifecycle, record lineage and access audit, hold bad data without losing replayability, and agree on thresholds with data owners.
- **AI operations:** run an operations agent read-only in shadow mode first; use allowlisted query tools, traced/evaluated prompts, least-privilege identity, human approval for remediation, and a measurable false-positive/rollback policy.
- **Productionization:** validate GxP applicability, test evidence, change control, CI promotion, release rollback, DR, runbooks, SLOs, platform cost, and product ownership.

## Scope statement

The active path is executable against synthetic records. API and JDBC connectors are implemented against generic HTTP/DB-API interfaces; the CDC and streaming local adapters use fixtures. Real Salesforce credentials, Azure provisioning, Event Hubs consumers, Key Vault integration, and organizational compliance validation are deployment-specific. The repository is a platform blueprint, not a claim of production certification.
