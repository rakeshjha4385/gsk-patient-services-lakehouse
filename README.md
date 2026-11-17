# Patient Services Lakehouse Reference Platform

An interview-ready reference implementation for a governed, metadata-driven patient-services data platform. It demonstrates a Salesforce PMP → lakehouse → outbound Salesforce pattern with synthetic data only.

## What it demonstrates

- Config-driven ingestion of Salesforce-style CDC records with schema checks and an idempotent event ledger.
- Bronze, silver, and gold layers, quarantine handling, and incremental watermarks.
- Patient-level pseudonymization, consent enforcement, and a synthetic outbound change feed.
- Data quality controls, source-to-target reconciliation, audit records, and operational metrics.
- A deterministic, tool-limited operations agent that diagnoses pipeline runs and proposes remediation; it never mutates records or calls an LLM.
- CI, container packaging, and an Azure deployment sketch.

This is a local reference implementation, not a validated GxP system, a production connector, or a substitute for privacy/legal review. No real patient data, Salesforce credentials, cloud subscription, or external service is needed.

## Run it

Requires Python 3.11+ and uses only the standard library.

```bash
python -m lakehouse_demo.cli demo --output ./build/demo
```

To regenerate inputs only:

```bash
python -m lakehouse_demo.cli generate --output ./build/input --patients 250 --days 14 --seed 42
```

To run against an input directory:

```bash
python -m lakehouse_demo.cli run --input ./build/input --output ./build/run
python -m lakehouse_demo.cli diagnose --run-dir ./build/run
```

The demo writes JSONL synthetic CDC, CSV silver/gold/outbound tables, and run artifacts (`quality.json`, `reconciliation.json`, `audit.jsonl`, `metrics.json`, `agent_report.json`). See [`docs/architecture.md`](docs/architecture.md) for boundaries and production evolution.

## Project map

```text
lakehouse_demo/       generator, config-driven pipeline, controls, CLI, agent
config/               dataset contract and governance policy
infra/                illustrative Azure Terraform scaffold
tests/                deterministic pipeline and safety checks
docs/                 architecture and interview walkthrough
```

## Interview walkthrough

1. Generate a repeatable synthetic source batch.
2. Walk the registry-driven ingestion and event-id deduplication.
3. Inspect quarantine and quality metrics, then reconcile source and target counts.
4. Explain why identifiers are HMAC-tokenized and consent is checked before outbound delivery.
5. Show that the operational agent is read-only and the audit trail records decisions.
6. Discuss production substitutions: ADLS/Delta, Databricks Workflows, ADF, Key Vault, Unity Catalog, Salesforce Bulk API/Platform Events, managed identity, and LangGraph/MCP behind reviewed tool permissions.

## Safety and limitations

The HMAC secret is generated per run for demonstration; production must use a managed secret and stable key versioning/rotation. The CSV files model logical layers, not ACID Delta tables. The outbound file is a simulated delivery queue with idempotency keys, not an API client. The sample policy is illustrative and requires jurisdiction-specific privacy, consent, retention, access, lineage, and GxP controls before real use.
