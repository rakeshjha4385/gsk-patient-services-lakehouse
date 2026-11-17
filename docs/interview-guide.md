# 10-minute interview walkthrough

## Opening

“I built a small reference platform for a Salesforce Patient Management Services feed. It focuses on reusable controls: metadata-driven validation, idempotent CDC ingestion, consent-aware transformations, reconciliation, and safe operational assistance. All records are synthetic.”

## Walk the pipeline

1. Run `python -m lakehouse_demo.cli demo --output ./build/demo`.
2. Show the contract at `config/pmp_patient_services.json` and explain how onboarding a new object becomes configuration plus contract review.
3. Inspect `quarantine/rejected.csv`: bad enumerations are visible and measurable, not silently dropped.
4. Compare quality and reconciliation gates; explain that failed gates prevent the CLI run from succeeding.
5. Show the outbound feed contains pseudonymous keys and only consented latest states.
6. Show the agent report: read-only diagnosis, bounded recommendations, and explicit human review.

## Principal-level discussion

- At scale, use Delta transaction semantics and a durable event ledger; partition for workload access rather than over-partitioning by patient.
- Separate ingestion, contract, transformation, and policy metadata, with owners, versions, effective dates, and approval workflow.
- Protect PHI with managed identity, Key Vault, Unity Catalog grants, masking, row filters, private networking, audit logs, and retention controls.
- Coordinate Salesforce Bulk API windows with a token bucket, bounded concurrency, exponential backoff, retry classification, dead-letter replay, and reconciliation before checkpoint commit.
- For autonomous remediation, begin with shadow recommendations and offline evaluation. Promote only specific low-risk tools behind least privilege, typed inputs, approvals, immutable audit, and rollback.
- For GxP/HIPAA/GDPR contexts, apply validated SDLC, traceability, change control, consent/legal-basis policies, retention/deletion, lineage, and jurisdictional review; the demo makes no compliance claim.

## Honest scope statement

This repository is an executable local prototype. It does not claim to deploy Azure resources, connect to Salesforce, process actual PHI, or demonstrate a production-grade AI agent. Describe which choices you would validate with security, privacy, clinical operations, Salesforce, and platform stakeholders before production.
