# Architecture and engineering decisions

## Metadata-driven execution

The execution contract is implemented by `MetadataDrivenIngestion` in `src/patient_lakehouse/engine.py`:

1. **Read active metadata.** `MetadataRegistry` loads the dataset contract, required fields, source connector, keys, watermark, storage targets, and quality policies.
2. **Select strategy.** `ConnectorRegistry` resolves REST API, JDBC, file, CDC, or streaming source; `ingestion_mode` configures full, incremental, CDC, or streaming behavior.
3. **Read control state.** `ControlStore` returns the last successfully committed watermark and previously accepted CDC event IDs.
4. **Extract.** The connector applies watermark predicates, pagination/batch limits, credentials by environment reference, retries, and source-specific normalization.
5. **Append Bronze.** Raw source records are retained with run ID, source identity/type, ingest timestamp, and mode.
6. **Validate.** Required fields, null keys, event duplicates, configured primitive types, allowed values, and optional timestamp rules produce accepted and quarantined sets.
7. **Merge Silver.** Configured PII fields are HMAC-tokenized; CDC INSERT/UPDATE/DELETE records merge by business key. The local sink performs an idempotent JSONL state merge; `DeltaLakeSink` executes Delta `MERGE` in Spark.
8. **Update operational state.** Runs record status, counts, duration, error and reconciliation. Watermark and event ledger commit transactionally only on success. If rows are quarantined, the watermark is held to allow corrected records to be replayed.
9. **Trigger downstream.** A consent-filtered Gold product is published locally; the callback boundary maps to Databricks Workflows, ADF, or an authorized outbound integration in production.

## Connector contract

Each connector implements `extract(metadata, watermark) -> IngestionBatch`. It returns records plus source counts, extraction timestamp, next watermark, and optional source offset. Source-specific code stays outside the generic orchestration engine.

| Source | Local reference behavior | Production adapter direction |
|---|---|---|
| REST API | JSON pagination, bearer token env var, timeout, retry/backoff, watermark query param | General OAuth-protected API adapter |
| Salesforce Bulk API v2 | Async query job, bounded polling, locator pagination, field coercion, bearer token env var | Key Vault-backed OAuth, API budget telemetry, source reconciliation |
| JDBC | DB-API driver module, secret DSN env var, parameterized watermark SQL | Spark JDBC with partitioning, fetch size, read consistency, and secret scope |
| File | CSV, JSON, JSONL | ADLS/ABFS landing, manifest/checksum, schema registry, event-time partitioning |
| CDC | CDC envelope normalization and operation mapping | Salesforce Platform Events, database CDC, durable event IDs and offset checkpoints |
| Streaming | Bounded fixture micro-batches | Event Hubs/Kafka Structured Streaming, checkpoint location and event-time handling |

## State and delivery semantics

- Bronze is append-only, so failed validation can be diagnosed and replayed.
- The event ledger suppresses already committed CDC IDs. It is committed with the run status and watermark in one SQLite transaction for the local implementation; use a Delta control table or transactional metadata store in production.
- A failed merge or downstream trigger leaves the watermark unchanged. Silver merge keys and event IDs make re-execution safe.
- If a run contains rejected records below the configured threshold, its watermark is held, permitting remediation and replay. This trades extra source reads for safer recovery.
- Reconciliation balances extracted = accepted + rejected and tracks resulting Silver state separately because CDC deletes and upserts make row counts differ.
- Current local event-ledger lookup uses a relational control table; production should batch/look up keys efficiently and govern retention based on source replay guarantees.

## Security and governance

- Credentials are referenced by environment variable name, never embedded in metadata. Production uses Key Vault plus managed identity.
- PII tokenization requires a key. The local synthetic profile passes a fixed local-only key; production key lifecycle and stable tokenization domain must be designed with privacy/security owners.
- Consent is applied before the Gold product. Outbound Salesforce synchronization should add per-record consent and purpose checks at the final connector boundary.
- Apply least privilege through Unity Catalog and Entra groups; mask direct identifiers and constrain row access where appropriate.
- Persist lineage, config version/hash, schema version, user/service identity, and approval references in production audit records.

## Azure platform mapping

| Capability | Azure pattern |
|---|---|
| Orchestration | ADF for source coordination; Databricks Workflows for lakehouse dependencies |
| Landing | ADLS Gen2 bronze zone, source manifests and immutable retention |
| Transformation | Databricks PySpark with Delta Lake and optimized incremental MERGE |
| Streaming | Event Hubs + Structured Streaming checkpoints |
| Governance | Unity Catalog, Entra groups, tags, lineage, row filters, column masks |
| Secrets and identity | Key Vault, managed identities, private endpoints |
| Delivery | GitHub Actions or Azure DevOps, Terraform/Bicep, Databricks Asset Bundles |
| Observability | Structured run events, Databricks/ADF metrics, Log Analytics and actionable alerts |

The Terraform sample creates a resource group, private container, and storage account only. Network isolation, private endpoints, identities, diagnostic settings, policies, encryption, state backend, and workspace integration need environment-specific design.
