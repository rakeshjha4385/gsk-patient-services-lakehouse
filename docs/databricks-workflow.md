# Databricks JDBC ingestion workflow

The Asset Bundle in `databricks.yml` provisions a manual Databricks Workflow job named `jdbc_patient_services_ingestion`. Its Python wheel task:

1. Creates a deterministic synthetic patient-services SQLite database.
2. Reads it through Spark's JDBC data source using the framework's `SparkJdbcConnector`.
3. Runs shared metadata validation, quarantine, HMAC tokenization, and checkpoint controls.
4. Writes Bronze and Silver as Delta data paths and publishes a consent-filtered Gold aggregate.
5. Emits run, quality, reconciliation, and audit artifacts and advances the watermark only after the downstream handoff succeeds.

This workflow intentionally caps `max_driver_records` at 5,000 because the current shared engine uses bounded Python record batches between its connector and sink interfaces. The local synthetic run is suitable for exercising this framework path. A high-volume production deployment should keep records in Spark DataFrames through validation and Delta MERGE rather than collect rows to the driver.

## Prerequisites

- Databricks CLI 0.218.0 or newer, authenticated to the target Azure Databricks workspace.
- An existing Unity Catalog catalog, schema, and writable volume.
- A job identity that can use the catalog/schema/volume and write files below the volume.
- A single-node compatible Azure VM family available in the workspace region and permission to attach Maven library `org.xerial:sqlite-jdbc:3.46.1.0`.

Create a volume and grant the job identity access, for example:

```sql
CREATE VOLUME main.default.synthetic_ingestion;
GRANT USE CATALOG ON CATALOG main TO `your-job-principal`;
GRANT USE SCHEMA ON SCHEMA main.default TO `your-job-principal`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME main.default.synthetic_ingestion TO `your-job-principal`;
```

Use catalog/schema/volume names you own and substitute your job identity. The workflow stores its source fixture, SQLite control database, and path-based Delta data under `/Volumes/<catalog>/<schema>/<volume>/patient-lakehouse`. The Delta paths are file-based and are not registered as Unity Catalog tables.

## Validate, deploy, and run

Configure a valid Databricks CLI profile named `agent` (or change `targets.dev.workspace.profile`):

```bash
databricks auth login --host https://<your-azure-workspace-host> --profile agent
databricks bundle validate --target dev \
  --var="catalog=main" --var="schema=default" --var="volume=synthetic_ingestion"
databricks bundle deploy --target dev \
  --var="catalog=main" --var="schema=default" --var="volume=synthetic_ingestion"
databricks bundle run --target dev jdbc_patient_services_ingestion
```

Check the task output for the run ID and control counts. The Delta output is under the volume paths `lakehouse/bronze/jdbc_synthetic_patient_services`, `lakehouse/silver/jdbc_synthetic_patient_services`, and `lakehouse/gold/jdbc_synthetic_patient_services/program_status`.

## Local equivalent

The same metadata and engine can be exercised without a Databricks workspace:

```bash
python3 -m pip install -e .
patient-lakehouse sample-jdbc --output ./build/jdbc-workflow-local --patients 250 --seed 42
```

This local command uses Python's SQLite DB-API connector and the JSONL sink. The Databricks Workflow instead uses Spark's JDBC reader and the framework's Delta sink.
