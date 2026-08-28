# MigrateX — Change Log

**Tiger Analytics · Developed by MDP**

---

## [1.0.0] — 2026-08-28

### Branding & UI Overhaul

- **Renamed to MigrateX** — "Accelerate Your Data Journey to Snowflake"
- Compact gradient header banner with account/warehouse/role context
- Source Type & Connection filter inline in header (DB Migration pages only)
- AI toggle + model selector inline with source controls
- Business-friendly page names: Overview, Pipeline Setup, Schema Mapping, Execute, Observability, Sources, File Loader, Auto-Ingest
- Forced dark theme via `.streamlit/config.toml`
- Sidebar navigation with grouped sections + "📖 User Guide" button
- Footer: "Powered by Tiger Analytics · Developed by MDP"

### Observability (merged History + Monitoring)

- **New `views/observability.py`** — single pane of glass with 3 tabs:
  - **Run Logs** — time-windowed execution history with batch grouping, trend chart, AI failure explainer
  - **Health Dashboard** — failed runs, stale tables, row mismatches, error patterns, step failures
  - **Alerts & Rules** — configurable alert rules (TABLE_STALE, RUN_FAILED, ROW_DRIFT_PCT, CONSECUTIVE_FAILURES) with webhook actions (Slack/Teams/Custom)
- New tables: `DMT_ALERT_RULES`, `DMT_ALERT_LOG`
- Segmented time window controls with aligned refresh button
- Removed separate History and Monitoring pages

### File Ingestion Enhancements

- Split into 6 tabs: Active Jobs, Run Jobs, Add/Edit Job, Upload & Ingest, Stages & Integrations, Run History
- **Force Reload** toggle in Run Jobs tab (bypasses Snowflake load dedup)
- Auto-convert glob patterns to regex (`*.csv` → `.*\.csv`) at save + runtime
- Fixed `MATCH_BY_COLUMN_NAME` not supported for CSV without PARSE_HEADER
- Fixed column count mismatch (audit columns excluded from COPY INTO target list)
- Fixed `LAST_ERROR` column missing — fallback + migration added
- Fixed SQL parse error from backslash in ESCAPE character (uses `$$` quoting)
- PATTERN clause omitted when empty (prevents empty-string regex error)
- Job execution results persisted in session state (visible after rerun)
- Error messages include failure details in Run Jobs display

### AI Integration

- AI Recommend button always visible (disabled state when fields empty)
- Per-feature model routing (LLM_MODEL_CONFIG, LLM_MODEL_DDL, LLM_MODEL_HISTORY)
- AI controls hidden on File Ingestion pages (not relevant)

### Documentation

- **Rewritten `userguide.md`** — 720 lines, covers all 8 pages with step-by-step walkthroughs and ASCII mockups
- **Rewritten `ARCHITECTURE.md`** — updated project structure, data flow diagrams, module dependencies
- **Rewritten `INSTALL.md`** — all 4 source types, external tools, Docker, storage integration
- User Guide accessible in-app via "📖 User Guide" sidebar button

---

## [0.11.0] — 2026-08-25

### MSSQL & Oracle Support

- **New `extractors/mssql_full.py`** — BCP bulk export with pipe delimiter + gzip
- **New `extractors/mssql_incremental.py`** — BCP queryout with CDC watermark
- **New `extractors/mssql_common.py`** — Secure BCP invocation (password on stdin)
- **New `extractors/oracle_full.py`** — Parallel range scan (>500MB threshold) + streaming
- **New `extractors/oracle_incremental.py`** — Streaming fetchmany with FF6 timestamp precision
- **New `extractors/oracle_common.py`** — Tuned cursor, table size estimation, range builder
- **New `ddl_generators/mssql.py`** — MSSQL type mapping (NVARCHAR, MONEY, BIT, UNIQUEIDENTIFIER)
- **New `ddl_generators/oracle.py`** — Oracle type mapping (NUMBER precision rules, CLOB, XMLTYPE→VARIANT)
- MSSQL 3-part naming support in Pipeline Setup (database.schema.table)
- Oracle service name in connection profile

---

## [0.10.0] — 2026-08-18

### Cloud File Ingestion Module

- **New `core/file_ingester.py`** — Ingestion engine:
  - Dynamic FILE FORMAT creation per job (CSV, Parquet, JSON, Avro)
  - Schema inference via `INFER_SCHEMA()` for auto table creation
  - Pattern-based file matching from external stages
  - APPEND / OVERWRITE / MERGE load modes
  - Pre-load file counting for validation
  - Fully-qualified file format names (avoid visibility issues)
  - SQL injection prevention via `_sanitize_sql_string()`

- **New `views/file_ingest.py`** — File Ingestion page:
  - Production UI with tabbed layout
  - Stage file browser with pagination
  - External stage creation (Storage Integration + Direct Credentials)
  - Upload & Ingest (Streamlit file uploader → internal stage → COPY INTO)
  - Date-partitioned path support

- **New `views/snowpipe_wizard.py`** — Auto-Ingest page:
  - CREATE PIPE DDL generation
  - Event notification guidance
  - COPY_HISTORY monitoring

- **New `metadata/file_ingest_config.py`** — CRUD for FILE_INGESTION_CONFIG
- **New `setup_file_ingestion.sql`** — Table definitions

---

## [0.9.0] — 2026-08-12

### AI Migration from Cortex to AI Gateway

- Migrated all AI features from Snowflake Cortex to Tiger Analytics Org AI Gateway
- OpenAI-compatible SDK integration (`openai` package)
- Per-feature model routing via DMT_SETTINGS
- Multiple model support with global LLM_MODELS_AVAILABLE setting
- AI-powered: Generate Config, Recommend Settings, Validate DDL, Explain Failures

---

## [0.8.0] — 2026-08-08

### Teradata Extraction Fixes

- **TPT reserved words** — all column names double-quoted in DEFINE SCHEMA
- **TPT type mismatch** — CAST all columns to VARCHAR(64000) in SELECT
- **CSV quoting** — split FIELD_OPTIONALLY_ENCLOSED_BY per source type (Teradata vs MSSQL)
- **Zero tables** — `TableKind IN ('T', 'O')` includes NoPI tables
- Source DDL formatting — `_format_source_ddl()` for readable display

### Oracle Incremental Fix

- Changed `FF3` to `FF6` in TO_TIMESTAMP format (6-digit microsecond precision)
- Fixed `_format_watermark` to store full precision

---

## [0.7.0] — 2026-08-01

### Schema Drift Detection

- **New `core/schema_drift.py`** — Additive column detection on every run
- New columns in source → automatic `ALTER TABLE ADD COLUMN`
- Removed columns → warning only (data preserved)

### Reconciliation

- **New `core/reconciler.py`** — Soft-delete via anti-join
- Configurable per table (RECONCILE = TRUE)

---

## [0.6.0] — 2026-07-25

### Storage Backends

- **New `storage/` package** — Abstract base + factory pattern
- `local.py` — Local filesystem (dev/testing)
- `s3.py` — AWS S3 via boto3 with archive-to-processed
- `azure_blob.py` — Azure Blob Storage
- `internal_stage.py` — Snowflake PUT/GET with parallel upload

### File Manifest

- **New `core/file_manifest.py`** — Track files across extract/upload/load lifecycle
- Enables decoupled extract and load phases

---

## [0.5.0] — 2026-07-18

### Resumable Pipelines

- **New `metadata/step_tracker.py`** — Step-level state machine
- Resume-from-failure via `--resume` flag
- Each table tracked independently within a batch

### Validator

- **New `core/validator.py`** — Source vs target row count + watermark parity

---

## [0.4.0] — 2026-07-10

### Streamlit UI

- **New `app.py`** — Multi-page Streamlit app with sidebar navigation
- Dashboard, Config, DDL, Run, History, Connections pages
- Tiger Analytics branding (orange accent, dark theme)
- Connection profile CRUD with live testing
- Real-time log streaming during pipeline execution

---

## [0.3.0] — 2026-07-01

### MySQL + Teradata Extractors

- `mysql_full.py` — mysqlsh parallel dump
- `mysql_incremental.py` — connectorx incremental
- `teradata_full.py` — TPT script generation
- `teradata_incremental.py` — teradatasql streaming

---

## [0.2.0] — 2026-06-20

### Core Pipeline Engine

- **New `core/orchestrator.py`** — CLI-based pipeline runner
- **New `core/loader.py`** — COPY INTO + MERGE execution
- **New `ddl_generators/`** — MySQL + Teradata type mapping

---

## [0.1.0] — 2026-06-10

### Initial Scaffold

- Project structure, requirements.txt, setup.sql
- HISTLOAD_DB.META schema with configuration tables
- Connection profiles, migration config, run log tables

---

*MigrateX v1.0 — Tiger Analytics · Developed by MDP*
