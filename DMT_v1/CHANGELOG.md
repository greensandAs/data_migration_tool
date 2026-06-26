# DMT v1 — Change Log

## Project Overview
Unified Data Migration Toolkit with modular, resumable pipelines. Multi-source (MySQL, Teradata) to Snowflake with decoupled extract/load, multi-cloud storage, and Snowflake-native configuration.

---

## [0.6.0] — 2026-06-25

### Teradata Support — Full Migration Pipeline
- New `ddl_generators/teradata.py` — Teradata→Snowflake type mapping from `DBC.ColumnsV`
  - Maps all TD types: BYTEINT, INTEGER, DECIMAL, TIMESTAMP, CLOB, BLOB, PERIOD, JSON, INTERVAL, NUMBER
  - `TD_SYSTEM_DATABASES` exclusion set (DBC, SYSLIB, SYSUDTLIB, etc.) per Snowflake migration guide
  - `list_tables()` and `list_databases()` for auto-discover
- New `extractors/teradata_full.py` — TPT-based full extraction (`tbuild`)
  - Dynamic TPT script generation with configurable delimiter, TRIM, instance count
  - 52MB file split (FileSizeMax), UTF-8 character set
  - Row count parsing from TPT stdout
- New `extractors/teradata_incremental.py` — Incremental via `teradatasql` + Arrow/Parquet
  - Supports multiple CDC columns (comma-separated), timestamp/ID watermark
  - FILTER_CONDITION support (same as MySQL)
  - Outputs Snappy-compressed Parquet files

### Orchestrator — Source-Type Dispatch
- `_source_connect()` routes to MySQL or Teradata based on profile `SOURCE_TYPE`
- `_teradata_connect()` — teradatasql with LOGMECH support (TD2/LDAP)
- DDL step dispatches to `ddl_generators.teradata` for Teradata sources
- Extract step dispatches to TPT (full) or teradatasql (incremental) for Teradata
- Schema drift detection works for both MySQL and Teradata sources
- All `mysql_conn` references replaced with generic `source_conn`

### Loader — CSV File Format
- New `CSV_FMT` (HISTLOAD_DB.META.CSV_FMT) — comma-delimited, quoted, null handling
- `copy_into_merge()` routes to CSV_FMT when `file_format="csv"` (TPT output)

### Connection Profiles
- `LOGMECH` column added to `CONNECTION_PROFILES` table (TD2/LDAP auth)
- Connection form shows Auth Method dropdown for Teradata profiles
- `create_profile()` accepts `logmech` parameter

### Config Auto-Discover — Teradata
- Schema dropdown queries `DBC.DatabasesV` (filtered by TD_SYSTEM_DATABASES)
- Table discovery queries `DBC.TablesV`, `DBC.ColumnsV`, `DBC.IndicesV`
- Detects primary index, watermark columns, recommends full/incremental

### Schema Changes
- `HISTLOAD_DB.META.MIGRATION_CONFIG`: added `DELIMITER`, `TRIM` columns
- `HISTLOAD_DB.META.CONNECTION_PROFILES`: added `LOGMECH` column
- `HISTLOAD_DB.META.CSV_FMT`: new file format for TPT CSV exports

---

## [0.5.0] — 2026-06-24

### SCD Type 0, 1, 2 Support
- New `SCD_TYPE` column in `MIGRATION_CONFIG` (0=append, 1=upsert, 2=history)
- **SCD Type 0 (Append):** Always INSERT, no key matching or dedup. Use for event/log tables.
- **SCD Type 1 (Upsert):** MERGE on primary key — UPDATE matched, INSERT new. (existing default behavior)
- **SCD Type 2 (History):** Close current record (`_VALID_TO=NOW`, `_IS_CURRENT=FALSE`) then INSERT new version with `_VALID_FROM=NOW`, `_IS_CURRENT=TRUE`. Full change history preserved.
- DDL auto-adds `_VALID_FROM`, `_VALID_TO`, `_IS_CURRENT` columns for SCD2 tables
- Existing tables upgraded via `ALTER TABLE ADD COLUMN IF NOT EXISTS` on DDL step

### Filter-Based Extraction
- New `FILTER_CONDITION` column in `MIGRATION_CONFIG` — static WHERE clause applied every run
- New `CUSTOM_SQL` column — full SELECT override (replaces auto-generated query entirely)
- Filter combines with watermark: `WHERE (filter) AND watermark > last_value`
- Priority: CUSTOM_SQL > FILTER_CONDITION + WATERMARK > WATERMARK only > full table

### External Stage (S3) — End-to-End Pipeline
- Full pipeline now works with S3 external storage: extract → upload (boto3) → load (COPY INTO from external stage) → merge → watermark
- Upload step correctly preserves multi-part file extensions (`.tsv.zst`) using `Path.suffixes` instead of `Path.suffix`
- Load step PATTERN fixed: `.*\.zst` matches both `.tsv.zst` and `.zst` files
- Defensive guard on COPY INTO result parsing (`tuple index out of range` prevention)
- Post-load: files moved to `processed/<sub>/<date>/` subfolder on S3

### Bucket Resolution (`_resolve_bucket`)
- New helper resolves S3/Azure bucket name without requiring `s3://` URLs in config
- Priority: `DMT_S3_BUCKET` env var → `DESCRIBE STAGE` fallback (extracts URL from stage definition)
- `STORAGE_PATH` in `MIGRATION_CONFIG` stays as the stage name (e.g. `DMT_EXT_S3`) — no change to config table convention
- Clear error messages when neither env var nor valid stage is available

### S3 Folder Structure
- Updated path structure: `dmt/<source_type>/<connection_profile>/<schema>/<table>/<full|incremental|processed>/`
- Connection profile name added between source_type and schema for multi-connection isolation
- Applied consistently across upload step, load step (ext_stage_path), and move-to-processed

### MySQL Type Mapping Overhaul
- `TIMESTAMP` → `TIMESTAMP_TZ` (was incorrectly `TIMESTAMP_NTZ`; MySQL TIMESTAMP stores UTC)
- `DATETIME` → `TIMESTAMP_NTZ` (unchanged — no timezone in MySQL DATETIME)
- `ENUM`, `SET` → `VARCHAR(512)` (was falling through to default VARCHAR(16M))
- `TEXT`, `TINYTEXT`, `MEDIUMTEXT`, `LONGTEXT` → explicit `VARCHAR(16777216)` (was accidental fallthrough)
- `BIT` → `NUMBER(38,0)` (was falling through to VARCHAR)
- `DECIMAL` with precision > 38 → `VARCHAR` (Snowflake max is 38 digits)

### BLOB Handling (`BLOB_MODE` config)
- New per-table `BLOB_MODE` column in `MIGRATION_CONFIG` (default: `binary`)
  - `binary`: maps BLOB → `BINARY` (true binary data, images, PDFs < 8 MB)
  - `text`: maps BLOB → `VARCHAR(16777216)` (BLOBs storing text/JSON/HTML — the "lazy schema" trap)
  - `skip`: maps BLOB → `VARCHAR` placeholder (oversized blobs to externalize to S3)
- `blob_mode` flows through DDL generation, schema drift, and COPY INTO column lists

### Zero-Date Handling
- `TSV_ZSTD_FMT`: added `NULL_IF = ('\\N', '0000-00-00', '0000-00-00 00:00:00')` — converts MySQL zero-dates to NULL during COPY INTO
- Incremental (connectorx → Parquet): added `?zeroDateTimeBehavior=convertToNull` to MySQL URI — driver converts to null before Arrow encoding

### Execution Mode Fix
- `EXECUTION_MODE = EXTRACT_ONLY` now includes `upload` step when `STORAGE_TYPE` is `s3` or `azure`
- Previously skipped upload, leaving extracted files stranded locally

### Logging Improvements
- Extract step no longer prints confusing `extracted: 0 rows` for mysqlsh (which doesn't report row counts)
- Now prints `extracted: 1 file(s)` when row count is unavailable; shows rows only when known (incremental)

### Fixes
- `TSV_ZSTD_FMT` / `PARQUET_FMT` referenced via `loader.TSV_ZSTD_FMT` (was undefined `NameError`)
- `.env.example` updated: `S3_BUCKET` → `DMT_S3_BUCKET` (matches `S3Storage` fallback logic)

---

## [0.3.0] — 2026-06-22

### Streamlit UI (Tiger Analytics branded)
- `app.py` — Main entry point with Tiger Analytics branding, custom CSS, dark theme
  - Fixed-width sidebar (240px), non-collapsible, sticky logo header
  - Navigation via styled buttons (Dashboard, Config, Run, History, Monitoring)
  - Source Connection dropdown in sidebar — scopes all pages to selected profile
  - Manage Connections button below dropdown
  - AI Assist toggle (Cortex COMPLETE) in sidebar
  - Connection status card (green/red dot, username, role, warehouse)
  - Subprocess-based job runner with background thread reader
  - Toast notifications (header kept transparent for visibility)
  - Sticky main content header

- `views/dashboard.py` — Pipeline health at a glance
  - Metric hero cards (Success, Failed, Pending, Active)
  - Per-table styled cards with colored status borders
  - Table/Cards view toggle with pagination (20 per page)
  - Filters: search, status, load type
  - Scoped by sidebar connection selection

- `views/connections.py` — Connection profile CRUD
  - Create form with password field (stored in Snowflake table)
  - Test Connection button (reads PASSWORD column directly)
  - Activate/Deactivate/Delete per profile
  - Proper type casting for Snowflake Decimal port values

- `views/config.py` — Migration configuration editor
  - Empty state when no profile selected ("Select a Source Connection")
  - Auto-discover MySQL schemas + tables (detects PK, watermark, composite keys)
  - Single-row layout: Schema dropdown | Generate Config | Add Table (dialog)
  - Selectable table import with checkboxes (data_editor)
  - Review notes from discovery stored in NOTES column
  - Table Configuration: collapsed rows with expand-on-edit (one at a time)
  - Compact one-line per table: active dot, name, load type, PK, status, Edit button
  - Expanded edit card: all fields + Review note with "Reviewed" button
  - Merge keys field with proper Snowflake ARRAY handling (ARRAY_CONSTRUCT)
  - Schema group headers with Activate All / Deactivate All
  - Pagination: 10 tables per page
  - `@st.dialog` for manual Add Table

- `views/run.py` — Pipeline execution
  - Run All Active / Run Single Table
  - Subprocess-based runner (process isolation, killable)
  - `st.fragment` live log refresh (or fallback polling)
  - Group-by-table log view
  - Stop button (terminates subprocess)
  - Resume from failure (reads LAST_FAILED_STEP)
  - Scoped by sidebar connection selection

- `views/history.py` — Audit log viewer
  - Summary metrics (total runs, success rate, avg duration)
  - Filters: table, status, date range, limit
  - Daily run trend bar chart
  - Batch grouping with expandable detail
  - AI failure explainer (Cortex COMPLETE)

- `views/monitoring.py` — Operational health
  - Failed runs, stale tables, validation mismatches
  - Error pattern analysis with bar chart
  - Step failure distribution
  - File manifest summary

- `shared.py` — Shared utilities (avoids circular imports)
  - `empty_state()` — reusable centered empty state component
  - `colorize_log()` — log line color coding
  - `start_job()` / `stop_job()` / `job_running()` — subprocess management
  - `live_running_panel` — fragment-based auto-refresh
  - `ai_enabled()` / `cortex_complete()` — Cortex AI helpers

### Backend Improvements
- `config_manager.py` — MERGE_KEYS handled via ARRAY_CONSTRUCT (not PARSE_JSON)
- `connection_manager.py` — PASSWORD column support (direct storage)
- `orchestrator.py`:
  - Pre-fetches all connection profiles before closing initial cursor
  - PASSWORD read from profile table (not just env vars)
  - Load step: lists stage files before COPY, skips if empty
  - Concise logging: extracted rows, merged rows, watermark changes, errors
  - Unicode fix (→ replaced with -> for Windows cp1252 console)
- `loader.py`:
  - `list_stage_files()` — LIST @STAGE for a table
  - `check_stage_before_load()` — pre-load check with audit logging if empty
- `.env.example` — Template with correct account format (GNNYEGN-IW99694)

### Database Schema Updates
- `CONNECTION_PROFILES.PASSWORD` column added (direct password storage)
- `RUN_LOG.CONNECTION_PROFILE` + `RUN_LOG.CONFIG_ID` columns
- `MIGRATION_CONFIG.NOTES` — stores review notes from auto-discovery

---

## [0.1.0] — 2026-06-22

### Added
- Project scaffolding (`DMT_v1/`)
- `setup.sql` — Snowflake bootstrap DDL:
  - `HISTLOAD_DB.META` control schema
  - `CONNECTION_PROFILES` table (multi-source connection registry)
  - `MIGRATION_CONFIG` table (replaces JSON config; per-table migration settings)
  - `PIPELINE_STEP_LOG` table (step-level execution state for retry-from-failure)
  - `FILE_MANIFEST` table (tracks extracted files across local/S3/Azure/internal stage)
  - `RUN_LOG` table (batch-level audit with CONNECTION_PROFILE)
  - `V_RUN_LOG` reporting view
  - Shared internal stage + file formats (Parquet, TSV+ZSTD)
- `config_manager.py` — CRUD operations against `MIGRATION_CONFIG` in Snowflake
- `connection_manager.py` — CRUD for `CONNECTION_PROFILES`
- `step_tracker.py` — step state machine with resume-from-failure logic
- `file_manifest.py` — file registration and lookup for decoupled extract/load
- `storage/__init__.py` — `StorageBackend` abstract base class + factory
- `storage/local.py` — local filesystem storage implementation
- `storage/s3.py` — AWS S3 storage implementation (boto3)
- `storage/azure_blob.py` — Azure Blob Storage implementation
- `storage/internal_stage.py` — Snowflake internal stage (PUT) implementation
- `extractors/__init__.py` — `BaseExtractor` ABC + `ExtractionResult` dataclass
- `extractors/mysql_full.py` — full-load extraction via MySQL Shell (mysqlsh)
- `extractors/mysql_incremental.py` — incremental extraction via connectorx
- `ddl_generators/__init__.py` + `mysql.py` — MySQL-to-Snowflake type mapping and DDL
- `loader.py` — shared Snowflake COPY INTO + MERGE logic
- `orchestrator.py` — unified step-based pipeline engine with retry/resume
- `schema_drift.py` — additive schema drift detection and resolution
- `reconciler.py` — soft-delete reconciliation via anti-join
- `validator.py` — source-vs-target parity checks (count, watermark, deep hash)
- `run_log.py` — audit row writer for `RUN_LOG`

### Architecture Decisions
- **Config in Snowflake** — no local JSON; all state in `MIGRATION_CONFIG` table
- **Step-based execution** — each step tracked independently; retry resumes from last failure
- **Decoupled extract/load** — extract writes files + registers in `FILE_MANIFEST`; can run separately
- **Storage abstraction** — pluggable backends (local, S3, Azure, Snowflake stage) per table
- **Multi-source ready** — `source_type` routes to correct extractor/DDL generator
- **MySQL first** — initial implementation; Teradata follows same interfaces

---

## Known Gaps — MySQL to Snowflake Migration

| # | Gap | Impact | Notes |
|---|-----|--------|-------|
| 1 | **No CDC (Change Data Capture) extractor** | Cannot capture deletes or real-time changes | Current approach: watermark-based incremental (inserts + updates only). Binlog/Debezium streaming not implemented. |
| 2 | **No scheduler** | Pipelines run manually only | No built-in cron/Task integration. Runs triggered from the Run UI page or CLI. No automatic recurring execution. |
| 3 | **No notification/alerting** | Failures go unnoticed until someone checks History page | No email, Slack, or webhook on pipeline failure. No notification integration configured. |
| 4 | **No data quality checks post-load** | Silent data corruption possible | Reconciler compares row counts only. No column-level checksum, no DMF integration, no sampling-based validation. |
| 5 | **No cleanup/archival of local files** | Disk fills up over time | `cleanup_old_manifests()` exists in `file_manifest.py` but is never called in the orchestrator flow. Extracted local files persist indefinitely after successful load. |
| 6 | **No SCD Type 2 support** | Cannot track historical dimension changes | Only current-state merge (Type 1 upsert). No silver-layer historization. |

---

## Planned

### [0.4.0] — Teradata Support
- `extractors/teradata_full.py` — TPT-based full extraction
- `extractors/teradata_incremental.py` — teradatasql incremental
- `ddl_generators/teradata.py` — Teradata type mapping

### [0.6.0] — Advanced Features
- Scheduling via Snowflake Tasks
- Alerting on failure (email/Slack via notification integration)
- Data quality checks (row-level sampling)
- SCD Type 2 support (optional silver layer)
- Validate page in UI (source vs target counts)
