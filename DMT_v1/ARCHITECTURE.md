# DMT v1 — Architecture Guide

> Data Migration Toolkit for Snowflake | Tiger Analytics

This document describes the internal package layout, responsibilities, and data flow for developer onboarding.

---

## Project Structure

```
DMT_v1/
├── app.py                      Streamlit entrypoint (nav, session, CSS)
├── setup.sql                   Snowflake bootstrap DDL (HISTLOAD_DB.META.*)
├── requirements.txt            Python dependencies
├── .env.example                Environment variable template
│
├── core/                       Pipeline engine (no Streamlit dependency)
├── metadata/                   Snowflake state management (config, tracking)
├── utils/                      Cross-cutting utilities
├── extractors/                 Source-specific data extraction
├── ddl_generators/             Source-to-Snowflake type mapping & DDL
├── storage/                    Pluggable file storage backends
├── views/                      Streamlit UI pages
└── assets/                     Brand logos and static files
```

---

## Package Responsibilities

### `core/` — Pipeline Engine

The heart of DMT. Contains the ETL orchestration logic with **zero Streamlit dependency** — runs as a standalone CLI subprocess.

| Module | Responsibility |
|--------|---------------|
| `orchestrator.py` | Step-based pipeline engine. Executes DDL → schema drift → extract → upload → load → merge → watermark per table. Supports `--full`, `--resume`, `--extract-only`, `--load-only`, `--reconcile`, `--validate` modes. Runs tables in parallel via `ThreadPoolExecutor`. |
| `loader.py` | Snowflake ingestion: `COPY INTO` for full loads, `COPY INTO` staging + `MERGE INTO` for incremental. Handles internal/external stage paths, parallel PUT, watermark retrieval. |
| `validator.py` | Source-vs-target parity checks at three levels: row count, watermark max, and deep hash (order-independent XOR of row MD5 slices). |
| `reconciler.py` | Soft-delete reconciliation. Extracts only key columns from source, loads into a transient table, then sets `_IS_DELETED = TRUE` on RAW rows missing from source via anti-join. |
| `schema_drift.py` | Additive schema drift detection. Compares source columns against existing Snowflake table and issues `ALTER TABLE ADD COLUMN` for new columns. Dropped columns are warned but preserved. |
| `file_manifest.py` | Tracks extracted files in `HISTLOAD_DB.META.FILE_MANIFEST`. Enables decoupled extract/load workflows — extract and load can run at different times or from different machines. |

**Key design decision:** The orchestrator is invoked as a subprocess (`python core/orchestrator.py`) by the UI's job runner. This isolates pipeline execution from the Streamlit process, prevents memory leaks, and enables clean stop/kill semantics.

---

### `metadata/` — Snowflake State Management

All persistent state lives in Snowflake tables under `HISTLOAD_DB.META`. These modules provide the Python CRUD layer.

| Module | Responsibility | Backing Table |
|--------|---------------|---------------|
| `config_manager.py` | CRUD for table migration configs (source/target mapping, load type, watermark settings, merge keys, partitioning). | `MIGRATION_CONFIG` |
| `connection_manager.py` | CRUD for source database connection profiles (host, port, credentials, source type). Supports encrypted password storage via `AUTH_SECRET` env vars. | `CONNECTION_PROFILES` |
| `run_log.py` | Records each pipeline run: start/end times, status, row counts, duration, error messages. Powers the History and Monitoring views. | `RUN_LOG` |
| `step_tracker.py` | Granular step-level progress within a run (ddl, schema_drift, extract, upload, load, merge, watermark, validate). Enables resume-from-failure. | `PIPELINE_STEP_LOG` |

**Key design decision:** Storing config in Snowflake (not JSON files) provides Time Travel audit, multi-user access, and survival across container restarts.

---

### `utils/` — Cross-Cutting Utilities

Shared functions consumed by both the UI layer and the pipeline engine.

| Module | Responsibility |
|--------|---------------|
| `shared.py` | Background job runner (subprocess + daemon thread reader), live log colorizer, stop/kill controls, Cortex AI helper (`cortex_complete`), `empty_state` UI widget, app settings reader. |
| `ui_theme.py` | Centralized brand tokens (colors, spacing), CSS injection for dark theme, reusable HTML helpers (pills, cards, metric boxes). |

**Key design decision:** `shared.py` uses `subprocess.Popen` (not `multiprocessing`) so the pipeline runs in a fully isolated process with its own memory space. A daemon thread streams stdout line-by-line for real-time log display.

---

### `extractors/` — Source Data Extraction

Implements the `BaseExtractor` interface for each supported source system.

| Module | Source | Engine | Output |
|--------|--------|--------|--------|
| `mysql_full.py` | MySQL | `mysqlsh` (`util.dumpTables`) | TSV + zstd compressed files |
| `mysql_incremental.py` | MySQL | `connectorx` (Arrow) | Snappy Parquet files |
| `teradata_full.py` | Teradata | `TPT` (`tbuild`) | Delimited CSV files |
| `teradata_incremental.py` | Teradata | `teradatasql` (Arrow) | Snappy Parquet files |

**Interface contract** (defined in `extractors/__init__.py`):

```python
class BaseExtractor(ABC):
    def extract_full(config, src_cfg, output_dir) -> ExtractionResult
    def extract_incremental(config, src_cfg, output_dir, source_conn) -> ExtractionResult
    @property
    def source_type -> str  # "mysql" | "teradata"
```

`ExtractionResult` carries: `files`, `row_count`, `watermark_to`, `file_format`, `engine`, `skipped`, `skip_reason`.

**Incremental cursor modes:**
- `time` — `WHERE watermark_col > last_loaded_at` (captures inserts + updates)
- `id` — `WHERE pk_col > last_loaded_key` (captures inserts only)

---

### `ddl_generators/` — Type Mapping & DDL

Reads source metadata and generates Snowflake `CREATE TABLE` statements.

| Module | Source | Metadata Source |
|--------|--------|-----------------|
| `mysql.py` | MySQL | `information_schema.columns` |
| `teradata.py` | Teradata | `DBC.ColumnsV` |

Both generators:
- Map source types to Snowflake equivalents (100+ type mappings)
- Append audit columns: `_LOAD_TS`, `_SRC_FILE`, `_BATCH_ID`, `_IS_DELETED`, `_DELETED_AT`
- Support SCD Type 2 (adds `_VALID_FROM`, `_VALID_TO`, `_IS_CURRENT`)
- Auto-create target database and RAW schema if missing

---

### `storage/` — Pluggable File Backends

Abstracts where extracted files physically reside between extraction and Snowflake ingestion.

| Backend | Module | Use Case |
|---------|--------|----------|
| Local filesystem | `local.py` | Dev/testing, single-node |
| AWS S3 | `s3.py` | Production cloud storage |
| Azure Blob | `azure_blob.py` | Azure deployments |
| Snowflake internal stage | `internal_stage.py` | Zero-egress, Snowflake-native |

All backends implement the `StorageBackend` ABC: `upload`, `download`, `list_files`, `delete`, `exists`, `get_stage_uri`.

---

### `views/` — Streamlit UI Pages

Each module exports a `render(conn)` function called by `app.py`.

| Page | Purpose |
|------|---------|
| `dashboard.py` | Pipeline health: metric cards, per-table status, filtering, search |
| `connections.py` | Create/edit/test/delete source connection profiles |
| `config.py` | Table migration config CRUD with AI-assisted recommendations |
| `ddl.py` | Side-by-side source vs Snowflake DDL with AI validation |
| `run.py` | Launch pipelines, live log streaming, stop controls |
| `history.py` | Run history with filtering, pagination, AI failure analysis |
| `monitoring.py` | Validation mismatches, drift alerts, reconciliation stats |

---

## Data Flow (End to End)

```
Source DB (MySQL/Teradata)
    │
    │  extractors/  ─── pull data via mysqlsh / TPT / connectorx / teradatasql
    │
    ▼
Local/S3/Azure/Stage  (Parquet or TSV files)
    │
    │  storage/  ─── upload to landing zone
    │
    ▼
Snowflake Stage (@HISTLOAD_DB.META.DMT_STAGE or external)
    │
    │  core/loader.py  ─── COPY INTO + MERGE
    │
    ▼
Target: <DB>.RAW.<TABLE>  (with audit columns)
    │
    │  core/validator.py  ─── count / watermark / hash parity
    │
    ▼
HISTLOAD_DB.META.*  (run logs, step logs, file manifest)
```

---

## Adding a New Source

1. Create `ddl_generators/<source>.py` — implement `map_<source>_type()` and `generate_and_apply()`
2. Create `extractors/<source>_full.py` and `extractors/<source>_incremental.py` — extend `BaseExtractor`
3. Register the source in `metadata/connection_manager.py` (connection form fields)
4. Add to `utils/shared.py` → `get_allowed_sources()` default list
5. Wire into `core/orchestrator.py` pipeline step dispatch

---

## Snowflake Objects (HISTLOAD_DB.META)

Created by `setup.sql`:

| Object | Purpose |
|--------|---------|
| `MIGRATION_CONFIG` | Table-level migration settings |
| `CONNECTION_PROFILES` | Source connection credentials |
| `RUN_LOG` | Per-run execution history |
| `PIPELINE_STEP_LOG` | Per-step granular progress |
| `FILE_MANIFEST` | Extracted file registry |
| `DMT_SETTINGS` | App-level key/value settings |
| `PARQUET_FMT` / `TSV_ZSTD_FMT` / `CSV_FMT` | File format objects |
| `DMT_STAGE` | Internal named stage for file PUT |
| `DMT_EXT_S3` / `DMT_EXT_AZURE` | External stage definitions |
