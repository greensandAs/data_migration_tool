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
| `connection_manager.py` | CRUD for source database connection profiles + `test_connection()` for all source types. | `CONNECTION_PROFILES` |
| `source_specs.py` | Declarative per-source field requirements (default port, required extras, extractor readiness). Single source of truth shared by the UI form, connection builder, and test helper. | — |
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
| `mssql_full.py` | MSSQL | `bcp` (bulk copy) | Pipe-delimited CSV + gzip |
| `mssql_incremental.py` | MSSQL | `bcp queryout` (CDC filter) | Pipe-delimited CSV + gzip |
| `mssql_common.py` | MSSQL | — | Shared BCP arg builder + stdin-password runner |
| `oracle_full.py` | Oracle | `oracledb` (streaming + parallel ranges) | Snappy Parquet files |
| `oracle_incremental.py` | Oracle | `oracledb` (CDC filter) | Snappy Parquet files |
| `oracle_common.py` | Oracle | — | Shared helpers: tuned cursor, size estimation, PK detection, Arrow conversion |

**Interface contract** (defined in `extractors/__init__.py`):

```python
class BaseExtractor(ABC):
    def extract_full(config, src_cfg, output_dir) -> ExtractionResult
    def extract_incremental(config, src_cfg, output_dir, source_conn) -> ExtractionResult
    @property
    def source_type -> str  # "mysql" | "teradata" | "mssql" | "oracle"
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
| `mssql.py` | MSSQL | `INFORMATION_SCHEMA.COLUMNS` (via pyodbc) |
| `oracle.py` | Oracle | `ALL_TAB_COLUMNS` (via oracledb) |

All generators:
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

## File Formats — the load contract

Each extractor writes a different physical format. The loader must pick the
matching Snowflake FILE FORMAT **and** glob pattern, or nothing loads.

| Source | Full | Incremental |
|---|---|---|
| MySQL | `tsv_zstd` → `*.tsv.zst` | `parquet` → `*.parquet` |
| Teradata | `csv` → `*.csv` | `parquet` → `*.parquet` |
| MSSQL | `csv_gzip` → `*.csv.gz` | `csv_gzip` → `*.csv.gz` |

> **Why this is dangerous to get wrong:** a `COPY INTO` whose `PATTERN` matches
> no file is **not an error**. Snowflake reports success having loaded zero rows.
> A format/pattern mismatch is therefore silent data loss, not a visible failure.

Two registries keep this honest, and they must stay in sync:

| Registry | Location | Answers |
|---|---|---|
| `_OUTPUT` | `metadata/source_specs.py` | "what does this source *write*?" |
| `FILE_FORMATS` | `core/loader.py` | "how do I *COPY* that?" |

```python
FILE_FORMATS["csv_gzip"] = {
    "fmt":      BCP_PIPE_FMT,       # HISTLOAD_DB.META.BCP_PIPE_FMT
    "pattern":  r".*\.csv\.gz",
    "match_by": "",                 # positional — needs an explicit column list
}
```

`resolve_format()` **raises** on an unknown format rather than falling back to a
default. A silent fallthrough to TSV/zstd is exactly what previously caused
Teradata and MSSQL loads to move zero rows while reporting success.

Adding an extractor means adding an entry to **both** registries plus a
`CREATE FILE FORMAT` in `setup.sql`.

**How the format is chosen at load time** — `_load_file_format()`, in precedence order:

1. This run's `ExtractionResult` — most accurate
2. `FILE_MANIFEST.FILE_FORMAT` — for `LOAD_ONLY`, where extract never ran in this process
3. `source_specs.output_format()` — last resort

---

## Execution Modes

| Mode | Steps | Notes |
|---|---|---|
| `FULL` | ddl → schema_drift → extract → upload → load → merge → watermark | End to end |
| `EXTRACT_ONLY` | ddl → schema_drift → extract (→ upload) | Registers files in `FILE_MANIFEST` |
| `LOAD_ONLY` | load → merge → watermark | Consumes pending `FILE_MANIFEST` rows |

The step list is built without reference to source type, so all three sources
behave identically.

### LOAD_ONLY reads its state from FILE_MANIFEST

`LOAD_ONLY` skips `extract`, so there is no in-memory `ExtractionResult`. Both
the file format and the "is there anything to do?" decision come from
`FILE_MANIFEST` instead:

```
EXTRACT_ONLY run                    LOAD_ONLY run (later, maybe another host)
────────────────                    ─────────────────────────────────────────
extract ─┐                          get_pending_files(config_id)
         └─► register_files()  ──►    status IN ('extracted','uploaded')
             FILE_MANIFEST                   │
             (path, format, part)            ├─► _load_file_format()
                                             └─► merge → mark_loaded()
```

`mark_loaded()` retires the consumed rows, so re-running `LOAD_ONLY` will not
merge the same files twice.

---

## Cross-Source Parity

| Capability | MySQL | Teradata | MSSQL | Oracle |
|---|:---:|:---:|:---:|:---:|
| Connect & test | ✅ | ✅ | ✅ | ✅ |
| Config collection | ✅ | ✅ | ✅ | — |
| DDL capture | ✅ | ✅ | ✅ | ❌ |
| Auto target creation | ✅ | ✅ | ✅ | ❌ |
| SCD 0 / 1 / 2 | ✅ | ✅ | ✅ | ❌ |
| FULL | ✅ | ✅ | ✅ | ❌ |
| EXTRACT_ONLY | ✅ | ✅ | ✅ | ❌ |
| LOAD_ONLY | ✅ | ✅ | ✅ | ❌ |
| Incremental merge | ✅ | ✅ | ✅ | ❌ |
| Result capture | ✅ | ✅ | ✅ | — |

Oracle is deliberately **connect-only**: profiles can be created and tested, but
`source_specs.extractor_ready("oracle")` is `False`, so `_process_table()` raises
a clear `NotImplementedError` before doing any work rather than silently falling
through to the MySQL extractor.

`RUN_LOG.ENGINE` records the real engine per source — `mysqlsh`/`connectorx`,
`tpt`/`teradatasql`, `bcp`/`bcp` — via `source_specs.engine_name()`.

---

## Data Flow (End to End)

```
Source DB (MySQL / Teradata / MSSQL)
    │
    │  extractors/  ─── pull data via mysqlsh / TPT / connectorx / teradatasql / bcp
    │
    ▼
Local/S3/Azure/Stage  (Parquet, TSV, or gzipped CSV files)
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

1. Add a `SOURCE_SPECS` entry in `metadata/source_specs.py` — default port, required
   extra fields, and `extractor_ready`. Also add the source to `_OUTPUT` (what format
   its extractors write) and `_ENGINE` (labels for `RUN_LOG.ENGINE`)
2. Create `ddl_generators/<source>.py` — implement `map_<source>_type()` and `generate_and_apply()`
3. Create `extractors/<source>_full.py` and `extractors/<source>_incremental.py` — extend `BaseExtractor`
4. If it emits a new physical format, add a `FILE_FORMATS` entry in `core/loader.py`
   **and** a `CREATE FILE FORMAT` in `setup.sql` — a missing pattern loads zero
   rows silently
5. Add a connect function and a `test_connection()` branch in `metadata/connection_manager.py`
6. Add to `ALLOWED_SOURCES` in `setup.sql` and the `utils/shared.py` default
7. Wire into `core/orchestrator.py`: `_build_src_cfg()`, `_source_connect()`,
   `_refetch_columns()`, and the ddl/extract step dispatch

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
| `PARQUET_FMT` / `TSV_ZSTD_FMT` / `CSV_FMT` / `BCP_PIPE_FMT` | File format objects |
| `DMT_STAGE` | Internal named stage for file PUT |
| `DMT_EXT_S3` / `DMT_EXT_AZURE` | External stage definitions |

---

## Authentication & Connection Profiles

**POC scope: username + password only.** Advanced mechanisms (Kerberos, LDAP, JWT, Oracle wallet, Azure managed identity, Windows integrated auth) are deliberately out of scope.

### Required input fields by source type

| Field | MySQL | MSSQL | Teradata | Oracle |
|---|:---:|:---:|:---:|:---:|
| Profile Name | ✅ | ✅ | ✅ | ✅ |
| Host | ✅ | ✅ | ✅ | ✅ |
| Port | ✅ `3306` | ✅ `1433` | ➖ unused | ✅ `1521` |
| Username | ✅ | ✅ | ✅ | ✅ |
| Password | ✅ | ✅ | ✅ | ✅ |
| ODBC Driver | — | ✅ **required** | — | — |
| Service Name | — | — | — | ✅ **required** |

Source-specific fields are stored in the `EXTRA_PARAMS` VARIANT column, so adding a source needs no schema migration:

```json
{"driver": "ODBC Driver 18 for SQL Server"}     // mssql
{"service_name": "FREEPDB1"}                    // oracle
```

### Why those two extras are mandatory

- **ODBC Driver (MSSQL)** — cannot be defaulted safely. Driver 18 defaults `Encrypt=yes`; driver 17 defaults `Encrypt=no`, so a profile that works on one host silently fails on another. Must be explicit.
- **Service Name (Oracle)** — the DSN is `host:port/service_name`. There is no portable default (`ORCL` / `XEPDB1` / `FREEPDB1` all vary), so a connect string cannot be built without it.

### Field definitions live in one place

`metadata/source_specs.py` declares default port, whether the port is used, required extra fields, and whether an extractor exists. The connection form, `_build_src_cfg()`, and `test_connection()` all read from it, so they cannot drift apart.

```python
SOURCE_SPECS["oracle"] = {
    "default_port": 1521,
    "uses_port": True,
    "extractor_ready": False,        # connect/test works; no extractor yet
    "extra_fields": [{"key": "service_name", "required": True, ...}],
}
```

### Connection string shapes

| Source | Connector | Target form |
|---|---|---|
| MySQL | `mysql.connector` | `host` + `port` |
| MSSQL | `pyodbc` | `DRIVER={...};SERVER=host,port;DATABASE=db;UID=;PWD=` |
| Teradata | `teradatasql` | `host` only (port resolved internally), `logmech=TD2` |
| Oracle | `oracledb` (thin) | Easy Connect DSN: `host:port/service_name` |

### MSSQL requires the database at connect time

Unlike MySQL, **MSSQL's `INFORMATION_SCHEMA` is scoped per-database**, not server-wide. A connection without `DATABASE=` lands in the login's default database, so metadata lookups for any other database silently return zero columns.

`_source_connect()` therefore takes a `database` argument, passed from the table's `SOURCE_DB`:

```python
source_conn = _source_connect(source_type, src_cfg, database=config["SOURCE_DB"])
```

This is why "database" is a **per-table** value (`MIGRATION_CONFIG.SOURCE_DB`) and not a connection-profile field.

### Credential resolution order

1. `CONNECTION_PROFILES.PASSWORD` column (plaintext)
2. Environment variable named by `CONNECTION_PROFILES.AUTH_SECRET`
3. Source-specific env var fallback (`MYSQL_PASSWORD`, `MSSQL_PASSWORD`, `TD_PASSWORD`, `ORACLE_PASSWORD`)

> **Security note:** passwords are currently stored in plaintext in `CONNECTION_PROFILES`. Anyone with `SELECT` on that table can read every source credential. Acceptable for a POC; use `AUTH_SECRET` (env var indirection) or a secret manager for anything beyond that.

### Credential handling in external extract tools

Each extract tool takes credentials differently, and the naive form of each leaks. Current handling:

| Tool | Naive form | Why it leaks | What DMT does |
|---|---|---|---|
| `mysqlsh` | `--password=x` | argv → `ps aux` | `--passwords-from-stdin` |
| `bcp` | `-P x` | argv → `ps aux` | flag omitted; bcp prompts, password written to **stdin** |
| `tbuild` (TPT) | password inside the job script | script file left on disk | script written **0600**, deleted in a `finally:` block |

Two distinct hazards, two distinct fixes:

1. **argv exposure** — `ps` reads a process's argument vector straight from the kernel, so any credential in `argv` is readable by every user on the host. `shell=False` does **not** help. The only fix is to keep it out of `argv`.
2. **shell interpolation** — building a command string and running it under `shell=True` lets a password containing `$`, `` ` ``, `"`, `;` or `|` be reinterpreted. All extract subprocesses use argument lists with `shell=False`, so no value is ever shell-parsed.

`extractors/mssql_common.py` centralises this for MSSQL:

```python
args = build_bcp_args(...)          # deliberately contains no -P
proc = run_bcp(args, password)      # password delivered on stdin
```

`server_spec()` also appends `,port` only when it is safe — a host carrying a named instance (`HOST\SQLEXPRESS`) or an explicit port is passed through untouched.

### Sources that connect but cannot migrate

`source_specs.extractor_ready` gates this. Oracle profiles can be created and tested, but `_process_table()` raises a clear `NotImplementedError` before doing any work:

| Source | Connect & Test | DDL Gen | Extract | Migrate |
|---|:---:|:---:|:---:|:---:|
| MySQL | ✅ | ✅ | ✅ | ✅ |
| Teradata | ✅ | ✅ | ✅ | ✅ |
| MSSQL | ✅ | ✅ | ✅ | ✅ |
| Oracle | ✅ | ❌ | ❌ | ❌ |

---

## MSSQL Integration Details

### Source Schema Handling

MSSQL uses a 3-part naming convention: `database.schema.table`. Unlike MySQL (which has no schema layer) and Teradata (where schema maps to database), MSSQL requires an explicit schema name.

The `MIGRATION_CONFIG` table includes a `SOURCE_SCHEMA` column:
- **MSSQL**: `"dbo"`, `"Sales"`, `"HumanResources"` (required, defaults to `"dbo"`)
- **MySQL**: `NULL` (MySQL's `SOURCE_DB` IS the schema)
- **Teradata**: `NULL` (uses `TARGET_SCHEMA` for Snowflake-side override)

The unique constraint on `MIGRATION_CONFIG` is: `(CONNECTION_PROFILE, SOURCE_DB, SOURCE_SCHEMA, SOURCE_TABLE)` — this prevents collisions when the same table name exists in multiple schemas (e.g., `dbo.Users` vs `Sales.Users`).

### BCP Extraction Engine

MSSQL extraction uses Microsoft's **BCP (Bulk Copy Program)** — a high-performance command-line tool for bulk data export/import.

#### Full Load Flow

```
bcp "DBName.schema.Table" out "file.csv" -S server -U user -P pass -c -t "|" -C 65001
    │
    ▼  (if file > 512MB)
split_and_gzip()  →  file_part1.csv.gz, file_part2.csv.gz, ...
    │
    ▼
storage backend upload → Snowflake COPY INTO (using BCP_PIPE_FMT)
```

#### Incremental Load Flow

```
build_cdc_condition()  →  WHERE clause
    │
    ▼
bcp "SELECT * FROM [schema].[Table] WHERE ..." queryout "file.csv" ...
    │
    ▼
gzip → storage upload → COPY INTO + MERGE
```

#### BCP Command Variants

| Scenario | BCP Mode | Command Pattern |
|----------|----------|-----------------|
| Full table, no filter | `out` | `bcp "DB.schema.Table" out file.csv ...` |
| Filtered or incremental | `queryout` | `bcp "SELECT ... WHERE ..." queryout file.csv ...` |
| Custom SQL override | `queryout` | `bcp "user_sql" queryout file.csv ...` |

#### File Format

BCP exports use pipe-delimited (`|`) CSV with UTF-8 encoding. The corresponding Snowflake file format is `BCP_PIPE_FMT`:

```sql
FILE_FORMAT = (
    TYPE = CSV,
    FIELD_DELIMITER = '|',
    COMPRESSION = GZIP,
    ENCODING = 'UTF8',
    NULL_IF = (''),
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
)
```

#### Large File Handling

Files larger than 512 MB are split at row boundaries (no mid-row splits) and individually gzip-compressed. This ensures:
- Snowflake can parallelize ingestion across multiple files
- No single file exceeds optimal COPY INTO chunk size
- Memory-bounded processing (streaming split, not in-memory)

### CDC Modes (Change Data Capture)

MSSQL incremental extraction supports two watermark strategies:

#### Timestamp Mode (`WATERMARK_TYPE = 'time'`)

Captures inserts AND updates using a datetime column:

```sql
-- Config: WATERMARK_COL = "UpdatedAt", LAST_LOADED_AT = "2024-01-15 10:30:00"
-- Generated condition:
WHERE ([UpdatedAt] >= TRY_CAST('2024-01-15 10:30:00' AS DATETIME2))
```

Multi-column CDC (comma-separated `WATERMARK_COL`):

```sql
-- Config: WATERMARK_COL = "CreatedAt,ModifiedAt"
-- Generated condition:
WHERE (([CreatedAt] >= TRY_CAST('...' AS DATETIME2))
   OR  ([ModifiedAt] >= TRY_CAST('...' AS DATETIME2)))
```

#### ID Mode (`WATERMARK_TYPE = 'id'`)

Captures inserts only using an auto-increment column:

```sql
-- Config: WATERMARK_COL = "OrderID", LAST_LOADED_KEY = "50000"
-- Generated condition:
WHERE [OrderID] > 50000
```

### MSSQL Type Mapping (Key Conversions)

| MSSQL Type | Snowflake Type | Notes |
|-----------|---------------|-------|
| `int` | `NUMBER(10,0)` | |
| `bigint` | `NUMBER(19,0)` | |
| `bit` | `BOOLEAN` | |
| `money` | `NUMBER(19,4)` | Preserves cents |
| `datetime2` | `TIMESTAMP_NTZ` | |
| `datetimeoffset` | `TIMESTAMP_TZ` | Preserves timezone |
| `varchar(n)` / `nvarchar(n)` | `VARCHAR(n)` | Max 16MB |
| `decimal(p,s)` | `NUMBER(p,s)` | |
| `uniqueidentifier` | `VARCHAR(36)` | GUIDs as text |
| `xml` | `VARIANT` | Semi-structured |
| `geography` / `geometry` | `VARCHAR(16777216)` | WKT text |
| `rowversion` / `timestamp` | `BINARY(8)` | Internal versioning |

### Connection Profile Example

```
PROFILE_NAME:  azure_sql_prod
SOURCE_TYPE:   mssql
HOST:          myserver.database.windows.net
PORT:          1433
USERNAME:      etl_user
PASSWORD:      (via AUTH_SECRET env var)
EXTRA_PARAMS:  {"driver": "ODBC Driver 17 for SQL Server"}
```

### Environment Variables

```bash
# Required for BCP extraction (used by extractor if not in CONNECTION_PROFILES)
MSSQL_SERVER=myserver.database.windows.net
MSSQL_USER=etl_user
MSSQL_PASSWORD=secret
MSSQL_PORT=1433
MSSQL_DRIVER="ODBC Driver 17 for SQL Server"
```

### Prerequisites

- **ODBC Driver**: Microsoft ODBC Driver 17+ for SQL Server must be installed on the host
- **BCP utility**: Included with SQL Server command-line tools (`mssql-tools` package on Linux)
- **Python**: `pyodbc>=4.0.35` (in `requirements.txt`)

