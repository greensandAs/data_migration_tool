# MigrateX — Architecture Guide

> Enterprise Data Migration Platform for Snowflake | Tiger Analytics · Developed by MDP

This document describes the internal package layout, responsibilities, and data flow for developer onboarding.

---

## Project Structure

```
DMT_v1/
├── app.py                      Streamlit entrypoint (nav, session, CSS, AI toggle)
├── setup.sql                   Snowflake bootstrap DDL (HISTLOAD_DB.META.*)
├── setup_file_ingestion.sql    File ingestion table DDL
├── requirements.txt            Python dependencies
├── userguide.md                User documentation (rendered in-app)
├── .streamlit/config.toml      Forced dark theme configuration
│
├── core/                       Pipeline engine (no Streamlit dependency)
│   ├── orchestrator.py         Main pipeline runner (CLI + resumable, 1062 lines)
│   ├── loader.py               Snowflake COPY INTO / MERGE execution
│   ├── file_ingester.py        Cloud file → Snowflake ingestion engine
│   ├── schema_drift.py         Additive column detection
│   ├── validator.py            Source-vs-target parity checks
│   ├── reconciler.py           Soft-delete reconciliation
│   └── file_manifest.py        File tracking across runs
│
├── views/                      Streamlit UI pages
│   ├── dashboard.py            Overview — health metrics + status cards
│   ├── config.py               Pipeline Setup — table CRUD + AI recommend
│   ├── connections.py          Sources — connection management
│   ├── run.py                  Execute — pipeline execution UI
│   ├── ddl.py                  Schema Mapping — DDL conversion viewer
│   ├── observability.py        Observability — logs + health + alerts
│   ├── file_ingest.py          File Loader — cloud file ingestion wizard
│   └── snowpipe_wizard.py      Auto-Ingest — Snowpipe DDL generator
│
├── metadata/                   Snowflake state management
│   ├── config_manager.py       MIGRATION_CONFIG CRUD
│   ├── connection_manager.py   CONNECTION_PROFILES CRUD
│   ├── file_ingest_config.py   FILE_INGESTION_CONFIG CRUD
│   ├── step_tracker.py         Step state machine + resume logic
│   ├── run_log.py              Audit log writer
│   └── source_specs.py         Declarative source-type specifications
│
├── extractors/                 Source data extraction engines
│   ├── __init__.py             Base interface (ExtractionResult, BaseExtractor)
│   ├── mysql_full.py           mysqlsh util.dumpTables
│   ├── mysql_incremental.py    connectorx → Arrow → Parquet
│   ├── teradata_full.py        TPT script generation
│   ├── teradata_incremental.py teradatasql → Arrow → Parquet
│   ├── mssql_full.py           BCP → pipe-delimited → gzip
│   ├── mssql_incremental.py    BCP queryout → CDC → gzip
│   ├── mssql_common.py         Shared BCP utilities (secure invocation)
│   ├── oracle_full.py          oracledb parallel range scan → Parquet
│   ├── oracle_incremental.py   oracledb streaming → Parquet
│   └── oracle_common.py        Shared Oracle utilities
│
├── ddl_generators/             Schema mapping & DDL generation
│   ├── __init__.py             MySQL DDL + shared constants (RAW_SCHEMA, AUDIT_COLS)
│   ├── mysql.py                Re-exports
│   ├── teradata.py             Teradata type mapping + list_tables/list_databases
│   ├── mssql.py                MSSQL type mapping
│   └── oracle.py               Oracle type mapping
│
├── storage/                    Pluggable file storage backends
│   ├── __init__.py             Abstract base + factory (get_backend)
│   ├── local.py                Local filesystem (dev/testing)
│   ├── s3.py                   AWS S3 via boto3
│   ├── azure_blob.py           Azure Blob Storage
│   └── internal_stage.py       Snowflake PUT/GET
│
├── utils/                      Cross-cutting utilities
│   ├── shared.py               AI Gateway, job runner, helpers
│   └── ui_theme.py             CSS injection + HTML components
│
├── assets/logos/               Brand assets (SVG/PNG)
├── docs/screenshots/           User guide screenshot placeholders
├── test_files/                 Sample data for file ingestion testing
└── export/                     Sample extractor output artifacts
```

---

## Data Flow — Database Migration

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│ Source DB    │     │  Extractor   │     │   Storage    │     │  Snowflake   │
│ MySQL/TD/    │ ──→ │ (mysqlsh/TPT/│ ──→ │ (Stage/S3/   │ ──→ │  COPY INTO   │
│ MSSQL/Oracle │     │  BCP/oracledb)│     │  Azure)      │     │  + MERGE     │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
       │                    │                    │                    │
       │                    ▼                    ▼                    ▼
       │            Local Parquet/CSV     Upload to cloud     Load into RAW
       │            files (temp)          or PUT to stage     schema tables
       │                                                            │
       └──────── DDL Generation ────────────────────────────────────┘
                (type mapping per source)
```

### Pipeline Steps (sequential, resumable)

```
DDL → Schema Drift → Extract → Upload → Load → Merge → Watermark → Validate
 │         │            │          │        │       │         │          │
 ▼         ▼            ▼          ▼        ▼       ▼         ▼          ▼
CREATE  ALTER ADD    Pull data   PUT/S3/  COPY    SCD0/1/2  Update    Count
TABLE   COLUMN      from source  Azure    INTO    logic     cursor    check
```

---

## Data Flow — File Ingestion

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│ Cloud Stage  │     │ File Ingester│     │  Snowflake   │
│ (S3/Azure/   │ ──→ │ (pattern     │ ──→ │  COPY INTO   │
│  Internal)   │     │  match, fmt) │     │  target table│
└──────────────┘     └──────────────┘     └──────────────┘
                            │
                            ▼
                     INFER_SCHEMA (if auto-create)
                     FORCE = TRUE (if retry)
                     APPEND / OVERWRITE / MERGE
```

---

## Key Design Principles

1. **Multi-source plugin architecture** — Each source has: DDL generator + full extractor + incremental extractor
2. **Resumable execution** — Step-level tracking (PIPELINE_STEP_LOG) enables retry-from-failure
3. **Decoupled phases** — Extract and Load can run independently (EXECUTION_MODE flag)
4. **Storage-agnostic** — Pluggable backends (local/S3/Azure/internal stage)
5. **Security** — Passwords never in argv (BCP stdin, mysqlsh stdin, TPT 0600+shredded)
6. **State in Snowflake** — All config/state in HISTLOAD_DB.META tables (auditable, Time Travel)
7. **AI-enhanced** — Tiger Analytics AI Gateway (OpenAI-compatible) for recommendations + diagnostics
8. **Dark theme forced** — `.streamlit/config.toml` ensures consistent brand appearance

---

## Snowflake Objects (HISTLOAD_DB.META)

### Configuration Tables
| Table | Purpose |
|-------|---------|
| DMT_SETTINGS | App-level settings (AI keys, allowed sources, models) |
| CONNECTION_PROFILES | Source database connection registry |
| MIGRATION_CONFIG | Per-table pipeline configuration |
| FILE_INGESTION_CONFIG | File Loader job configurations |

### Tracking Tables
| Table | Purpose |
|-------|---------|
| RUN_LOG | Batch-level audit trail (per table per run) |
| PIPELINE_STEP_LOG | Step-level state machine (for resume) |
| FILE_MANIFEST | Tracks extracted files across storage backends |
| FILE_INGESTION_LOG | File Loader execution logs |

### Alert Tables
| Table | Purpose |
|-------|---------|
| DMT_ALERT_RULES | Configurable alert rules (stale, failed, drift) |
| DMT_ALERT_LOG | Alert trigger history |

### Stages & Formats
| Object | Purpose |
|--------|---------|
| DMT_STAGE | Internal landing stage for extractions |
| DMT_UPLOAD_STAGE | Internal stage for Upload & Ingest |
| PARQUET_FMT, TSV_ZSTD_FMT, CSV_FMT, BCP_PIPE_FMT | Pre-defined file formats |

---

## Extraction Engine Matrix

| Source | Full Load | Incremental | Output Format | Parallelism |
|--------|-----------|-------------|---------------|-------------|
| MySQL | mysqlsh `util.dumpTables` | connectorx → Arrow | TSV+zstd / Parquet | mysqlsh built-in |
| Teradata | TPT `tbuild` | teradatasql | CSV / Parquet | TPT sessions |
| MSSQL | BCP utility | BCP `queryout` | Pipe-CSV+gzip | Split chunks |
| Oracle | oracledb | oracledb | Parquet | ThreadPoolExecutor (>500MB) |

---

## AI Integration

```
┌──────────────┐     ┌──────────────────────┐     ┌──────────────┐
│ App Feature  │ ──→ │ Tiger Analytics       │ ──→ │ AI Response  │
│ (config/ddl/ │     │ AI Gateway            │     │ (JSON/text)  │
│  history)    │     │ (OpenAI-compatible)   │     │              │
└──────────────┘     └──────────────────────┘     └──────────────┘
      │                       │
      ▼                       ▼
 Per-feature model     LLM_API_BASE + LLM_API_KEY
 (LLM_MODEL_CONFIG,    stored in DMT_SETTINGS
  LLM_MODEL_DDL,
  LLM_MODEL_HISTORY)
```

---

## Module Dependencies

```
app.py
├── views/* (UI rendering)
│   ├── metadata/* (data access)
│   └── utils/* (helpers, AI, theme)
│
core/orchestrator.py (CLI pipeline)
├── ddl_generators/* (schema mapping)
├── extractors/* (data extraction)
├── storage/* (file backends)
├── core/loader.py (Snowflake loading)
├── core/schema_drift.py
├── core/validator.py
├── core/file_manifest.py
└── metadata/* (config, tracking)
```

**No circular dependencies.** `core/` has zero Streamlit imports. `views/` never imports from `core/orchestrator.py` directly (uses subprocess for pipeline runs).

---

## Adding a New Source Type

1. Create `extractors/<source>_full.py` implementing `BaseExtractor.extract_full()`
2. Create `extractors/<source>_incremental.py` implementing `BaseExtractor.extract_incremental()`
3. Create `ddl_generators/<source>.py` with `map_<source>_type()`, `get_<source>_columns()`, `generate_and_apply()`
4. Add source spec in `metadata/source_specs.py`
5. Add connection test logic in `metadata/connection_manager.py`
6. Add source type to `ALLOWED_SOURCES` in DMT_SETTINGS
7. Update `core/orchestrator.py` to route to new extractor/DDL generator

---

*MigrateX v1.0 — Tiger Analytics · Developed by MDP*
