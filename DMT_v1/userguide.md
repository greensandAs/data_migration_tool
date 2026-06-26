# DMT v1 — User Guide

## Overview

DMT (Data Migration Toolkit) is a unified pipeline for migrating data from **MySQL** and **Teradata** to Snowflake. It provides:

- Step-based, resumable pipelines
- SCD Type 0/1/2 merge strategies
- Multi-cloud storage (internal stage, S3, Azure)
- Auto-discovery of source tables
- Schema drift detection
- Filter-based extraction
- Streamlit UI for configuration and monitoring

---

## 1. Setting Up Source Connections

### Navigate to: Connections page

### MySQL

| Field | Example |
|-------|---------|
| Profile Name | `mysql_prod` |
| Source Type | `mysql` |
| Host | `10.0.0.1` |
| Port | `3306` |
| Username | `etl_user` |
| Password | *(your password)* |

### Teradata

| Field | Example |
|-------|---------|
| Profile Name | `td_warehouse` |
| Source Type | `teradata` |
| Host | `tdserver.company.com` |
| Port | `1025` (not used by teradatasql) |
| Username | `dbc_user` |
| Password | *(your password)* |
| Auth Method | `TD2` (native) or `LDAP` |

### Test Connection

Click **"Test Connection"** on any profile to verify connectivity:
- **Success:** `Connected — MySQL 8.0.35` or `Connected — Default DB: HR_DB`
- **Failure:** Error message with details (check host, credentials, firewall)

---

## 2. Configuring Tables

### Navigate to: Config page → Select a connection profile

### Option A: Auto-Discover (Recommended)

1. Select your **connection profile** in the sidebar
2. Choose a **schema** (MySQL) or **database** (Teradata) from the dropdown
3. Click **"Generate Config"**
4. The system will:
   - List all tables in that schema/database
   - Detect primary keys (MySQL) or primary index (Teradata)
   - Detect watermark columns (timestamps for incremental loads)
   - Suggest full vs incremental load type
5. Review the discovered tables and click **"Add All"** or select individually

### Option B: Add Table Manually

Click **"Add Table"** button to open the dialog:

| Field | Description | Example |
|-------|-------------|---------|
| Source schema/DB | MySQL database or Teradata database | `employees` |
| Source table | Table name in source | `salaries` |
| Target table | Snowflake table name (auto-uppercased) | Leave blank for auto |
| Primary key | Column for MERGE dedup | `EMP_NO` |
| Watermark column | Timestamp/ID column for incremental | `UPDATED_AT` |
| Merge keys | Composite keys (comma-separated) | `EMP_NO, FROM_DATE` |
| Load type | `full` or `incremental` | `incremental` |
| SCD Type | 0=Append, 1=Upsert, 2=History | `1` |
| Filter Condition | Static WHERE clause | `region = 'US'` |
| Storage | `internal_stage`, `s3`, or `azure` | `internal_stage` |
| External Stage | Stage name (shown for S3/Azure) | `DMT_EXT_S3` |
| Partitions | Parallel threads for extraction | `8` |
| Active | Enable for pipeline runs | `True` |

### Target Table Naming

**MySQL:**
```
SOURCE: employees.salaries
TARGET: EMPLOYEES.RAW.SALARIES
```

**Teradata:**
```
SOURCE: HR_DB.EMPLOYEES
TARGET: HR_DB.RAW.EMPLOYEES_RAW  (default)
TARGET: DW.HR_DB.EMPLOYEES_RAW   (with TARGET_DB=DW, consolidation mode)
```

---

## 3. Editing Configuration

Click the **✏️** button on any table row to expand the edit card.

Fields you can modify:
- Load type, Watermark column, Primary key, Merge keys
- Partitions, Storage type, External stage
- **SCD Type** — change merge strategy
- **Filter Condition** — add/remove static WHERE clause
- **Target Schema** (Teradata only) — override Snowflake schema
- Active/Inactive toggle, Reconcile toggle

Click **"Save"** to persist changes. Click **"Delete"** to remove the table from config.

---

## 4. Running Pipelines

### Navigate to: Run page

### Execution Modes

| Mode | What it does | When to use |
|------|-------------|-------------|
| **Full Run** | DDL → Schema Drift → Extract → Upload → Load → Merge → Watermark | Normal pipeline execution |
| **Extract Only** | DDL → Schema Drift → Extract (+ Upload for S3) | Extract data without loading to Snowflake |
| **Load Only** | Load → Merge → Watermark | Load previously extracted files |
| **Resume** | Continue from last failed step | After fixing the root cause of a failure |

### Pipeline Steps (in order)

| Step | Description |
|------|-------------|
| `ddl` | Create target table in Snowflake (if not exists) |
| `schema_drift` | Detect new columns in source, ALTER TABLE ADD COLUMN |
| `extract` | Pull data from source (mysqlsh/TPT for full, connectorx/teradatasql for incremental) |
| `upload` | Push files to storage (PUT to stage, or boto3 to S3) |
| `load` | COPY INTO Snowflake from stage/S3 |
| `merge` | SCD0: INSERT / SCD1: MERGE / SCD2: close+insert |
| `watermark` | Update cursor (LAST_LOADED_AT) for next incremental run |

### Running from UI

1. Select connection profile in sidebar
2. Choose tables (or run all active)
3. Select execution mode
4. Click **"Run Pipeline"**
5. Monitor progress in real-time log output

### Running from CLI

```bash
# Run all active tables
python orchestrator.py

# Run single table
python orchestrator.py --table employees

# Force full reload (ignore watermark)
python orchestrator.py --full

# Resume from last failure
python orchestrator.py --resume

# Extract only (no load)
python orchestrator.py --mode EXTRACT_ONLY

# Load only (from prior extract)
python orchestrator.py --mode LOAD_ONLY
```

---

## 5. Pipeline Output — Good Path

### Successful Full Load (MySQL, internal stage)
```
[FULL/mysqlsh] employees.salaries -> EMPLOYEES.RAW.SALARIES
   DDL ready: EMPLOYEES.RAW.SALARIES (6 cols + audit)
   schema drift: no changes
   mysqlsh full dump -> ./export/mysql/mysql_prod/salaries/full
   full dump complete: 1 file(s)
   extracted: 2,844,047 rows -> 1 file(s)
   load: 1 file(s) on stage:
     employees/salaries/full/employees@salaries@@0.tsv.zst (48,231,456 bytes)
   COPY full: 2,844,047 rows into EMPLOYEES.RAW.SALARIES
   cleanup: deleted 1 local file(s)
   watermark: None -> 2026-06-24 03:00:00.000
   done (success) — 2,844,047 rows — 44.2s
```

### Successful Incremental Load (MySQL, S3)
```
[INCREMENTAL/connectorx] employees.salaries -> EMPLOYEES.RAW.SALARIES
   DDL ready: EMPLOYEES.RAW.SALARIES (6 cols + audit)
   schema drift: no changes
   connectorx query: SELECT * FROM `employees`.`salaries` WHERE `updated_at` > '2026-06-24...'
   rows fetched: 1,234
   extracted: 1,234 rows -> 1 file(s)
   uploaded: 1 file(s) -> s3://ta-dmt/dmt/mysql/mysql_prod/employees/salaries/incremental/
   cleanup: deleted 1 local file(s)
   merged: 1,234 rows
   SCD1 MERGE/INSERT: 1,234 rows into EMPLOYEES.RAW.SALARIES
   archive: moved 1 file(s) to processed/incremental/20260624/
   watermark: 2026-06-24 03:00:00.000 -> 2026-06-24 15:30:00.000
   done (success) — 1,234 rows — 12.8s
```

### Successful Full Load (Teradata, S3)
```
[FULL/tpt] HR_DB.EMPLOYEES -> HR_DB.RAW.EMPLOYEES_RAW
   DDL ready: HR_DB.RAW.EMPLOYEES_RAW (15 cols + audit)
   schema drift: no changes
   TPT script: ./export/teradata/td_warehouse/EMPLOYEES/full/HR_DB_EMPLOYEES_TPT_JOB.tpt
   running: tbuild -f ... -j ... -e UTF-8 -C
   TPT export complete: 500,000 rows exported
   output files: 3
   uploaded: 3 file(s) -> s3://ta-dmt/dmt/teradata/td_warehouse/HR_DB/EMPLOYEES/full/
   cleanup: deleted 3 local file(s)
   loaded: 500,000 rows from s3
   moved 3 file(s) to processed/full/20260624/
   watermark: None -> 2026-06-24 10:00:00.000
   done (success) — 500,000 rows — 95.3s
```

### Skipped (No New Data)
```
[INCREMENTAL/connectorx] employees.salaries -> EMPLOYEES.RAW.SALARIES
   DDL ready: EMPLOYEES.RAW.SALARIES (6 cols + audit)
   schema drift: no changes
   connectorx query: SELECT * FROM `employees`.`salaries` WHERE `updated_at` > '2026-06-24...'
   rows fetched: 0
   skipped: no new rows
   done (skipped) — 0 rows — 3.1s
```

---

## 6. Pipeline Output — Bad Path (Failures)

### Connection Failure
```
[FULL/mysqlsh] employees.salaries -> EMPLOYEES.RAW.SALARIES
   FAILED at step 'extract': Can't connect to MySQL server on '10.0.0.1:3306'
```
**Fix:** Check host/port/credentials in Connections page. Test connection first.

### S3 Upload Failure
```
[FULL/mysqlsh] employees.salaries -> EMPLOYEES.RAW.SALARIES
   DDL ready: EMPLOYEES.RAW.SALARIES (6 cols + audit)
   extracted: 300,024 rows -> 1 file(s)
   FAILED at step 'upload': An error occurred (NoSuchBucket) when calling PutObject
```
**Fix:** Verify the external stage URL and S3 bucket exist. Check storage integration permissions.

### Load Failure (Format Mismatch)
```
   FAILED at step 'load': Number of columns in file (8) does not match table (10)
```
**Fix:** Re-run DDL step or check if schema drift added columns after extraction.

### Resume After Failure
```
python orchestrator.py --resume
```
```
[FULL/mysqlsh] employees.salaries -> EMPLOYEES.RAW.SALARIES
   RESUMING from step 'upload' (run a15633bc9815)
   uploaded: 1 file(s) -> s3://ta-dmt/...
   loaded: 300,024 rows from s3
   watermark: None -> 2026-06-24 03:00:00.000
   done (success) — 300,024 rows — 18.5s
```

---

## 7. Checking Run History

### Navigate to: History page

### Time Slicer
Use the time-based filter at the top: `7d | 4d | 3d | 2d | 24hrs | 8hrs | 2hrs`

### Filters
- **Table** — filter by source table
- **Status** — `success`, `failed`, `skipped`
- **Load Type** — `full` or `incremental`
- **Date Range** — custom date picker

### Summary Cards
- **Total Runs** — count of pipeline executions in window
- **Success Rate** — percentage of successful runs
- **Failed** — count of failures
- **Avg Duration** — average run time in seconds

### Batch Grouping
Runs are grouped by batch (multiple tables in one execution). Expand a batch to see per-table results.

### AI Failure Explanation
Click on a failed run to see the error message. The system uses Cortex AI to provide a human-readable explanation and suggested fix.

---

## 8. SCD Types Explained

| Type | Behavior | Use Case | Target Table |
|------|----------|----------|-------------|
| **SCD 0** | Always INSERT, no dedup | Event logs, audit trails, append-only facts | Grows indefinitely |
| **SCD 1** | MERGE: UPDATE matched rows, INSERT new | Dimension tables where history isn't needed | Current state only |
| **SCD 2** | Close current record + INSERT new version | Dimension tables needing full change history | Versioned with `_VALID_FROM`, `_VALID_TO`, `_IS_CURRENT` |

### SCD2 Additional Columns
When SCD_TYPE = 2, these columns are auto-added:
- `_VALID_FROM` — when this version became active
- `_VALID_TO` — when this version was superseded (NULL = current)
- `_IS_CURRENT` — TRUE for the active version

---

## 9. Filter-Based Extraction

Set `FILTER_CONDITION` in the config to apply a static WHERE clause on every run:

```sql
-- Only extract US region data
FILTER_CONDITION = "region = 'US' AND status = 'active'"

-- Combined with incremental watermark, the query becomes:
SELECT * FROM db.table
WHERE (region = 'US' AND status = 'active')
  AND updated_at > '2026-06-24 00:00:00'
```

---

## 10. Storage Options

| Storage | How files move | Best for |
|---------|---------------|----------|
| `internal_stage` | PUT → @DMT_STAGE → COPY INTO | Simple setups, small-medium tables |
| `s3` | boto3 upload → external stage → COPY INTO | Large tables, multi-cloud, decoupled extract/load |
| `azure` | BlobServiceClient → external stage → COPY INTO | Azure-based environments |

### File Lifecycle (S3/Azure)
1. Extract → local files created
2. Upload → files pushed to `dmt/<source>/<connection>/<db>/<table>/<full|incremental>/`
3. Local files deleted
4. Load/Merge from external stage
5. Files moved to `processed/<full|incremental>/<date>/`

---

## 11. Schema Drift Detection

On every run, the DDL step compares source columns against the Snowflake target:
- **New columns** in source → `ALTER TABLE ADD COLUMN` (auto-applied)
- **Removed columns** in source → Warning only (Snowflake data preserved)

No action needed — this is automatic.

---

## 12. Reconciliation

When `RECONCILE = TRUE` on a table config:
- After load, compares source row count vs target row count
- Reports discrepancies in the run log
- Does NOT block the pipeline (informational only)

---

## 13. Admin: Restricting Source Types

Control which source types are available in the app via `DMT_SETTINGS`:

```sql
-- Restrict to MySQL only (for consumer deployments)
UPDATE HISTLOAD_DB.META.DMT_SETTINGS
SET SETTING_VALUE = 'mysql'
WHERE SETTING_KEY = 'ALLOWED_SOURCES';

-- Allow both MySQL and Teradata
UPDATE HISTLOAD_DB.META.DMT_SETTINGS
SET SETTING_VALUE = 'mysql,teradata'
WHERE SETTING_KEY = 'ALLOWED_SOURCES';
```

This controls the Source Type dropdown on the Connections page. No code changes needed.

---

## 14. Troubleshooting

| Issue | Cause | Fix |
|-------|-------|-----|
| "Unable to locate credentials" | AWS credentials not configured | Set `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` or use storage integration |
| "NoSuchBucket" | Stage URL incorrect | Run `DESCRIBE STAGE <name>` to verify URL |
| "tuple index out of range" | COPY INTO returned no results | Check file format matches data (TSV vs CSV vs Parquet) |
| "No files on stage" | Extract didn't produce files or upload failed | Check prior step logs, re-run |
| "SCD Type 2 requires PRIMARY_KEY" | Merge keys needed for history | Set PRIMARY_KEY in table config |
| Table not appearing in UI | Profile not selected or table inactive | Select profile in sidebar, check Active checkbox |

---

## 15. Key Tables (Snowflake)

| Table | Purpose |
|-------|---------|
| `HISTLOAD_DB.META.DMT_SETTINGS` | App-level settings (allowed sources, etc.) |
| `HISTLOAD_DB.META.CONNECTION_PROFILES` | Source connection registry |
| `HISTLOAD_DB.META.MIGRATION_CONFIG` | Per-table pipeline configuration |
| `HISTLOAD_DB.META.PIPELINE_STEP_LOG` | Step-level execution tracking |
| `HISTLOAD_DB.META.RUN_LOG` | Batch-level audit trail |
| `HISTLOAD_DB.META.FILE_MANIFEST` | Tracks extracted files across storage |
| `HISTLOAD_DB.META.DMT_STAGE` | Internal landing stage |
| `HISTLOAD_DB.META.DMT_EXT_S3` | External S3 stage (if configured) |
