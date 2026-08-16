# MSSQL → Snowflake Data Migration Tool

A production-ready, self-service data migration console that moves data from **Azure SQL (MSSQL)** to **Snowflake** using an enterprise-grade pipeline: **BCP Export → GZip → Azure Blob → COPY INTO → MERGE**.

Built with a Streamlit UI for operations teams to configure, execute, and monitor migrations without writing code.

---

## What This Tool Does

- Extracts data from Azure SQL Server using Microsoft's native **BCP** (Bulk Copy Program)
- Compresses and uploads to **Azure Blob Storage** via `azcopy`
- Loads into Snowflake through an **External Stage** using `COPY INTO`
- Applies the correct merge strategy (**SCD Type 0, 1, or 2**) based on table configuration
- Tracks every step in a centralized **LOG_TABLE** for full audit trail
- Supports **parallel execution** of 100+ tables with configurable worker threads

---

## Business Value

| Capability | Benefit |
|---|---|
| Self-Service UI | Operations teams run migrations without DBA involvement |
| Auto-Discovery | Scans MSSQL schemas and auto-detects primary keys and CDC columns |
| Parallel Execution | Migrates 100+ tables concurrently — hours become minutes |
| SCD Support | Maintains historical data accuracy with Type 0/1/2 merge logic |
| Auto-Sync Config | Local JSON and Snowflake CONFIG_TABLE stay in sync automatically |
| Full Audit Trail | Every run is logged with source/target counts, duration, and SQL used |
| Zero-Downtime Loads | Full loads use atomic `ALTER TABLE SWAP` — no reader disruption |
| Incremental CDC | Only moves changed rows — reduces cost and load on source systems |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         STREAMLIT UI (app.py)                            │
│         Configuration  │  Run (Parallel)  │  Results / History          │
└────────────────────────┼─────────────────────────────────────────────────┘
                         │
              ┌──────────▼──────────┐
              │   core/pipeline.py  │  ← Orchestrator (ThreadPoolExecutor)
              └──────────┬──────────┘
                         │
         ┌───────────────┼───────────────────┐
         ▼               ▼                   ▼
┌────────────────┐ ┌────────────────┐ ┌────────────────┐
│ core/extract.py│ │ core/upload.py │ │ core/load.py   │
│                │ │                │ │                │
│ • BCP Export   │ │ • azcopy cp    │ │ • COPY INTO WRK│
│ • CDC Builder  │ │ • move to      │ │ • MERGE (SCD)  │
│ • Split + GZip │ │   processed/   │ │ • SWAP (full)  │
└────────┬───────┘ └────────┬───────┘ └────────┬───────┘
         │                  │                   │
         ▼                  ▼                   ▼
  ┌─────────────┐   ┌──────────────┐   ┌──────────────────┐
  │  Azure SQL  │   │  Azure Blob  │   │    Snowflake     │
  │  (Source)   │   │  (Landing)   │   │    (Target)      │
  └─────────────┘   └──────────────┘   └──────────────────┘
```

### Supporting Modules

| Module | Responsibility |
|---|---|
| `core/connections.py` | Snowflake + MSSQL connection management |
| `core/config.py` | JSON config + auto-sync to Snowflake CONFIG_TABLE |
| `core/logger.py` | Audit trail — LOG_TABLE operations |

---

## Application Flow

### Tab 1 — Configuration

1. **Discover**: Scans MSSQL `INFORMATION_SCHEMA` to find tables, primary keys, and CDC columns
2. **Add Manually**: Full form for custom table configuration
3. **Manage**: Enable/disable/edit/delete existing table configs
4. **Auto-Sync**: Every save writes to local JSON AND Snowflake CONFIG_TABLE simultaneously

### Tab 2 — Run

1. Select tables (multi-select, defaults to all active)
2. Choose execution mode: **FULL**, **EXPORT**, or **LOAD**
3. Click "Start Migration" → spawns parallel workers (up to 12)
4. Real-time progress bar with success/failure counter
5. Per-table logs displayed on completion

### Tab 3 — Results

- Historical view of all migration runs from LOG_TABLE
- KPI metrics: total jobs, success rate, average duration
- Full detail table with source/target counts

---

## Pipeline Execution Modes

### FULL Mode (End-to-End)

```
BCP Export → GZip → Upload to Blob → COPY INTO _WRK → MERGE/SWAP → Move to processed/
```

Runs the complete pipeline from source extraction through to target loading.

### EXPORT Mode (Extract Only)

```
BCP Export → GZip → Upload to Blob
```

Extracts and stages data without loading. Useful for pre-staging before a maintenance window.

### LOAD Mode (Load Only)

```
Check files in stage → COPY INTO _WRK → MERGE/SWAP → Move to processed/
```

Loads previously staged files. Useful for re-running failed loads without re-extracting.

---

## Load Types

### Full Load

**Use case**: Initial load, reference tables, small tables where full refresh is acceptable.

**Pipeline**:
```
1. Check files exist in @STAGE/{TABLE}/
2. CREATE TABLE _WRK (clone structure of target)
3. TRUNCATE _WRK
4. COPY INTO _WRK FROM @STAGE/{TABLE}/
5. ALTER TABLE target SWAP WITH _WRK  ← atomic, zero-downtime
6. Recreate empty _WRK for next run
7. Move files to processed/
```

**Why SWAP?**
- Instant operation (metadata-only, no data movement)
- Zero reader disruption — queries never see partial data
- Falls back to `DELETE + INSERT` if SWAP fails (permissions)

### Incremental Load

**Use case**: Large transactional tables where only new/changed rows should move.

**Pipeline**:
```
1. Get last CDC watermark from LOG_TABLE
2. BCP export with WHERE clause (only changed rows)
3. GZip → Upload → COPY INTO _WRK
4. MERGE from _WRK into target (based on SCD type)
5. Move files to processed/
```

**CDC Modes**:
- **TIMESTAMP**: Uses `modified_at`/`updated_at` columns — captures rows changed since last successful run
- **ID**: Uses auto-increment ID — captures rows with ID greater than last loaded max

### Filter Load

**Use case**: Reload a specific partition or date range (e.g., "reload all orders from July 2026").

**Pipeline**:
```
1. BCP export with user-defined WHERE clause
2. GZip → Upload → COPY INTO _WRK
3. DELETE FROM target WHERE {filter_condition}
4. INSERT INTO target SELECT * FROM _WRK
5. Move files to processed/
```

---

## SCD (Slowly Changing Dimension) Types

### SCD Type 0 — Append Only

**Behavior**: Insert all rows from source. No dedup, no update.

**SQL**:
```sql
INSERT INTO target SELECT * FROM _WRK;
```

**Use case**: Event/log tables, audit trails, immutable facts.

### SCD Type 1 — Upsert (Current State)

**Behavior**: Update existing rows with new values, insert new rows.

**SQL**:
```sql
MERGE INTO target AS T
USING _WRK AS S
ON T.PK = S.PK
WHEN MATCHED THEN UPDATE SET T.col1=S.col1, T.col2=S.col2, ...
WHEN NOT MATCHED THEN INSERT (all_cols) VALUES (S.all_cols);
```

**Key details**:
- Primary key columns are **excluded** from the UPDATE SET (no point updating join keys)
- Single-pass MERGE statement (cost-efficient, no multi-scan)
- Returns inserted/updated counts for audit

**Use case**: Dimension tables where you only need the current version (customers, products).

### SCD Type 2 — History Tracking

**Behavior**: Preserves full change history. Changed rows are expired (marked inactive) and new versions are inserted.

**Required target columns**: `IS_CURRENT` (BOOLEAN), `EFF_START_DATE` (TIMESTAMP), `EFF_END_DATE` (TIMESTAMP)

**SQL (2-step)**:
```sql
-- Step 1: Expire changed rows
UPDATE target SET
  IS_CURRENT = FALSE,
  EFF_END_DATE = CURRENT_TIMESTAMP()
WHERE IS_CURRENT = TRUE
  AND EXISTS (SELECT 1 FROM _WRK WHERE PK matches AND data differs);

-- Step 2: Insert new versions
INSERT INTO target (cols..., IS_CURRENT, EFF_START_DATE, EFF_END_DATE)
SELECT cols..., TRUE, CURRENT_TIMESTAMP(), NULL
FROM _WRK
WHERE NOT EXISTS (SELECT 1 FROM target WHERE PK matches AND IS_CURRENT=TRUE AND data unchanged);
```

**Use case**: Dimensions requiring historical tracking (customer address history, employee role changes, pricing history).

**Falls back to SCD1** if `IS_CURRENT`/`EFF_START_DATE`/`EFF_END_DATE` columns are not present in the target table.

---

## Project Structure

```
migration-dashboard/
├── app.py                      # Streamlit UI (tabs, branding, parallel runner)
├── core/
│   ├── __init__.py
│   ├── connections.py          # Snowflake + MSSQL connections
│   ├── config.py               # JSON config + Snowflake auto-sync
│   ├── extract.py              # BCP export, CDC builder, split+gzip
│   ├── upload.py               # azcopy upload, move-to-processed
│   ├── load.py                 # COPY INTO, MERGE (SCD 0/1/2), SWAP
│   ├── logger.py               # LOG_TABLE audit trail
│   └── pipeline.py             # Orchestrator (single + parallel execution)
├── assets/
│   ├── logos/                   # Tiger Analytics logos (dark/light/mono)
│   └── brand/brand_tokens.py   # Brand color tokens
├── .env                        # Credentials (MSSQL, Snowflake, Azure)
├── .streamlit/
│   ├── config.toml             # Streamlit theme
│   └── secrets.toml            # Streamlit secrets (alternative to .env)
├── migration_config.json       # Active table configurations
├── requirements.txt            # Python dependencies
└── scripts/
    └── setup_azure_sql.sql     # Test data seeding (3 tables, 650 rows)
```

---

## Snowflake Objects

| Object | Purpose |
|---|---|
| `DATA_MIGRATION.CONTROL.CONFIG_TABLE` | Table configurations (auto-synced from app) |
| `DATA_MIGRATION.CONTROL.LOG_TABLE` | Audit log — every job step tracked |
| `DATA_MIGRATION.CONTROL.MIGRATION_STAGE` | External stage pointing to Azure Blob |
| `ANALYTICS.PUBLIC.*` | Target tables (CUSTOMERS, ORDERS, etc.) |
| `ANALYTICS.PUBLIC_WRK.*` | Work tables (temporary, used during load) |

---

## Deployment Guide

### Prerequisites

| Component | Version | Purpose |
|---|---|---|
| Python | 3.9+ | Runtime |
| BCP (mssql-tools) | 18+ | MSSQL bulk export |
| azcopy | 10+ | Azure Blob upload |
| ODBC Driver | 17 or 18 | pyodbc connection to MSSQL |
| Snowflake Account | Any | Target data warehouse |
| Azure Blob Storage | Any | Intermediate landing zone |

### Step 1 — Clone and Install

```bash
# Clone the repository
git clone <repo-url>
cd migration-dashboard

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate   # Windows

# Install dependencies
pip install -r requirements.txt
```

### Step 2 — Install System Dependencies

**Ubuntu/Debian:**
```bash
# BCP and ODBC Driver
curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc
sudo add-apt-repository "$(curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list)"
sudo apt-get update
sudo apt-get install -y mssql-tools18 unixodbc-dev

# azcopy
wget https://aka.ms/downloadazcopy-v10-linux -O azcopy.tar.gz
tar -xf azcopy.tar.gz --strip-components=1
sudo mv azcopy /usr/local/bin/
```

**Windows:**
```powershell
# BCP comes with SQL Server Command Line Utilities
# Download: https://learn.microsoft.com/en-us/sql/tools/sqlcmd/sqlcmd-utility

# azcopy
# Download: https://aka.ms/downloadazcopy-v10-windows
```

### Step 3 — Configure Environment

Create `.env` in the `migration-dashboard/` folder:

```env
# ─── MSSQL Source ───
MSSQL_SERVER=your-server.database.windows.net
MSSQL_USER=your_user
MSSQL_PASSWORD=your_password
MSSQL_DRIVER=ODBC Driver 18 for SQL Server

# ─── Snowflake Target ───
SF_ACCOUNT=your_account.region.cloud
SF_USER=your_user
SF_PASSWORD=your_password
SF_WAREHOUSE=COMPUTE_WH
SF_DATABASE=DATA_MIGRATION
SF_SCHEMA=CONTROL
SF_ROLE=ACCOUNTADMIN
SF_STAGE=@DATA_MIGRATION.CONTROL.MIGRATION_STAGE

# ─── Azure Blob ───
CLOUD_PATH=https://your_storage.blob.core.windows.net/your_container/
AZ_SAS_TOKEN=sv=2026-...&sig=...

# ─── Local ───
EXPORT_DIR=./export
```

### Step 4 — Set Up Snowflake Objects

Run these once in your Snowflake account:

```sql
-- Control database
CREATE DATABASE IF NOT EXISTS DATA_MIGRATION;
CREATE SCHEMA IF NOT EXISTS DATA_MIGRATION.CONTROL;

-- Config table
CREATE TABLE IF NOT EXISTS DATA_MIGRATION.CONTROL.CONFIG_TABLE (
    JOB_ID INT AUTOINCREMENT,
    MSSQL_DATABASE_NAME VARCHAR,
    MSSQL_SCHEMA_NAME VARCHAR,
    MSSQL_TABLE_NAME VARCHAR,
    SF_DATABASE_NAME VARCHAR,
    SF_SCHEMA_NAME VARCHAR,
    SF_TABLE_NAME VARCHAR,
    WAREHOUSE_NAME VARCHAR,
    SCD_TYPE INT,
    LOAD_TYPE VARCHAR,
    CDC_COLUMNS VARCHAR,
    PRIMARY_KEY VARCHAR,
    DELIMITER VARCHAR DEFAULT '|',
    FILTER_CONDITION VARCHAR,
    TRIM VARCHAR DEFAULT 'N',
    ENCRYPTION_COLUMNS VARCHAR,
    S3_PATH VARCHAR,
    CUSTOM_SQL VARCHAR,
    EXECUTION_MODE VARCHAR DEFAULT 'FULL',
    CDC_TYPE VARCHAR DEFAULT 'TIMESTAMP',
    ENABLED VARCHAR DEFAULT 'Y'
);

-- Log table
CREATE TABLE IF NOT EXISTS DATA_MIGRATION.CONTROL.LOG_TABLE (
    BATCH_ID INT,
    JOB_ID INT,
    MSSQL_DATABASE_NAME VARCHAR,
    MSSQL_SCHEMA_NAME VARCHAR,
    MSSQL_TABLE_NAME VARCHAR,
    SF_DATABASE_NAME VARCHAR,
    SF_SCHEMA_NAME VARCHAR,
    SF_TABLE_NAME VARCHAR,
    LOAD_TYPE VARCHAR,
    S3_PATH VARCHAR,
    EXECUTION_MODE VARCHAR,
    EXPORT_FILENAME VARCHAR,
    MSSQL_TABLE_COUNT INT,
    SF_TABLE_COUNT INT,
    JOB_START_TIME TIMESTAMP,
    JOB_END_TIME TIMESTAMP,
    JOB_DURATION INT,
    BCP_EXPORT_STATUS VARCHAR,
    BCP_EXPORT_LOG VARCHAR,
    S3_UPLOAD_STATUS VARCHAR,
    S3_UPLOAD_LOG VARCHAR,
    COPY_COMMAND VARCHAR(10000),
    COPY_COMMAND_STATUS VARCHAR,
    COPY_COMMAND_LOG VARCHAR,
    MERGE_STATEMENT VARCHAR(10000),
    MERGE_STATEMENT_STATUS VARCHAR,
    MERGE_STATEMENT_LOG VARCHAR,
    FINAL_STATUS VARCHAR,
    INGESTION_COMPLETED VARCHAR
);

-- External stage (Azure Blob)
CREATE OR REPLACE STAGE DATA_MIGRATION.CONTROL.MIGRATION_STAGE
    URL = 'azure://your_storage.blob.core.windows.net/your_container/'
    CREDENTIALS = (AZURE_SAS_TOKEN = 'your_sas_token');

-- Target database
CREATE DATABASE IF NOT EXISTS ANALYTICS;
CREATE SCHEMA IF NOT EXISTS ANALYTICS.PUBLIC;
CREATE SCHEMA IF NOT EXISTS ANALYTICS.PUBLIC_WRK;
```

### Step 5 — Run the Application

```bash
cd migration-dashboard
streamlit run app.py --server.port 8501
```

The app opens at `http://localhost:8501`.

---

## Deploying to a New Server

The tool is designed for **portable deployment**. To move it to another VM or server:

```bash
# 1. Copy the project folder (exclude .venv and export/)
scp -r migration-dashboard/ user@new-server:/opt/migration/

# 2. SSH into the new server
ssh user@new-server

# 3. Install system deps (BCP, azcopy, ODBC driver) — see Step 2 above

# 4. Set up Python
cd /opt/migration/migration-dashboard
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 5. Update .env with the new server's credentials
nano .env

# 6. Run
streamlit run app.py --server.port 8501
```

**What to change per environment:**
- `.env` — credentials (MSSQL, Snowflake, Azure SAS token)
- `migration_config.json` — clear or update table list (or pull from Snowflake via the UI)

**What stays the same:**
- All Python code (core/, app.py)
- Snowflake objects (once created, shared across all deployment servers)
- Pipeline logic and SCD merge behavior

---

## Running as a Background Service

### systemd (Linux)

```ini
# /etc/systemd/system/migration-dashboard.service
[Unit]
Description=MSSQL to Snowflake Migration Dashboard
After=network.target

[Service]
Type=simple
User=appuser
WorkingDirectory=/opt/migration/migration-dashboard
Environment="PATH=/opt/migration/migration-dashboard/.venv/bin:/usr/local/bin:/usr/bin"
ExecStart=/opt/migration/migration-dashboard/.venv/bin/streamlit run app.py --server.port 8501 --server.headless true
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable migration-dashboard
sudo systemctl start migration-dashboard
```

---

## Configuration Reference

Each table entry in `migration_config.json` supports:

| Field | Type | Description |
|---|---|---|
| `source_db` | string | MSSQL database name |
| `source_schema` | string | MSSQL schema (default: `dbo`) |
| `source_table` | string | MSSQL table name |
| `target_db` | string | Snowflake database (default: `ANALYTICS`) |
| `target_schema` | string | Snowflake schema (default: `PUBLIC`) |
| `target_table` | string | Snowflake table name |
| `primary_key` | string | Comma-separated PK columns |
| `load_type` | string | `full`, `incremental`, or `filter` |
| `scd_type` | int | `0`, `1`, or `2` |
| `cdc_columns` | string | Columns used for change detection |
| `cdc_type` | string | `TIMESTAMP` or `ID` |
| `filter_condition` | string | SQL WHERE clause (for filter load) |
| `custom_sql` | string | Custom SELECT for BCP export |
| `delimiter` | string | Field delimiter (default: `|`) |
| `trim` | string | `Y`/`N` — trim whitespace |
| `encryption_columns` | string | Columns to encrypt (future use) |
| `execution_mode` | string | `FULL`, `EXPORT`, or `LOAD` |
| `warehouse_name` | string | Snowflake warehouse |
| `active` | boolean | Enable/disable table |

---

## Troubleshooting

| Issue | Cause | Fix |
|---|---|---|
| BCP "Login failed" | Wrong credentials or firewall | Check `.env` MSSQL_* vars; whitelist VM IP in Azure SQL firewall |
| BCP "procedure request failed" | Using `queryout` with table name | Ensure `1=1` condition uses `out` not `queryout` |
| azcopy 403 Forbidden | SAS token expired or IP-restricted | Generate new SAS token without IP restrictions |
| COPY INTO loads 0 rows | No files in stage path | Run EXPORT mode first, or check `LIST @stage/TABLE/` |
| Duplicate rows in target | Old files still in blob | Clean blob with `REMOVE @stage/TABLE/` before re-running |
| "User is empty" Snowflake | Missing SF_USER in .env | Verify all SF_* environment variables are set |
| Target count > Source count | Multiple runs without cleanup | Use FULL load (SWAP replaces entirely) or clean blob first |

---

## Technology Stack

- **UI**: Streamlit (Python)
- **Extract**: BCP (Microsoft SQL Server command-line tool)
- **Transport**: azcopy (Azure CLI tool for blob operations)
- **Load**: Snowflake COPY INTO + MERGE
- **Orchestration**: Python `concurrent.futures.ThreadPoolExecutor`
- **Config Store**: Local JSON + Snowflake CONFIG_TABLE (auto-synced)
- **Audit**: Snowflake LOG_TABLE

---

*Built by Tiger Analytics*
