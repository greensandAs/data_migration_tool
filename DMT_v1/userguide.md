# MigrateX — User Guide

**Tiger Analytics · Developed by MDP**
Version 1.0

---

## Overview

MigrateX is an enterprise data migration platform that accelerates your journey from source databases to Snowflake. It provides:

- **4 Source Databases:** MySQL, Teradata, MSSQL, Oracle
- **Resumable Pipelines:** Step-level tracking with automatic retry from failure
- **SCD Strategies:** Type 0 (Append), Type 1 (Upsert), Type 2 (History)
- **File Ingestion:** Load CSV, Parquet, JSON, Avro from cloud stages
- **Auto-Ingest:** Snowpipe DDL generation for continuous loading
- **AI-Powered:** Recommendations, failure explanations, schema validation
- **Observability:** Run logs, health dashboard, configurable alerts

---

## App Layout

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  MigrateX  Accelerate Your Data Journey to Snowflake    ⛁ rn16376 ⚙ COMPUTE_WH 🛡 ACCOUNTADMIN │
├─────────────────────────────────────────────────────────────────────────────┤
│  [🔌 All Sources (3)]  [📡 All Connections (5)]               [🤖 AI ○ ]  │
├──────────┬──────────────────────────────────────────────────────────────────┤
│ SIDEBAR  │                                                                  │
│          │                   PAGE CONTENT                                    │
│ DB MIGR. │                                                                  │
│ ├ Overview│                                                                  │
│ ├ Pipeline│                                                                  │
│ ├ Schema │                                                                  │
│ ├ Execute│                                                                  │
│ ├ Observe│                                                                  │
│ └ Sources│                                                                  │
│          │                                                                  │
│ FILE ING.│                                                                  │
│ ├ Loader │                                                                  │
│ └ Auto   │                                                                  │
└──────────┴──────────────────────────────────────────────────────────────────┘
│              Powered by Tiger Analytics · Developed by MDP                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Header bar:** Shows on all pages. Source/Connection filters + AI toggle only appear on Database Migration pages.

**Screenshot:** *App header with MigrateX branding, Snowflake context (account, warehouse, role), source filter, and AI toggle*
![App Header](docs/screenshots/app_header.png)

**Screenshot:** *Sidebar navigation with Database Migration and File Ingestion groups*
![Sidebar Navigation](docs/screenshots/sidebar_nav.png)

---

## Page 1: Sources — Connection Management

### What You See

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ SOURCES                                                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─ Add New Connection ───────────────────────────────────────────────────┐ │
│  │                                                                         │ │
│  │  Profile Name: [_______________]    Source Type: [mysql ▾]             │ │
│  │                                                                         │ │
│  │  Host: [_______________]            Port: [3306___]                    │ │
│  │                                                                         │ │
│  │  Username: [___________]            Password: [••••••••]              │ │
│  │                                                                         │ │
│  │  [Extra fields based on source type...]                                │ │
│  │                                                                         │ │
│  │  [🔌 Test Connection]              [💾 Save Connection]               │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌─ Existing Connections ─────────────────────────────────────────────────┐ │
│  │                                                                         │ │
│  │  🟢 mysql_prod          MYSQL    10.0.0.1:3306         [✏️] [🗑️]     │ │
│  │  🟢 td_warehouse        TERADATA tdserver.com:1025     [✏️] [🗑️]     │ │
│  │  🟢 mssql_erp           MSSQL    sqlserver.corp:1433   [✏️] [🗑️]     │ │
│  │  🟢 oracle_hr           ORACLE   oradb.company:1521    [✏️] [🗑️]     │ │
│  │                                                                         │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Screenshot:** *Sources page showing the connection form and existing connections list*
![Sources Page](docs/screenshots/sources_page.png)

### Step-by-Step: Add a New Connection

1. Click **"Sources"** in the sidebar navigation
2. In the **"Add New Connection"** form:
   - Enter a **Profile Name** (e.g., `oracle_finance`)
   - Select **Source Type** from dropdown (mysql / teradata / mssql / oracle)
   - Fill in **Host** and **Port** (port auto-fills based on source type)
   - Enter **Username** and **Password**
   - For MSSQL: also enter **Database** name
   - For Oracle: also enter **Service Name**
   - For Teradata: select **Auth Method** (TD2 or LDAP)
3. Click **"🔌 Test Connection"** to verify connectivity
   - Success: Shows green message with server version
   - Failure: Shows red message with error details
4. Click **"💾 Save Connection"** to persist

### Step-by-Step: Edit or Delete

1. Find the connection in the **Existing Connections** list
2. Click **✏️** to expand edit form (pre-filled with current values)
3. Modify fields and click **"Save"**
4. Or click **🗑️** to permanently delete

---

## Page 2: Pipeline Setup — Table Configuration

### What You See

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ PIPELINE SETUP                                                               │
├─────────────────────────────────────────────────────────────────────────────┤
│ [🔌 MYSQL (1) ▾]  [📡 mysql_prod ▾]                         [🤖 AI ● ]   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─ Config Summary ───────────────────────────────────────────────────────┐ │
│  │  ┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐                      │ │
│  │  │   10   │  │   1    │  │   8    │  │  11/11 │                      │ │
│  │  │ Tables │  │ Failed │  │ Incr.  │  │ Active │                      │ │
│  │  └────────┘  └────────┘  └────────┘  └────────┘                      │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  Schema/DB: [employees ▾]   Search: [__________]   View: [Cards ▾]        │
│                                                                              │
│  ┌─ Generate Config ──┐  ┌─ Add Table ──┐  ┌─ AI Recommend ──┐            │
│  │  [Generate Config]  │  │ [+ Add Table]│  │ [🤖 Recommend] │            │
│  └────────────────────┘  └──────────────┘  └─────────────────┘            │
│                                                                              │
│  ┌─ Table Cards ──────────────────────────────────────────────────────────┐ │
│  │                                                                         │ │
│  │  ┌──────────────────────────────┐ ┌──────────────────────────────┐    │ │
│  │  │ ✅ SUCCESS                    │ │ ❌ FAILED                     │    │ │
│  │  │ 🟢 EMP_SALARIES              │ │ 🟢 DEPT_MANAGER               │    │ │
│  │  │ incremental · SCD1           │ │ full · SCD0                   │    │ │
│  │  │ → EMPLOYEES.RAW.EMP_SALARIES │ │ → EMPLOYEES.RAW.DEPT_MANAGER │    │ │
│  │  │ 2,844,047 rows at 14:30      │ │ Failed: load step            │    │ │
│  │  │ [✏️ Edit] [⏸ Pause] [🗑️]   │ │ [✏️ Edit] [⏸ Pause] [🗑️]   │    │ │
│  │  └──────────────────────────────┘ └──────────────────────────────┘    │ │
│  │                                                                         │ │
│  │  ┌──────────────────────────────┐ ┌──────────────────────────────┐    │ │
│  │  │ ⏳ PENDING                    │ │ ✅ SUCCESS                    │    │ │
│  │  │ 🟢 EMPLOYEES                  │ │ 🟢 DEPARTMENTS               │    │ │
│  │  │ full · SCD1                   │ │ incremental · SCD2           │    │ │
│  │  │ → EMPLOYEES.RAW.EMPLOYEES    │ │ → EMPLOYEES.RAW.DEPARTMENTS  │    │ │
│  │  └──────────────────────────────┘ └──────────────────────────────┘    │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  Page 1 of 2 (11 tables)   [◀ Prev]  [Next ▶]                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Screenshot:** *Pipeline Setup showing config summary cards, table grid, and Generate Config button*
![Pipeline Setup Page](docs/screenshots/pipeline_setup_page.png)

**Screenshot:** *Add Table dialog with AI Recommend button*
![Add Table Dialog](docs/screenshots/pipeline_setup_add_table.png)

### Step-by-Step: Auto-Discover Tables

1. Select your **Source Type** (e.g., MYSQL) and **Connection** (e.g., mysql_prod) from header
2. Choose a **Schema/DB** from the dropdown (e.g., `employees`)
3. Click **"Generate Config"**
4. Review discovered tables:
   - Primary keys auto-detected
   - Watermark columns identified (timestamps)
   - Load type suggested (full vs incremental)
5. Click **"Add All"** to add all discovered tables, or select individually

### Step-by-Step: Add a Table Manually

1. Click **"+ Add Table"** button
2. Fill in the dialog:
   - **Source Schema/DB:** `employees`
   - **Source Table:** `salaries`
   - **Primary Key:** `EMP_NO`
   - **Watermark Column:** `UPDATED_AT`
   - **Load Type:** `incremental`
   - **SCD Type:** `1` (Upsert)
3. (Optional) Click **"🤖 AI Recommend"** for AI-suggested settings
4. Click **"Save"**

### Step-by-Step: Edit a Table

1. Find the table card in the grid
2. Click **"✏️ Edit"** to expand inline editor
3. Modify: Load type, SCD type, Keys, Filter condition, Storage, Partitions
4. Click **"Save"** to persist

### Step-by-Step: AI Recommend (all sources)

1. Enable **🤖 AI** toggle in header
2. Click **"🤖 Recommend"** on any table's Add dialog
3. AI analyzes column names and suggests:
   - Best SCD type for the table
   - Appropriate load strategy (full vs incremental)
   - Merge keys based on column patterns
4. Accept or modify suggestions, then Save

---

## Page 3: Schema Mapping — DDL Conversion

### What You See

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ SCHEMA MAPPING                                                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Table: [employees.salaries ▾]                    [🤖 Validate Mapping]    │
│                                                                              │
│  ┌─ Source DDL ─────────────────┐  ┌─ Snowflake DDL ─────────────────────┐ │
│  │                               │  │                                     │ │
│  │ CREATE TABLE `salaries` (     │  │ CREATE TABLE EMPLOYEES.RAW.SALARIES │ │
│  │   `emp_no`    INT NOT NULL,   │  │ (                                   │ │
│  │   `salary`    INT NOT NULL,   │  │   "EMP_NO"    NUMBER(38,0) NOT NULL,│ │
│  │   `from_date` DATE NOT NULL,  │  │   "SALARY"    NUMBER(38,0) NOT NULL,│ │
│  │   `to_date`   DATE NOT NULL,  │  │   "FROM_DATE" DATE NOT NULL,        │ │
│  │   PRIMARY KEY (`emp_no`,      │  │   "TO_DATE"   DATE NOT NULL,        │ │
│  │                `from_date`)   │  │   "_LOAD_TS"  TIMESTAMP_NTZ ...,   │ │
│  │ );                            │  │   "_SRC_FILE" VARCHAR,              │ │
│  │                               │  │   "_BATCH_ID" VARCHAR               │ │
│  │                               │  │ );                                   │ │
│  └───────────────────────────────┘  └─────────────────────────────────────┘ │
│                                                                              │
│  ┌─ Column Type Mapping ─────────────────────────────────────────────────┐  │
│  │                                                                        │  │
│  │  Source Column    Source Type     Snowflake Type      Notes            │  │
│  │  ─────────────    ───────────     ──────────────      ─────            │  │
│  │  emp_no           INT             NUMBER(38,0)        PK               │  │
│  │  salary           INT             NUMBER(38,0)                         │  │
│  │  from_date        DATE            DATE                PK               │  │
│  │  to_date          DATE            DATE                                 │  │
│  │                                                                        │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌─ AI Validation ───────────────────────────────────────────────────────┐  │
│  │ 🤖 All type mappings look correct. No data loss risk detected.        │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Screenshot:** *Schema Mapping with side-by-side source/Snowflake DDL and column type table*
![Schema Mapping Page](docs/screenshots/schema_mapping_page.png)

### Step-by-Step: View Schema Mapping

1. Click **"Schema Mapping"** in sidebar
2. Select a **table** from the dropdown
3. View:
   - **Left panel:** Original source DDL (formatted)
   - **Right panel:** Generated Snowflake CREATE TABLE DDL
   - **Column mapping table:** Each column with source → target type

### Step-by-Step: AI Validate

1. Enable **🤖 AI** toggle in header
2. Select a table
3. Click **"🤖 Validate Mapping"**
4. AI reviews type conversions and warns about:
   - Potential precision loss (e.g., FLOAT → NUMBER)
   - Truncation risks (e.g., VARCHAR(MAX) → VARCHAR(16M))
   - Missing NOT NULL constraints
5. Green result = safe, Yellow = warnings to review

---

## Page 4: Execute — Run Pipelines

### What You See

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ EXECUTE                                                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─ Run Pipeline ────────────────────────────────────────────────────────┐  │
│  │                                                                        │  │
│  │  Execution Mode:                                                       │  │
│  │  ( ● Full Run )  ( ○ Extract Only )  ( ○ Load Only )  ( ○ Resume )   │  │
│  │                                                                        │  │
│  │  Tables:                                                               │  │
│  │  ( ● All Active Tables )  ( ○ Select Specific )                       │  │
│  │                                                                        │  │
│  │  ┌─────────────────────────────────────────────────────────────────┐  │  │
│  │  │                   [▶️  Run Pipeline]                             │  │  │
│  │  └─────────────────────────────────────────────────────────────────┘  │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌─ Live Execution Log ──────────────────────────────────────────────────┐  │
│  │                                                                        │  │
│  │  [FULL/mysqlsh] employees.salaries -> EMPLOYEES.RAW.SALARIES          │  │
│  │     DDL ready: EMPLOYEES.RAW.SALARIES (6 cols + audit)                │  │
│  │     schema drift: no changes                                           │  │
│  │     mysqlsh full dump -> ./export/mysql/mysql_prod/salaries/full       │  │
│  │     extracted: 2,844,047 rows -> 1 file(s)                            │  │
│  │     load: COPY full: 2,844,047 rows                                   │  │
│  │     watermark: None -> 2026-08-28 14:30:00.000                        │  │
│  │     done (success) — 2,844,047 rows — 44.2s                           │  │
│  │                                                                        │  │
│  │  [INCREMENTAL/connectorx] employees.departments -> ...                 │  │
│  │     DDL ready...                                                       │  │
│  │     ▌ (running)                                                        │  │
│  │                                                                        │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  [⏹ Stop]                                                                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Screenshot:** *Execute page with mode selection and live log output during a pipeline run*
![Execute Page](docs/screenshots/execute_page.png)

**Screenshot:** *Live execution log showing step-by-step progress with row counts*
![Execute Live Log](docs/screenshots/execute_live_log.png)

### Step-by-Step: Full Pipeline Run

1. Click **"Execute"** in sidebar
2. Ensure correct **Source Connection** is selected in header
3. Select **Execution Mode:**
   - **Full Run** — runs all 7 steps (DDL → extract → load → merge → watermark)
   - **Extract Only** — extracts data but does not load to Snowflake
   - **Load Only** — loads previously extracted files
   - **Resume** — continues from last failed step
4. Choose scope:
   - **All Active Tables** — runs every table with Active = TRUE
   - **Select Specific** — pick individual tables from dropdown
5. Click **"▶️ Run Pipeline"**
6. Monitor progress in the **Live Execution Log** panel (auto-refreshes)
7. Click **"⏹ Stop"** to abort if needed

### Step-by-Step: Resume After Failure

1. A pipeline fails at step `load` for table `ORDERS`
2. Fix the root cause (e.g., stage permissions)
3. Come back to **Execute** page
4. Select **"Resume"** mode
5. Click **"▶️ Run Pipeline"**
6. System picks up from the `load` step — skips DDL, schema_drift, extract

### Pipeline Steps (executed in order)

```
┌──────┐    ┌──────────────┐    ┌─────────┐    ┌────────┐    ┌──────┐    ┌───────┐    ┌───────────┐
│ DDL  │ →  │ Schema Drift │ →  │ Extract │ →  │ Upload │ →  │ Load │ →  │ Merge │ →  │ Watermark │
└──────┘    └──────────────┘    └─────────┘    └────────┘    └──────┘    └───────┘    └───────────┘
 Create      Add new cols       Pull from       PUT/S3/      COPY INTO   SCD 0/1/2    Update
 table       from source        source DB       Azure                    logic        cursor
```

---

## Page 5: Observability — Logs, Health & Alerts

### What You See

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ OBSERVABILITY                                                                │
├─────────────────────────────────────────────────────────────────────────────┤
│  [ 📜 Run Logs ]  [ 🩺 Health Dashboard ]  [ 🔔 Alerts & Rules ]           │
├─────────────────────────────────────────────────────────────────────────────┤
```

**Screenshot:** *Observability — Run Logs tab with time window, metrics, trend chart, and batch list*
![Observability Run Logs](docs/screenshots/observability_run_logs.png)

**Screenshot:** *Observability — Health Dashboard showing stale tables and error patterns*
![Observability Health](docs/screenshots/observability_health.png)

**Screenshot:** *Observability — Alerts & Rules with active rules and create form*
![Observability Alerts](docs/screenshots/observability_alerts.png)

### Tab 1: Run Logs

```
│  Time Window:  [7d] [4d] [3d] [2d] [24h] [8h] [2h]                [🔄]   │
│                                                                              │
│  ┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐                            │
│  │   45   │  │  91.2% │  │   4    │  │  12.8s │                            │
│  │ Runs   │  │Success │  │ Failed │  │Avg Dur │                            │
│  └────────┘  └────────┘  └────────┘  └────────┘                            │
│                                                                              │
│  Table: [All ▾]  Status: [All ▾]  Load Type: [All ▾]                       │
│  Showing 45 of 45 rows                                                       │
│                                                                              │
│  ┌─ Daily Run Trend ─────────────────────────────────────────────────────┐  │
│  │  █████                                                                 │  │
│  │  █████ ██                                                              │  │
│  │  █████ ████ ███                                                        │  │
│  │  █████ ████ █████ ██                                                   │  │
│  │  ──────────────────────                                                │  │
│  │  Mon   Tue  Wed   Thu   ■ success  ■ failed                           │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ▼ ✅ Batch a15633bc — 2026-08-28 14:30 — 5 tables (5 ok, 0 err) — 92s    │
│  ▶ ❌ Batch f8a23bc1 — 2026-08-28 10:15 — 3 tables (2 ok, 1 err) — 38s    │
│    ┌────────────────────────────────────────────────────────────────────┐    │
│    │ SOURCE_TABLE   LOAD_TYPE    STATUS   DURATION  ROWS     ERROR      │    │
│    │ ORDERS         full         ✅       22s       50,000             │    │
│    │ CUSTOMERS      incremental  ✅       8s        1,200              │    │
│    │ PAYMENTS       incremental  ❌       7s        0        ORA-01830 │    │
│    │                                                                    │    │
│    │ ❌ PAYMENTS — step: extract                                        │    │
│    │ ORA-01830: date format picture ends before converting...           │    │
│    │ [🤖 Explain]                                                       │    │
│    └────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Page 1/3   [◀ Prev]  [Next ▶]                                             │
```

### Step-by-Step: Investigate a Failure

1. Click **"Observability"** → **"📜 Run Logs"** tab
2. Select time window (e.g., `24h`)
3. Filter by **Status: failed**
4. Expand the failed batch (red ❌ icon)
5. See per-table breakdown with error message
6. Click **"🤖 Explain"** for AI-powered diagnosis
7. AI responds with:
   - What went wrong (plain English)
   - Suggested fix
8. Go to Pipeline Setup or Sources to apply the fix
9. Return to Execute → Resume

### Tab 2: Health Dashboard

```
│  Alert Window:  [12h] [24h] [48h] [72h]                           [🔄]    │
│                                                                              │
│  ┌────────┐  ┌────────┐  ┌────────┐                                        │
│  │   0    │  │   11   │  │   0    │                                        │
│  │Failed  │  │ Stale  │  │Mismatch│                                        │
│  │ (24h)  │  │ Tables │  │        │                                        │
│  └────────┘  └────────┘  └────────┘                                        │
│                                                                              │
│  [❌ Failed] [⏰ Stale Tables] [⚠️ Mismatches] [🔁 Errors] [📊 Steps]     │
│                                                                              │
│  ── Stale Tables ──                                                          │
│  ⚠️ 11 active table(s) haven't run in 24h                                  │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ SOURCE_DB    TABLE          STATUS      LAST_LOADED_AT               │   │
│  │ employees    salaries       success     2026-08-27 10:30             │   │
│  │ employees    titles         success     2026-08-26 22:15             │   │
│  │ HR_DB        EMPLOYEES      NULL        never                        │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
```

### Step-by-Step: Monitor Health

1. Go to **"🩺 Health Dashboard"** tab
2. Select alert window (e.g., 24h)
3. Review metric cards:
   - **Failed** — recent pipeline failures
   - **Stale** — tables that need to be re-run
   - **Mismatches** — source/target row count drift
4. Click sub-tabs for details:
   - **Error Patterns** — recurring errors (fix the root cause, not the symptom)
   - **Step Failures** — which step fails most (helps prioritize fixes)

### Tab 3: Alerts & Rules

```
│  Configure alert rules to get notified when pipelines fail...                │
│                                                                              │
│  ── Active Rules ──                                                          │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ 🟢 Stale Orders Alert                                                │   │
│  │    Condition: TABLE_STALE · Threshold: 24 · Action: WEBHOOK_SLACK    │   │
│  │    [⏸ Disable]  [🗑️ Delete]                                         │   │
│  ├──────────────────────────────────────────────────────────────────────┤   │
│  │ 🟢 Any Failure Notify                                                │   │
│  │    Condition: RUN_FAILED · Threshold: 1 · Action: WEBHOOK_TEAMS      │   │
│  │    [⏸ Disable]  [🗑️ Delete]                                         │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  ── Create Alert Rule ──                                                     │
│  ▼ ➕ New Rule                                                               │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  Rule Name: [Stale Payment Alert___]   Condition: [TABLE_STALE ▾]    │   │
│  │  Threshold: [48___]                    Table Scope: [PAYMENTS___]     │   │
│  │                                                                       │   │
│  │  Action Type: [WEBHOOK_SLACK ▾]                                       │   │
│  │  Webhook URL: [https://hooks.slack.com/services/T00/B00/xxx___]       │   │
│  │                                                                       │   │
│  │  [💾 Save Rule]                                                       │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  ── Alert History ──                                                         │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ TRIGGERED_AT          RULE_NAME              TABLE     ACTION         │   │
│  │ 2026-08-28 06:00      Stale Orders Alert     ORDERS   sent           │   │
│  │ 2026-08-27 14:22      Any Failure Notify     PAYMENTS sent           │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
```

### Step-by-Step: Create an Alert Rule

1. Go to **"🔔 Alerts & Rules"** tab
2. Expand **"➕ New Rule"**
3. Fill in:
   - **Rule Name:** `Stale Orders Alert`
   - **Condition:** `TABLE_STALE` (table not loaded in X hours)
   - **Threshold:** `24` (hours)
   - **Table Scope:** `ORDERS` (leave blank for all tables)
   - **Action:** `WEBHOOK_SLACK`
   - **Webhook URL:** paste your Slack incoming webhook URL
4. Click **"💾 Save Rule"**
5. Rule appears in Active Rules with 🟢 status
6. When triggered, notification sent + logged in Alert History

### Alert Conditions Reference

| Condition | Threshold Meaning | Example |
|-----------|-------------------|---------|
| TABLE_STALE | Hours since last load | 24 = alert if no load in 24h |
| RUN_FAILED | Minimum failures to trigger | 1 = alert on any failure |
| ROW_DRIFT_PCT | % difference allowed | 5 = alert if >5% row count drift |
| CONSECUTIVE_FAILURES | N failures in a row | 3 = alert after 3 consecutive fails |

---

## Page 6: File Loader — Cloud File Ingestion

### What You See

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ FILE INGESTION                                                               │
├─────────────────────────────────────────────────────────────────────────────┤
│ [Active Jobs] [Run Jobs] [Add/Edit] [Upload & Ingest] [Stages] [History]    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Active: 2     Last OK: 1     Last Failed: 0                                │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ ✅ SUCCESS                                                            │   │
│  │ 🟢 orders_s3_file                                                    │   │
│  │ CSV · APPEND → ORDERS_S3                                             │   │
│  │ Stage: HISTLOAD_DB.META.DMT_EXT_S3/dated/ · Pattern: .*\.csv         │   │
│  │ Last run: 5,000 rows at 2026-08-28 14:30                            │   │
│  │ [⏸ Pause]  [🗑️ Delete]                                              │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  ── Stage File Browser ──                                                    │
│  Select job: [orders_s3_file ▾]                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ name                                              size    modified    │   │
│  │ dated/20260828/orders_20260828.csv                4.2KB   14:28       │   │
│  │ dated/20260827/orders_20260827.csv                3.8KB   yesterday   │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
```

**Screenshot:** *File Loader — Active Jobs tab with job cards and stage file browser*
![File Loader Active Jobs](docs/screenshots/file_loader_active_jobs.png)

**Screenshot:** *File Loader — Run Jobs tab with Force Reload checkbox and result display*
![File Loader Run Jobs](docs/screenshots/file_loader_run_jobs.png)

**Screenshot:** *File Loader — Add/Edit Job form with CSV options and target configuration*
![File Loader Add Job](docs/screenshots/file_loader_add_job.png)

### Step-by-Step: Create and Run a File Ingestion Job

1. Go to **"File Loader"** → **"Add / Edit Job"** tab
2. Fill in:
   - **Job Name:** `orders_s3_file`
   - **Stage Name:** select from dropdown or type fully-qualified name
   - **Cloud Path:** `dated/` (subfolder)
   - **File Pattern:** `.*\.csv` (regex, not glob)
   - **File Type:** CSV
   - **Target DB / Schema / Table:** `HISTLOAD_DB` / `RAW` / `ORDERS_S3`
   - **Load Mode:** APPEND
3. Configure CSV options: delimiter, enclosed-by, skip header
4. Click **"💾 Save Job"**
5. Go to **"Run Jobs"** tab
6. Select your job, check **"Force Reload"** if retrying
7. Click **"Run"**
8. Result shows below: ✅ rows loaded or ❌ error message

---

## Page 7: Auto-Ingest — Snowpipe Wizard

### What You See

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ AUTO-INGEST (SNOWPIPE)                                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│ [Storage Integration] [Create Stage] [Generate Pipe] [Monitor]              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ── Generate Snowpipe DDL ──                                                 │
│                                                                              │
│  Stage: [HISTLOAD_DB.META.DMT_EXT_S3 ▾]                                    │
│  Target Table: [HISTLOAD_DB.RAW.ORDERS_S3 ▾]                               │
│  File Format: [CSV ▾]     Auto-Ingest: [✓]                                 │
│  Pattern: [.*\.csv____________]                                              │
│                                                                              │
│  [⚡ Generate DDL]                                                           │
│                                                                              │
│  ┌─ Generated SQL ──────────────────────────────────────────────────────┐   │
│  │ CREATE PIPE HISTLOAD_DB.META.ORDERS_PIPE                              │   │
│  │   AUTO_INGEST = TRUE                                                  │   │
│  │   AS                                                                  │   │
│  │   COPY INTO HISTLOAD_DB.RAW.ORDERS_S3                                │   │
│  │   FROM @HISTLOAD_DB.META.DMT_EXT_S3                                  │   │
│  │   FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)                       │   │
│  │   PATTERN = '.*\.csv';                                                │   │
│  │                                                                       │   │
│  │ -- Configure this notification ARN in your S3 bucket:                 │   │
│  │ -- arn:aws:sqs:us-east-1:123456789:sf-snowpipe-...                   │   │
│  └───────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  [📋 Copy to Clipboard]  [▶️ Execute in Snowflake]                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Screenshot:** *Auto-Ingest wizard showing generated CREATE PIPE DDL with notification ARN*
![Auto-Ingest Page](docs/screenshots/auto_ingest_page.png)

### Step-by-Step: Set Up Auto-Ingest

1. Go to **"Auto-Ingest"** → **"Generate Pipe"** tab
2. Select **Stage** (must be an external stage with storage integration)
3. Select **Target Table** (must already exist)
4. Choose **File Format** and enter **Pattern**
5. Enable **Auto-Ingest** checkbox
6. Click **"⚡ Generate DDL"**
7. Review the generated `CREATE PIPE` SQL
8. Copy the **notification ARN** and configure in your cloud provider:
   - **AWS:** Add SQS event notification to your S3 bucket
   - **Azure:** Configure Event Grid on your blob container
9. Click **"▶️ Execute"** to create the pipe in Snowflake
10. Monitor via the **"Monitor"** tab (shows COPY_HISTORY)

---

## Quick Reference: Source-Specific Workflows

### MySQL Workflow
```
Sources: Create mysql connection (host:3306)
    ↓
Pipeline Setup: Generate Config → auto-detects tables in schema
    ↓
Schema Mapping: MySQL types → Snowflake types (INT→NUMBER, etc.)
    ↓
Execute: Full Run → mysqlsh extract → PUT → COPY INTO → MERGE
```

### MSSQL Workflow
```
Sources: Create mssql connection (host:1433 + database name)
    ↓
Pipeline Setup: Generate Config → discovers database.schema.table (3-part)
    ↓
Schema Mapping: MSSQL types → Snowflake (NVARCHAR→VARCHAR, MONEY→NUMBER, etc.)
    ↓
Execute: Full Run → BCP extract → gzip → PUT → COPY INTO → MERGE
```

### Oracle Workflow
```
Sources: Create oracle connection (host:1521 + service name)
    ↓
Pipeline Setup: Generate Config → discovers schema.table
    ↓
Schema Mapping: Oracle types → Snowflake (NUMBER(p,s), DATE→TIMESTAMP_NTZ, etc.)
    ↓
Execute: Full Run → oracledb parallel/streaming → Parquet → PUT → COPY INTO → MERGE
```

### Teradata Workflow
```
Sources: Create teradata connection (host:1025 + auth method)
    ↓
Pipeline Setup: Generate Config → discovers database.table (incl. NoPI tables)
    ↓
Schema Mapping: Teradata types → Snowflake (DECIMAL, PERIOD→VARCHAR, etc.)
    ↓
Execute: Full Run → TPT script → CSV → PUT → COPY INTO → MERGE
```

---

## Keyboard Shortcuts & Tips

| Tip | Description |
|-----|-------------|
| **Force Reload** | Check this in Run Jobs tab to reload previously-attempted files |
| **Pattern auto-fix** | Enter `*.csv` and the app converts it to `.*\.csv` automatically |
| **AI toggle** | Enable for smart recommendations; disable for faster page loads |
| **Resume** | Always try Resume before re-running failed tables from scratch |
| **Date partition** | Enable for daily file drops — only processes today's folder |
| **SCD2** | Requires PRIMARY_KEY set — composite keys use comma separation |

---

## Support

- **App Version:** MigrateX v1.0
- **Documentation:** This guide (userguide.md)
- **Architecture:** ARCHITECTURE.md
- **Setup:** Run `setup.sql` in Snowflake to bootstrap metadata tables

---

*Powered by Tiger Analytics · Developed by MDP*
