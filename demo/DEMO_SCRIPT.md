# MigrateX — Demo Script

**Total Duration: ~45 minutes**
**Dataset: E-Commerce Platform (3.5M rows per source × 4 sources = 14M rows)**

---

## Pre-Demo Checklist

- [ ] 4 source databases running with demo data loaded
- [ ] Snowflake account accessible (ACCOUNTADMIN role)
- [ ] `setup.sql` executed in Snowflake (HISTLOAD_DB.META tables exist)
- [ ] AI Gateway key configured in DMT_SETTINGS
- [ ] S3 bucket with `demo/s3_test_files/orders_20260828.csv` uploaded
- [ ] Slack webhook URL ready (for Alerts demo)

### Load Demo Data into Source Databases

```bash
# MySQL
mysql -u root -p < demo/mysql_setup.sql

# MSSQL
sqlcmd -S localhost -U sa -P 'YourPassword' -i demo/mssql_setup.sql

# Oracle
sqlplus sys/password@//localhost:1521/ORCL as sysdba @demo/oracle_setup.sql

# Teradata (via BTEQ)
bteq < demo/teradata_setup.sql
```

---

## Act 1: First Impressions (3 min)

### Show the App

1. Open MigrateX in browser
2. Point out:
   - **Header**: MigrateX branding, Snowflake account/warehouse/role context
   - **Sidebar**: Two navigation groups (Database Migration + File Ingestion)
   - **Overview page**: Empty state (no pipelines configured yet)

> **Talking point:** "MigrateX is a single platform for migrating data from any source to Snowflake — MySQL, Teradata, MSSQL, Oracle, and cloud files."

---

## Act 2: Connect All Sources (5 min)

### Navigate to: Sources

1. **Create MySQL connection:**
   - Profile Name: `mysql_ecom`
   - Source Type: `mysql`
   - Host/Port/Credentials
   - Click **Test Connection** → show green ✅
   - Save

2. **Create MSSQL connection:**
   - Profile Name: `mssql_ecom`
   - Source Type: `mssql`
   - Host/Port/Database: `MSSQL_DEMO_2908`
   - Test → Save

3. **Create Oracle connection:**
   - Profile Name: `oracle_ecom`
   - Source Type: `oracle`
   - Host/Port/Service Name
   - Test → Save

4. **Create Teradata connection:**
   - Profile Name: `td_ecom`
   - Source Type: `teradata`
   - Host/Auth Method
   - Test → Save

> **Talking point:** "Four connections, four different databases, managed from one screen."

---

## Act 3: MySQL Migration — Full Feature Demo (10 min)

### 3a. Pipeline Setup

1. Select **Source Type: MYSQL** → **Connection: mysql_ecom** in header
2. Navigate to **Pipeline Setup**
3. Select Schema: `MYSQL_DEMO_2908`
4. Click **"Generate Config"**
   - Show auto-discovery: 4 tables found
   - PK detected, watermark columns identified
   - Load types suggested (full vs incremental)
5. Click **"Add All"**

### 3b. AI Recommend

1. Enable **🤖 AI toggle** in header
2. Click **"+ Add Table"** → enter `MYSQL_DEMO_2908` / `CUSTOMERS`
3. Click **"🤖 AI Recommend Settings"**
   - AI suggests: SCD2, watermark=UPDATED_AT, merge_key=CUSTOMER_ID
4. Accept and save

> **Talking point:** "AI analyzes column patterns and recommends the optimal migration strategy."

### 3c. Schema Mapping

1. Navigate to **Schema Mapping**
2. Select `CUSTOMERS` table
3. Show side-by-side DDL:
   - Left: MySQL CREATE TABLE
   - Right: Snowflake CREATE TABLE (with type mapping)
4. Show column mapping table (INT→NUMBER, VARCHAR→VARCHAR, DATETIME→TIMESTAMP_NTZ)
5. Click **"🤖 Validate Mapping"** → AI confirms no data loss risk

### 3d. Execute — Full Load (1M+ rows)

1. Navigate to **Execute**
2. Select **Full Run** mode
3. Click **"▶️ Run Pipeline"**
4. Watch the live log:
   ```
   [FULL/mysqlsh] MYSQL_DEMO_2908.ORDERS → MYSQL_DEMO_2908.RAW.ORDERS
      DDL ready (7 cols + audit)
      mysqlsh full dump → 1,000,000 rows
      COPY full: 1,000,000 rows
      done (success) — 44.2s
   ```
5. Wait for all 4 tables (~3.5M rows total)
6. Show **Overview** → 4 green status cards

> **Talking point:** "1 million orders migrated in under a minute. MigrateX handles the full pipeline — DDL, extraction, staging, loading, and merge."

### 3e. Incremental Load

1. **Run incremental inserts in MySQL:**
   ```sql
   -- Run the MySQL section from demo/incremental_inserts.sql
   USE MYSQL_DEMO_2908;
   -- (5,000 new orders + 500 updated customers)
   ```
2. Go back to **Execute** → **Full Run** again
3. Show the log: only NEW rows are picked up (watermark-based)
   ```
   [INCREMENTAL/connectorx] MYSQL_DEMO_2908.ORDERS
      rows fetched: 5,000 (watermark: 2026-08-28 14:30 → 2026-08-28 18:00)
      SCD1 MERGE: 5,000 rows
   ```
4. Show CUSTOMERS → SCD2 with `_VALID_FROM`, `_VALID_TO`, `_IS_CURRENT`

> **Talking point:** "Incremental loads only process new data. SCD2 preserves full customer history automatically."

---

## Act 4: MSSQL Migration (5 min)

1. Switch header to **MSSQL** → **mssql_ecom**
2. **Pipeline Setup** → Generate Config from `MSSQL_DEMO_2908`
   - Show 3-part naming: `MSSQL_DEMO_2908.dbo.ORDERS`
3. Add all tables
4. **Execute** → Full Run
5. Show BCP extraction in logs:
   ```
   [FULL/bcp] MSSQL_DEMO_2908.dbo.ORDERS → MSSQL_DEMO_2908.RAW.ORDERS
      BCP export: 1,000,000 rows → 4 chunks
   ```

> **Talking point:** "BCP provides high-throughput extraction from SQL Server with pipe-delimited, compressed output."

---

## Act 5: Oracle Migration (5 min)

1. Switch to **Oracle** → **oracle_ecom**
2. **Pipeline Setup** → Generate Config from `ORACLE_DEMO_2908`
   - Show schema.table discovery
3. Add all tables
4. **Execute** → Full Run
5. Show parallel extraction for large ORDERS table:
   ```
   [FULL/oracledb] ORACLE_DEMO_2908.ORDERS → ORACLE_DEMO_2908.RAW.ORDERS
      parallel range scan (4 threads, 1,000,000 rows)
      extracted → Parquet
   ```

> **Talking point:** "For tables over 500MB, MigrateX automatically switches to parallel extraction with range partitioning."

---

## Act 6: Teradata Migration (5 min)

1. Switch to **Teradata** → **td_ecom**
2. **Pipeline Setup** → Generate Config from `TD_DEMO_2908`
3. Add all tables
4. **Execute** → Full Run
5. Show TPT extraction:
   ```
   [FULL/tpt] TD_DEMO_2908.ORDERS → TD_DEMO_2908.RAW.ORDERS
      TPT script generated
      all columns double-quoted (reserved word safe)
      500,000 rows exported
   ```

> **Talking point:** "TPT handles Teradata's unique quirks — NoPI tables, reserved words, and PERIOD types."

---

## Act 7: Observability (5 min)

1. Navigate to **Observability**

### Run Logs
2. Show **📜 Run Logs** tab:
   - 4 sources × 2 runs = 8 batches visible
   - Trend chart showing today's activity
   - Filter by source, status, load type

### Health Dashboard
3. Switch to **🩺 Health Dashboard**:
   - Show stale tables (if any haven't run)
   - Error patterns (if any failures)

### Alerts & Rules
4. Switch to **🔔 Alerts & Rules**:
   - Create rule: "Stale Orders Alert"
   - Condition: TABLE_STALE, Threshold: 24 hours
   - Action: WEBHOOK_SLACK
   - Paste Slack webhook URL
   - Save → show green active rule

### AI Failure Explainer
5. (Optional) Deliberately break something:
   - Change a connection password to wrong value
   - Run → fails at extract step
   - Go to Run Logs → expand failed batch
   - Click **"🤖 Explain"** → AI diagnoses the issue
   - Fix password → **Resume** → completes from where it left off

> **Talking point:** "One page to answer: Is my pipeline healthy? What failed? And will I get notified before it's too late?"

---

## Act 8: File Ingestion (5 min)

1. Navigate to **File Loader**

### Upload & Ingest
2. Go to **Upload & Ingest** tab
3. Drag-and-drop `orders_20260828.csv`
4. Show file preview (first 10 rows)
5. Set target: `DEMO_FILES.RAW.ORDERS_UPLOAD`
6. Click **"Ingest"** → 5,000 rows loaded

### Stage-Based Job
7. Go to **Add / Edit Job** tab
8. Create job:
   - Job Name: `s3_daily_orders`
   - Stage: (your external S3 stage)
   - Pattern: `.*\.csv`
   - Target: `DEMO_FILES.RAW.ORDERS_S3`
   - Date partition: ON
9. Save → Go to **Run Jobs** → Run
10. Show **Force Reload** toggle explanation

> **Talking point:** "For files already in cloud storage, MigrateX handles the full flow — stage scanning, schema inference, and COPY INTO."

---

## Act 9: Schema Drift (2 min)

1. Add a column to MySQL source:
   ```sql
   ALTER TABLE MYSQL_DEMO_2908.ORDERS ADD COLUMN PRIORITY VARCHAR(10) DEFAULT 'normal';
   ```
2. Re-run pipeline for MySQL
3. Show in logs:
   ```
   schema drift: +1 new column (PRIORITY VARCHAR)
   ALTER TABLE MYSQL_DEMO_2908.RAW.ORDERS ADD COLUMN "PRIORITY" VARCHAR(10)
   ```

> **Talking point:** "Schema changes in the source are detected and applied automatically — no manual DDL maintenance."

---

## Act 10: User Guide (1 min)

1. Click **"📖 User Guide"** in sidebar
2. Show in-app documentation with screenshots
3. Scroll through step-by-step walkthroughs

> **Talking point:** "Full documentation embedded in the app. No external wiki to maintain."

---

## Closing Summary

### What We Demonstrated

| Feature | Shown |
|---------|-------|
| 4 source databases (MySQL, MSSQL, Oracle, Teradata) | ✅ |
| Full load (1M+ rows per source) | ✅ |
| Incremental load (watermark-based) | ✅ |
| SCD Type 0, 1, 2 | ✅ |
| AI recommendations | ✅ |
| AI failure explanation | ✅ |
| Schema mapping + validation | ✅ |
| Schema drift detection | ✅ |
| Resume from failure | ✅ |
| File ingestion (CSV, Parquet) | ✅ |
| Observability (logs, health, alerts) | ✅ |
| Webhook alerting (Slack) | ✅ |
| In-app user guide | ✅ |

### Key Metrics

| Metric | Value |
|--------|-------|
| Total rows migrated | ~14M (3.5M × 4 sources) |
| Source types supported | 4 (MySQL, Teradata, MSSQL, Oracle) |
| File types supported | 4 (CSV, Parquet, JSON, Avro) |
| SCD strategies | 3 (Type 0, 1, 2) |
| Pipeline steps | 7 (DDL → Watermark) |
| Alert conditions | 4 types with webhook delivery |

---

*MigrateX v1.0 — Powered by Tiger Analytics · Developed by MDP*
