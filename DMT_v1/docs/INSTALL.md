# MigrateX — Deployment & Installation Guide

**Tiger Analytics · Developed by MDP**

---

## Prerequisites

- Python 3.10+
- Network access to source databases (MySQL, Teradata, MSSQL, Oracle) and Snowflake
- Snowflake account with ACCOUNTADMIN or equivalent role
- (Optional) AWS CLI / Azure CLI for cloud storage integration

---

## 1. Python Dependencies

```bash
pip install -r requirements.txt
```

### Core Dependencies

| Package | Purpose |
|---------|---------|
| `streamlit` | Web UI framework |
| `snowflake-connector-python` | Snowflake connectivity |
| `pandas` | Data manipulation |
| `pyarrow` | Arrow/Parquet handling |
| `openai` | AI Gateway integration (OpenAI-compatible SDK) |
| `python-dotenv` | Environment variable loading |

### Source-Specific Dependencies

| Package | Source | Purpose |
|---------|--------|---------|
| `mysql-connector-python` | MySQL | Connection + metadata queries |
| `connectorx` | MySQL | Fast incremental extraction → Arrow |
| `teradatasql` | Teradata | Connection + incremental extraction |
| `pyodbc` | MSSQL | Connection + metadata queries |
| `oracledb` | Oracle | Connection + extraction (streaming + parallel) |
| `boto3` | S3 | AWS S3 file upload/download |
| `azure-storage-blob` | Azure | Azure Blob Storage |

### External Tools (must be on PATH)

| Tool | Source | Purpose |
|------|--------|---------|
| `mysqlsh` | MySQL | Full-load parallel dump (util.dumpTables) |
| `bcp` | MSSQL | Full + incremental bulk export |
| `tbuild` | Teradata | TPT full-load export |

---

## 2. Environment Variables

Create a `.env` file in the `DMT_v1/` directory:

```bash
# Snowflake connection
SF_ACCOUNT=your_account.region
SF_USER=your_user
SF_PASSWORD=your_password
SF_ROLE=ACCOUNTADMIN
SF_WAREHOUSE=COMPUTE_WH

# AI Gateway (optional — enables AI features)
LLM_API_BASE=https://api.ai-gateway.tigeranalytics.com
LLM_API_KEY=sk-your-key-here

# AWS (for S3 storage backend)
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-east-1

# Azure (for Azure Blob storage backend)
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;...
```

---

## 3. Snowflake Setup

### Bootstrap the metadata database

Run `setup.sql` in a Snowflake worksheet (as ACCOUNTADMIN):

```sql
-- Execute the entire setup.sql file
-- This creates:
--   HISTLOAD_DB database
--   META schema
--   All configuration tables
--   File formats
--   Internal stages
--   Schema migrations (ALTER TABLE ADD COLUMN)
--   Alert tables
```

### File Ingestion tables (if using File Loader)

```sql
-- Execute setup_file_ingestion.sql for file ingestion tables
-- Or rely on the migrations in setup.sql (preferred)
```

### Verify setup

```sql
USE DATABASE HISTLOAD_DB;
USE SCHEMA META;
SHOW TABLES;
-- Should show: MIGRATION_CONFIG, CONNECTION_PROFILES, RUN_LOG,
-- PIPELINE_STEP_LOG, FILE_MANIFEST, DMT_SETTINGS,
-- FILE_INGESTION_CONFIG, FILE_INGESTION_LOG,
-- DMT_ALERT_RULES, DMT_ALERT_LOG
```

---

## 4. Initial Configuration

### Set allowed sources

```sql
INSERT INTO HISTLOAD_DB.META.DMT_SETTINGS (SETTING_KEY, SETTING_VALUE)
VALUES ('ALLOWED_SOURCES', 'mysql,teradata,mssql,oracle');
```

### Configure AI (optional)

```sql
INSERT INTO HISTLOAD_DB.META.DMT_SETTINGS (SETTING_KEY, SETTING_VALUE)
VALUES
  ('LLM_API_BASE', 'https://api.ai-gateway.tigeranalytics.com'),
  ('LLM_API_KEY', 'sk-your-key'),
  ('LLM_MODEL_CONFIG', 'gemini-2.0-flash'),
  ('LLM_MODEL_DDL', 'gemini-2.0-flash'),
  ('LLM_MODEL_HISTORY', 'gemini-2.0-flash'),
  ('LLM_MODELS_AVAILABLE', 'gemini-2.0-flash,gemini-1.5-pro,gpt-4o-mini');
```

---

## 5. Running the App

### Local development

```bash
cd DMT_v1
streamlit run app.py
```

App opens at `http://localhost:8501`

### Snowflake Snowsight (Workspace)

1. Upload the `DMT_v1/` folder to a Snowflake Workspace
2. The app runs natively in Snowsight — no local setup needed
3. Authentication is handled by the Snowflake session
4. `.env` variables should be stored in `DMT_SETTINGS` table instead

### Docker (production)

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY DMT_v1/ .
EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.headless=true"]
```

---

## 6. Storage Integration Setup

### S3 (AWS)

```sql
-- In Snowflake:
CREATE STORAGE INTEGRATION DMT_S3_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789:role/snowflake-access'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('s3://your-bucket/dmt/');

-- Create external stage
CREATE STAGE HISTLOAD_DB.META.DMT_EXT_S3
  STORAGE_INTEGRATION = DMT_S3_INT
  URL = 's3://your-bucket/dmt/';
```

### Azure Blob

```sql
CREATE STORAGE INTEGRATION DMT_AZURE_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  AZURE_TENANT_ID = 'your-tenant-id'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('azure://account.blob.core.windows.net/container/dmt/');

CREATE STAGE HISTLOAD_DB.META.DMT_EXT_AZURE
  STORAGE_INTEGRATION = DMT_AZURE_INT
  URL = 'azure://account.blob.core.windows.net/container/dmt/';
```

---

## 7. External Tool Installation

### mysqlsh (MySQL Shell)

```bash
# Ubuntu/Debian
apt-get install mysql-shell

# macOS
brew install mysql-shell

# Verify
mysqlsh --version
```

### BCP (SQL Server Bulk Copy)

```bash
# Ubuntu/Debian
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
apt-get install mssql-tools18

# macOS
brew tap microsoft/mssql-release
brew install mssql-tools18

# Verify
bcp -v
```

### TPT (Teradata Parallel Transporter)

```bash
# Download from Teradata Downloads portal
# Install Teradata Tools and Utilities (TTU)
# Verify
tbuild -h
```

---

## 8. Network Requirements

| Source | Default Port | Protocol |
|--------|-------------|----------|
| MySQL | 3306 | TCP |
| Teradata | 1025 | TCP |
| MSSQL | 1433 | TCP |
| Oracle | 1521 | TCP |
| Snowflake | 443 | HTTPS |
| AI Gateway | 443 | HTTPS |
| AWS S3 | 443 | HTTPS |
| Azure Blob | 443 | HTTPS |

Ensure firewall rules allow outbound connections from the app server to all source databases and Snowflake.

---

## 9. Upgrading

### Schema migrations

When updating the app, run the **Schema Migrations** section at the bottom of `setup.sql`:

```sql
-- These are idempotent (ADD COLUMN IF NOT EXISTS)
-- Safe to re-run on every deployment
ALTER TABLE IF EXISTS FILE_INGESTION_CONFIG ADD COLUMN IF NOT EXISTS LAST_ERROR VARCHAR;
-- ... (all ALTER statements)
```

### App update

```bash
git pull origin main
pip install -r requirements.txt  # in case new deps
streamlit run app.py
```

---

## 10. Troubleshooting Installation

| Issue | Fix |
|-------|-----|
| `ModuleNotFoundError: connectorx` | `pip install connectorx` — requires Rust toolchain on some platforms |
| `mysqlsh: command not found` | Install MySQL Shell, add to PATH |
| `bcp: command not found` | Install mssql-tools18, add `/opt/mssql-tools18/bin` to PATH |
| Snowflake SSL error | Ensure `pyOpenSSL` and `cryptography` are up-to-date |
| `.env` not loading | Ensure `.env` is in the same directory as `app.py` |
| Dark theme not applying | Check `.streamlit/config.toml` exists with `base = "dark"` |
| AI features not working | Verify `LLM_API_KEY` starts with `sk-` in DMT_SETTINGS |

---

*MigrateX v1.0 — Tiger Analytics · Developed by MDP*
