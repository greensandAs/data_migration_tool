# DMT v1 — Deployment & Installation Guide

## Prerequisites

- Python 3.10+
- Network access to source databases (MySQL/Teradata) and Snowflake
- Snowflake account with ACCOUNTADMIN or equivalent role

---

## 1. Python Dependencies

```bash
pip install -r requirements.txt
```

This installs: `snowflake-connector-python`, `streamlit`, `mysql-connector-python`, `connectorx`, `teradatasql`, `pyarrow`, `pandas`, `boto3`, `openai`, `python-dotenv`

---

## 2. MySQL Shell (required for MySQL full loads)

### Ubuntu/Debian
```bash
wget https://dev.mysql.com/get/mysql-apt-config_0.8.29-1_all.deb
sudo dpkg -i mysql-apt-config_0.8.29-1_all.deb
sudo apt update
sudo apt install mysql-shell
mysqlsh --version
```

### macOS
```bash
brew install mysql-shell
mysqlsh --version
```

### Windows
- Download MSI from https://dev.mysql.com/downloads/shell/
- Run installer, ensure it's added to PATH
- Verify: `mysqlsh --version`

---

## 3. TPT / tbuild (required for Teradata full loads)

TPT is part of Teradata Tools & Utilities (TTU). Not available via pip.

### Step 1: Download
- Go to https://downloads.teradata.com (free account required)
- Download "Teradata Tools and Utilities" for your OS

### Step 2: Install (Linux)
```bash
tar -xzf TeradataToolsAndUtilities*.tar.gz
cd TeradataToolsAndUtilities/Linux/
sudo dpkg -i tdicu*.deb       # ICU libraries (dependency)
sudo dpkg -i tptbase*.deb     # TPT base (includes tbuild)
tbuild -h
```

### Step 2: Install (macOS)
```bash
tar -xzf TeradataToolsAndUtilities*.tar.gz
cd TeradataToolsAndUtilities/MacOS/
sudo installer -pkg tdicu*.pkg -target /
sudo installer -pkg tptbase*.pkg -target /
export PATH=$PATH:/opt/teradata/client/bin
tbuild -h
```

### Step 2: Install (Windows)
- Run the .exe installer from the downloaded package
- Select "TPT" component during installation
- Default path: `C:\Program Files\Teradata\Client\bin\`
- Verify: `tbuild -h`

### Without TPT (fallback)
If TPT cannot be installed, the DMT app still works for Teradata using the `teradatasql` Python connector for extraction. This is slower for large tables (>10M rows) but requires no extra binaries.
- Set `LOAD_TYPE = incremental` (extracts via SQL SELECT instead of TPT)
- Or use full load mode — the orchestrator will fall back to SQL if `tbuild` is not found

---

## 4. Snowflake Setup

Run `setup.sql` in your Snowflake account:
```sql
-- Execute in Snowflake (ACCOUNTADMIN role)
-- This creates all required tables, stages, and file formats
@setup.sql
```

Or copy the contents of `DMT_v1/setup.sql` into a Snowflake worksheet and execute.

---

## 5. Environment Variables (optional)

Create a `.env` file in the `DMT_v1/` directory:

```env
# Snowflake connection (if not using Snowsight session)
SF_ACCOUNT=eo49814
SF_USER=ASLAM
SF_PASSWORD=<your-snowflake-password>
SF_WAREHOUSE=COMPUTE_WH
SF_DATABASE=HISTLOAD_DB
SF_SCHEMA=META

# MySQL (override connection profile)
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=<password>

# Teradata (override connection profile)
TD_HOST=dmt-test-rkb1f0y9r9qezspy.env.trial.teradata.com
TD_USER=demo_user
TD_PASSWORD=<password>
TD_LOGMECH=TD2

# S3 (if using external stage storage)
AWS_ACCESS_KEY_ID=<key>
AWS_SECRET_ACCESS_KEY=<secret>
AWS_DEFAULT_REGION=eu-north-1

# LLM AI Gateway (optional)
LLM_API_BASE=https://api.ai-gateway.tigeranalytics.com
LLM_API_KEY=sk-<your-key>
LLM_MODEL=gemini-3-flash-preview
```

Note: Environment variables override DMT_SETTINGS table values.

---

## 6. Running the App

### Streamlit UI (recommended)
```bash
cd DMT_v1
streamlit run app.py --server.port=8501
```
Open http://localhost:8501

### CLI (headless)
```bash
cd DMT_v1

# Run all active tables
python orchestrator.py

# Run single table
python orchestrator.py --table EMPLOYEES

# Force full reload
python orchestrator.py --full

# Resume from failure
python orchestrator.py --resume

# Extract only (no load)
python orchestrator.py --mode EXTRACT_ONLY

# Load only (from prior extract)
python orchestrator.py --mode LOAD_ONLY
```

---

## 7. Docker Deployment

```dockerfile
FROM python:3.11-slim

# System dependencies
RUN apt-get update && apt-get install -y wget gnupg && \
    wget https://dev.mysql.com/get/mysql-apt-config_0.8.29-1_all.deb && \
    dpkg -i mysql-apt-config_0.8.29-1_all.deb && \
    apt-get update && apt-get install -y mysql-shell && \
    rm -rf /var/lib/apt/lists/*

# TPT (copy from local Teradata download)
# COPY tdicu*.deb tptbase*.deb /tmp/
# RUN dpkg -i /tmp/tdicu*.deb /tmp/tptbase*.deb && rm /tmp/*.deb

# Python dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r /app/requirements.txt

# Application
COPY DMT_v1/ /app/
WORKDIR /app

EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

Build and run:
```bash
docker build -t dmt-v1 .
docker run -p 8501:8501 --env-file .env dmt-v1
```

---

## 8. Verification Checklist

```bash
# Python packages
python -c "import teradatasql; print('teradatasql OK')"
python -c "import mysql.connector; print('mysql-connector OK')"
python -c "import snowflake.connector; print('snowflake-connector OK')"
python -c "import pyarrow; print('pyarrow OK')"
python -c "import streamlit; print('streamlit OK')"
python -c "import boto3; print('boto3 OK')"
python -c "import openai; print('openai OK')"

# External tools
mysqlsh --version       # MySQL Shell for full extraction
tbuild -h               # TPT for Teradata full extraction (optional)

# Connectivity
python -c "
import teradatasql
conn = teradatasql.connect(host='dmt-test-rkb1f0y9r9qezspy.env.trial.teradata.com',
                           user='demo_user', password='<pwd>')
cur = conn.cursor()
cur.execute('SELECT 1')
print('Teradata: OK')
conn.close()
"

python -c "
import snowflake.connector
conn = snowflake.connector.connect(account='eo49814', user='ASLAM',
                                    password='<pwd>', warehouse='COMPUTE_WH')
cur = conn.cursor()
cur.execute('SELECT CURRENT_VERSION()')
print('Snowflake:', cur.fetchone()[0])
conn.close()
"
```

---

## 9. Troubleshooting

| Issue | Fix |
|-------|-----|
| `mysqlsh: command not found` | Add MySQL Shell to PATH or reinstall |
| `tbuild: command not found` | Install TTU or add `/opt/teradata/client/bin` to PATH |
| `teradatasql` connection timeout | Check firewall allows outbound port 1025 |
| `snowflake.connector` auth error | Verify account URL, user, password |
| `No module named 'connectorx'` | `pip install connectorx` (requires Rust — use binary wheel) |
| Streamlit port conflict | Use `--server.port=8502` or another free port |
| Docker DNS issues | Use `--network=host` or set DNS in Docker config |
