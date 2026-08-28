# MigrateX

**Accelerate Your Data Journey to Snowflake**

Tiger Analytics · Developed by MDP

---

## What is MigrateX?

MigrateX is an enterprise data migration platform that moves data from source databases into Snowflake with resumable pipelines, automatic schema mapping, and AI-powered recommendations.

### Supported Sources

| Source | Full Load | Incremental | Engine |
|--------|-----------|-------------|--------|
| MySQL | ✅ | ✅ | mysqlsh / connectorx |
| Teradata | ✅ | ✅ | TPT / teradatasql |
| MSSQL | ✅ | ✅ | BCP / BCP queryout |
| Oracle | ✅ | ✅ | oracledb (parallel + streaming) |

### Key Features

- **Resumable Pipelines** — Step-level tracking with automatic retry from failure
- **SCD Strategies** — Type 0 (Append), Type 1 (Upsert), Type 2 (History)
- **File Ingestion** — Load CSV, Parquet, JSON, Avro from cloud stages
- **Auto-Ingest** — Snowpipe DDL generation for continuous loading
- **AI-Powered** — Recommendations, failure explanations, schema validation
- **Observability** — Run logs, health dashboard, configurable alerts with webhooks
- **Multi-Cloud Storage** — Internal stage, S3, Azure Blob

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
# Edit .env with your Snowflake credentials

# 3. Bootstrap Snowflake metadata
# Run setup.sql in a Snowflake worksheet

# 4. Launch the app
streamlit run app.py
```

---

## Project Structure

```
DMT_v1/
├── app.py                  Streamlit app entry point
├── setup.sql               Snowflake DDL bootstrap
├── requirements.txt        Python dependencies
├── .streamlit/config.toml  Dark theme configuration
│
├── core/                   Pipeline engine
├── views/                  Streamlit UI pages
├── metadata/               Snowflake state management
├── extractors/             Source data extraction engines
├── ddl_generators/         Schema mapping & DDL generation
├── storage/                Pluggable file storage backends
├── utils/                  Cross-cutting utilities
├── assets/                 Brand assets (logos)
├── docs/                   Documentation
│   ├── userguide.md        User guide (also accessible in-app)
│   ├── ARCHITECTURE.md     Technical architecture
│   ├── INSTALL.md          Deployment & installation
│   ├── CHANGELOG.md        Version history
│   └── screenshots/        UI screenshots
└── test_files/             Sample data for testing
```

---

## Documentation

| Document | Description |
|----------|-------------|
| [User Guide](docs/userguide.md) | End-user documentation with step-by-step walkthroughs |
| [Architecture](docs/ARCHITECTURE.md) | Technical design, data flows, module dependencies |
| [Installation](docs/INSTALL.md) | Deployment guide, prerequisites, Docker setup |
| [Changelog](docs/CHANGELOG.md) | Version history and release notes |

---

## App Pages

| Page | Purpose |
|------|---------|
| **Overview** | Pipeline health summary — metrics + status cards |
| **Pipeline Setup** | Configure tables for migration |
| **Schema Mapping** | Source-to-Snowflake DDL conversion |
| **Execute** | Run pipelines (Full / Extract / Load / Resume) |
| **Observability** | Run logs, health checks, alerts & webhooks |
| **Sources** | Manage database connections |
| **File Loader** | Cloud file → Snowflake ingestion |
| **Auto-Ingest** | Snowpipe DDL wizard |

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| UI | Streamlit (dark theme, Tiger Analytics branded) |
| Backend | Python 3.10+ |
| Target | Snowflake |
| AI | Tiger Analytics AI Gateway (OpenAI-compatible) |
| Storage | S3 (boto3), Azure Blob, Snowflake Internal Stage |
| Extraction | mysqlsh, TPT, BCP, oracledb, connectorx |

---

## License

Proprietary — Tiger Analytics. Internal use only.

---

*MigrateX v1.0 — Powered by Tiger Analytics · Developed by Aslam*
