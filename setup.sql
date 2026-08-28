-- Snowflake bootstrap DDL for DMT v1: control database, config tables, step tracking, file manifest, and audit log.
-- ============================================================================
-- DMT v1 — Snowflake-side setup (idempotent)
--
-- Creates:
--   HISTLOAD_DB              — control database
--   HISTLOAD_DB.META         — shared metadata schema
--   CONNECTION_PROFILES      — source connection registry
--   MIGRATION_CONFIG         — per-table migration settings (replaces JSON)
--   PIPELINE_STEP_LOG        — step-level state for retry-from-failure
--   FILE_MANIFEST            — tracks extracted files across storage backends
--   RUN_LOG                  — batch-level audit
--   V_RUN_LOG                — reporting view
--   Internal stage + file formats
-- ============================================================================

-- ─── Control database & schema ───────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS HISTLOAD_DB;
CREATE SCHEMA   IF NOT EXISTS HISTLOAD_DB.META;

USE SCHEMA HISTLOAD_DB.META;

-- ─── App settings (controls source types, limits, defaults) ──────────────────
CREATE TABLE IF NOT EXISTS DMT_SETTINGS (
    SETTING_KEY     VARCHAR        NOT NULL,
    SETTING_VALUE   VARCHAR,
    DESCRIPTION     VARCHAR,
    UPDATED_AT      TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_DMT_SETTINGS PRIMARY KEY (SETTING_KEY)
);

-- Default: allow all implemented sources. Remove entries to restrict.
-- Example: SET SETTING_VALUE = 'mysql' to only allow MySQL migrations.
INSERT INTO DMT_SETTINGS (SETTING_KEY, SETTING_VALUE, DESCRIPTION)
SELECT 'ALLOWED_SOURCES', 'mysql,teradata,mssql,oracle', 'Comma-separated list of enabled source types. Options: mysql, teradata, mssql, oracle, postgres'
WHERE NOT EXISTS (SELECT 1 FROM DMT_SETTINGS WHERE SETTING_KEY = 'ALLOWED_SOURCES');

-- ─── Connection profiles (multi-source registry) ─────────────────────────────
-- POC auth scope: username + password only. Advanced mechanisms (Kerberos,
-- LDAP, JWT, Oracle wallet, Azure managed identity) are not yet supported.
--
-- Required input fields by SOURCE_TYPE:
--   mysql     : HOST, PORT (3306), USERNAME, PASSWORD
--   mssql     : HOST, PORT (1433), USERNAME, PASSWORD
--               + EXTRA_PARAMS:{"driver": "ODBC Driver 18 for SQL Server"}
--   teradata  : HOST, USERNAME, PASSWORD   (PORT unused; LOGMECH fixed to TD2)
--   oracle    : HOST, PORT (1521), USERNAME, PASSWORD
--               + EXTRA_PARAMS:{"service_name": "FREEPDB1"}
--
-- NOTE: PASSWORD is stored in plaintext. Prefer AUTH_SECRET (an environment
-- variable name) for anything beyond a POC.
CREATE TABLE IF NOT EXISTS CONNECTION_PROFILES (
    PROFILE_NAME    VARCHAR        NOT NULL,
    SOURCE_TYPE     VARCHAR        NOT NULL,       -- mysql | teradata | mssql | oracle | postgres
    HOST            VARCHAR,
    PORT            NUMBER,
    USERNAME        VARCHAR,
    PASSWORD        VARCHAR,                        -- stored password (alternative to SECRET)
    AUTH_SECRET     VARCHAR,                        -- Snowflake SECRET name (optional)
    LOGMECH         VARCHAR        DEFAULT 'TD2',  -- Teradata only: TD2 | LDAP
    EXTRA_PARAMS    VARIANT,                        -- JSON: driver (mssql), service_name (oracle), ssl, etc.
    IS_ACTIVE       BOOLEAN        DEFAULT TRUE,
    CREATED_AT      TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT      TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_CONN_PROFILES PRIMARY KEY (PROFILE_NAME)
);

-- ─── Migration config (replaces histload_config.json) ────────────────────────
CREATE TABLE IF NOT EXISTS MIGRATION_CONFIG (
    CONFIG_ID           VARCHAR        DEFAULT UUID_STRING(),
    CONNECTION_PROFILE  VARCHAR        NOT NULL,
    SOURCE_DB           VARCHAR        NOT NULL,
    SOURCE_TABLE        VARCHAR        NOT NULL,
    SOURCE_SCHEMA       VARCHAR,                    -- MSSQL: dbo, Sales, etc. (NULL for MySQL/Teradata)
    TARGET_DB           VARCHAR,                    -- defaults to UPPER(SOURCE_DB) at runtime
    TARGET_TABLE        VARCHAR,                    -- defaults to UPPER(SOURCE_TABLE) at runtime
    TARGET_SCHEMA       VARCHAR,                    -- Teradata only: override schema (NULL = auto-resolve)
    LOAD_TYPE           VARCHAR        DEFAULT 'full',       -- full | incremental
    WATERMARK_COL       VARCHAR,
    WATERMARK_TYPE      VARCHAR,                    -- time | id | NULL
    LAST_LOADED_AT      TIMESTAMP_NTZ,
    LAST_LOADED_KEY     VARCHAR,
    PRIMARY_KEY         VARCHAR,
    MERGE_KEYS          ARRAY,                      -- e.g. ['EMP_NO', 'FROM_DATE']
    PARTITION_COL       VARCHAR,
    PARTITION_NUM       NUMBER         DEFAULT 8,
    ROWS_PER_FILE       NUMBER         DEFAULT 1000000,

    -- Storage settings (where extracted files land)
    STORAGE_TYPE        VARCHAR        DEFAULT 'internal_stage',  -- local | s3 | azure | internal_stage
    STORAGE_PATH        VARCHAR,                    -- stage name (e.g. DMT_EXT_S3) or s3://bucket/prefix/
    STORAGE_CREDENTIALS VARCHAR,                    -- Snowflake SECRET for cloud access (optional)

    -- Column handling
    BLOB_MODE           VARCHAR        DEFAULT 'binary',  -- binary | text | skip

    -- SCD and extraction settings
    SCD_TYPE            NUMBER         DEFAULT 1,   -- 0=append, 1=upsert, 2=history
    FILTER_CONDITION    VARCHAR,                    -- static WHERE clause for extraction
    CUSTOM_SQL          VARCHAR,                    -- full SELECT override (reserved for future)
    DELIMITER           VARCHAR        DEFAULT ',', -- Export field delimiter (Teradata TPT, MSSQL BCP)
    TRIM                BOOLEAN        DEFAULT FALSE, -- Trim column whitespace in export (Teradata, MSSQL)

    -- Execution control
    EXECUTION_MODE      VARCHAR        DEFAULT 'FULL',  -- FULL | EXTRACT_ONLY | LOAD_ONLY
    RECONCILE           BOOLEAN        DEFAULT FALSE,
    ACTIVE              BOOLEAN        DEFAULT FALSE,
    LAST_RUN_STATUS     VARCHAR,
    LAST_RUN_ID         VARCHAR,
    LAST_FAILED_STEP    VARCHAR,                    -- enables retry-from-failure

    NOTES               VARCHAR,
    CREATED_AT          TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT          TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_MIG_CONFIG PRIMARY KEY (CONFIG_ID),
    CONSTRAINT UQ_MIG_CONFIG UNIQUE (CONNECTION_PROFILE, SOURCE_DB, SOURCE_SCHEMA, SOURCE_TABLE)
);

-- ─── Pipeline step log (step-level state for retry) ──────────────────────────
CREATE TABLE IF NOT EXISTS PIPELINE_STEP_LOG (
    STEP_ID         VARCHAR        DEFAULT UUID_STRING(),
    RUN_ID          VARCHAR        NOT NULL,
    CONFIG_ID       VARCHAR        NOT NULL,
    SOURCE_DB       VARCHAR,
    SOURCE_TABLE    VARCHAR,
    STEP_NAME       VARCHAR        NOT NULL,       -- ddl | schema_drift | extract | upload | load | merge | watermark | validate
    STEP_ORDER      NUMBER         NOT NULL,       -- 1..N execution sequence
    STATUS          VARCHAR        DEFAULT 'pending',  -- pending | running | success | failed | skipped
    STARTED_AT      TIMESTAMP_NTZ,
    ENDED_AT        TIMESTAMP_NTZ,
    ERROR_MESSAGE   VARCHAR,
    RETRY_COUNT     NUMBER         DEFAULT 0,
    METADATA        VARIANT,                        -- step-specific data (row counts, file paths, etc.)

    CONSTRAINT PK_STEP_LOG PRIMARY KEY (STEP_ID)
);

-- ─── File manifest (tracks extracted files for decoupled extract/load) ───────
CREATE TABLE IF NOT EXISTS FILE_MANIFEST (
    MANIFEST_ID     VARCHAR        DEFAULT UUID_STRING(),
    RUN_ID          VARCHAR        NOT NULL,
    CONFIG_ID       VARCHAR        NOT NULL,
    SOURCE_DB       VARCHAR,
    SOURCE_TABLE    VARCHAR,
    FILE_PATH       VARCHAR        NOT NULL,       -- full path (local, s3://, azure://, @stage/)
    STORAGE_TYPE    VARCHAR        NOT NULL,       -- local | s3 | azure | internal_stage
    FILE_FORMAT     VARCHAR,                        -- parquet | tsv_zstd | csv
    FILE_SIZE_BYTES NUMBER,
    ROW_COUNT       NUMBER,
    PART_NUMBER     NUMBER,                         -- for multi-part extracts
    EXTRACTED_AT    TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    UPLOADED_AT     TIMESTAMP_NTZ,                  -- when moved to cloud/stage (NULL if local-only)
    LOADED_AT       TIMESTAMP_NTZ,                  -- when COPY INTO consumed it
    STATUS          VARCHAR        DEFAULT 'extracted',  -- extracted | uploaded | loaded | failed

    CONSTRAINT PK_FILE_MANIFEST PRIMARY KEY (MANIFEST_ID)
);

-- ─── Run log (batch-level audit) ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS RUN_LOG (
    RUN_ID          VARCHAR        DEFAULT UUID_STRING(),
    BATCH_ID        VARCHAR,
    CONFIG_ID       VARCHAR,
    CONNECTION_PROFILE VARCHAR,
    SOURCE_DB       VARCHAR,
    SOURCE_TABLE    VARCHAR,
    TARGET_DB       VARCHAR,
    TARGET_TABLE    VARCHAR,
    LOAD_TYPE       VARCHAR,                        -- full | incremental | reconcile | validate
    ENGINE          VARCHAR,                        -- mysqlsh | connectorx | tpt | bcp | reconciler | validator
    ROWS_EXTRACTED  NUMBER,
    ROWS_LOADED     NUMBER,
    WATERMARK_FROM  VARCHAR,
    WATERMARK_TO    VARCHAR,
    WATERMARK_TYPE  VARCHAR,                        -- time | id | NULL
    STATUS          VARCHAR,                        -- success | failed | skipped | mismatch
    ERROR_MESSAGE   VARCHAR,
    FAILED_STEP     VARCHAR,
    DURATION_SEC    NUMBER(38,2),
    RUN_START       TIMESTAMP_NTZ,
    RUN_END         TIMESTAMP_NTZ,
    INSERTED_AT     TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_RUN_LOG PRIMARY KEY (RUN_ID)
);

-- ─── Internal stage ──────────────────────────────────────────────────────────
CREATE STAGE IF NOT EXISTS HISTLOAD_DB.META.DMT_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT   = 'Landing stage for DMT extracts (parquet + tsv.zst)';

-- ─── Storage Integration (for S3/Azure external stages) ─────────────────────
-- Uncomment and configure ONE of the following based on your cloud provider.
-- After creating, run: DESC INTEGRATION DMT_S3_INTEGRATION;
-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID to your AWS IAM
-- role trust policy.

-- ── AWS S3 ──
-- CREATE STORAGE INTEGRATION IF NOT EXISTS DMT_S3_INTEGRATION
--     TYPE = EXTERNAL_STAGE
--     STORAGE_PROVIDER = 'S3'
--     ENABLED = TRUE
--     STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<account_id>:role/dmt-snowflake-role'
--     STORAGE_ALLOWED_LOCATIONS = ('s3://<your-bucket>/');

-- ── Azure Blob ──
-- CREATE STORAGE INTEGRATION IF NOT EXISTS DMT_AZURE_INTEGRATION
--     TYPE = EXTERNAL_STAGE
--     STORAGE_PROVIDER = 'AZURE'
--     ENABLED = TRUE
--     AZURE_TENANT_ID = '<tenant-id>'
--     STORAGE_ALLOWED_LOCATIONS = ('azure://<account>.blob.core.windows.net/<container>/');

-- ─── External stage (created after storage integration is set up) ────────────
-- Uncomment after creating your storage integration above.

-- ── S3 External Stage ──
-- CREATE STAGE IF NOT EXISTS HISTLOAD_DB.META.DMT_EXT_S3
--     URL = 's3://<your-bucket>/'
--     STORAGE_INTEGRATION = DMT_S3_INTEGRATION
--     FILE_FORMAT = (FORMAT_NAME = HISTLOAD_DB.META.PARQUET_FMT)
--     COMMENT = 'External S3 stage for DMT extracts';

-- ── Azure External Stage ──
-- CREATE STAGE IF NOT EXISTS HISTLOAD_DB.META.DMT_EXT_AZURE
--     URL = 'azure://<account>.blob.core.windows.net/<container>/'
--     STORAGE_INTEGRATION = DMT_AZURE_INTEGRATION
--     FILE_FORMAT = (FORMAT_NAME = HISTLOAD_DB.META.PARQUET_FMT)
--     COMMENT = 'External Azure stage for DMT extracts';

-- ─── File formats ────────────────────────────────────────────────────────────
CREATE OR REPLACE FILE FORMAT HISTLOAD_DB.META.PARQUET_FMT
    TYPE = PARQUET
    USE_LOGICAL_TYPE = TRUE;

CREATE FILE FORMAT IF NOT EXISTS HISTLOAD_DB.META.TSV_ZSTD_FMT
    TYPE             = CSV
    FIELD_DELIMITER  = '\t'
    COMPRESSION      = ZSTD
    NULL_IF          = ('\\N', '0000-00-00', '0000-00-00 00:00:00')
    EMPTY_FIELD_AS_NULL = FALSE
    SKIP_HEADER      = 0
    FIELD_OPTIONALLY_ENCLOSED_BY = NONE
    ESCAPE_UNENCLOSED_FIELD = '\\';

CREATE FILE FORMAT IF NOT EXISTS HISTLOAD_DB.META.CSV_FMT
    TYPE             = CSV
    FIELD_DELIMITER  = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER      = 0
    NULL_IF          = ('')
    EMPTY_FIELD_AS_NULL = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    TIMESTAMP_FORMAT = 'AUTO'
    DATE_FORMAT      = 'AUTO'
    COMMENT          = 'CSV format for Teradata TPT exports';

CREATE FILE FORMAT IF NOT EXISTS HISTLOAD_DB.META.BCP_PIPE_FMT
    TYPE             = CSV
    FIELD_DELIMITER  = '|'
    COMPRESSION      = GZIP
    FIELD_OPTIONALLY_ENCLOSED_BY = NONE
    SKIP_HEADER      = 0
    NULL_IF          = ('')
    EMPTY_FIELD_AS_NULL = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    TIMESTAMP_FORMAT = 'AUTO'
    DATE_FORMAT      = 'AUTO'
    ENCODING         = 'UTF8'
    COMMENT          = 'Pipe-delimited gzip format for MSSQL BCP exports';

-- ─── Reporting view ──────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW HISTLOAD_DB.META.V_RUN_LOG AS
SELECT
    INSERTED_AT,
    BATCH_ID,
    CONFIG_ID,
    CONNECTION_PROFILE,
    SOURCE_DB,
    SOURCE_TABLE,
    TARGET_DB,
    TARGET_TABLE,
    LOAD_TYPE,
    ENGINE,
    STATUS,
    FAILED_STEP,
    DURATION_SEC,
    CASE LOAD_TYPE
        WHEN 'reconcile' THEN 'deleted=' || COALESCE(ROWS_LOADED::STRING, '0')
        WHEN 'validate'  THEN 'source=' || COALESCE(ROWS_EXTRACTED::STRING, '?')
                             || ' raw=' || COALESCE(ROWS_LOADED::STRING, '?')
        ELSE 'extracted=' || COALESCE(ROWS_EXTRACTED::STRING, '0')
             || ' loaded=' || COALESCE(ROWS_LOADED::STRING, '0')
    END                           AS ROW_DETAIL,
    WATERMARK_FROM,
    WATERMARK_TO,
    WATERMARK_TYPE,
    ERROR_MESSAGE,
    RUN_START,
    RUN_END
FROM HISTLOAD_DB.META.RUN_LOG
ORDER BY INSERTED_AT DESC;

-- ─── Step progress view (for UI / monitoring) ────────────────────────────────
CREATE OR REPLACE VIEW HISTLOAD_DB.META.V_PIPELINE_PROGRESS AS
SELECT
    p.RUN_ID,
    p.CONFIG_ID,
    p.SOURCE_DB,
    p.SOURCE_TABLE,
    p.STEP_NAME,
    p.STEP_ORDER,
    p.STATUS,
    p.STARTED_AT,
    p.ENDED_AT,
    DATEDIFF('second', p.STARTED_AT, COALESCE(p.ENDED_AT, CURRENT_TIMESTAMP())) AS ELAPSED_SEC,
    p.ERROR_MESSAGE,
    p.RETRY_COUNT
FROM HISTLOAD_DB.META.PIPELINE_STEP_LOG p
ORDER BY p.RUN_ID DESC, p.STEP_ORDER;

-- ─── Schema migrations (idempotent ALTER for existing installations) ─────────
-- Add SOURCE_SCHEMA column for MSSQL support (NULL for MySQL/Teradata).
ALTER TABLE HISTLOAD_DB.META.MIGRATION_CONFIG
    ADD COLUMN IF NOT EXISTS SOURCE_SCHEMA VARCHAR;

-- Update ALLOWED_SOURCES to include mssql for existing installations.
MERGE INTO HISTLOAD_DB.META.DMT_SETTINGS t
USING (SELECT 'ALLOWED_SOURCES' AS K) s ON t.SETTING_KEY = s.K
WHEN MATCHED AND NOT CONTAINS(t.SETTING_VALUE, 'mssql') THEN
    UPDATE SET SETTING_VALUE = t.SETTING_VALUE || ',mssql', UPDATED_AT = CURRENT_TIMESTAMP();

-- Update ALLOWED_SOURCES to include oracle for existing installations.
MERGE INTO HISTLOAD_DB.META.DMT_SETTINGS t
USING (SELECT 'ALLOWED_SOURCES' AS K) s ON t.SETTING_KEY = s.K
WHEN MATCHED AND NOT CONTAINS(t.SETTING_VALUE, 'oracle') THEN
    UPDATE SET SETTING_VALUE = t.SETTING_VALUE || ',oracle', UPDATED_AT = CURRENT_TIMESTAMP();

-- ─── File Ingestion (cloud file → Snowflake) ─────────────────────────────────
-- Separate config table for file-based ingestion (no source database needed).

CREATE TABLE IF NOT EXISTS FILE_INGESTION_CONFIG (
    CONFIG_ID                   VARCHAR DEFAULT UUID_STRING() NOT NULL,
    JOB_NAME                    VARCHAR NOT NULL,
    ACTIVE                      BOOLEAN DEFAULT TRUE,

    -- Source (cloud files on an external stage)
    CLOUD_PROVIDER              VARCHAR DEFAULT 'S3',
    STAGE_NAME                  VARCHAR NOT NULL,
    CLOUD_PATH                  VARCHAR DEFAULT '',
    FILE_PATTERN                VARCHAR NOT NULL,
    FILE_TYPE                   VARCHAR DEFAULT 'CSV',

    -- Target (Snowflake)
    TARGET_DB                   VARCHAR NOT NULL,
    TARGET_SCHEMA               VARCHAR DEFAULT 'RAW',
    TARGET_TABLE                VARCHAR NOT NULL,
    WAREHOUSE                   VARCHAR,

    -- Load behavior
    LOAD_MODE                   VARCHAR DEFAULT 'APPEND',
    TABLE_EXISTS                BOOLEAN DEFAULT TRUE,
    MERGE_KEYS                  VARCHAR,

    -- File format options
    FIELD_DELIMITER             VARCHAR DEFAULT ',',
    FIELD_ENCLOSED_BY           VARCHAR DEFAULT '"',
    ESCAPE_CHARACTER            VARCHAR DEFAULT '\\',
    SKIP_HEADER                 NUMBER  DEFAULT 1,
    NULL_IF                     VARCHAR DEFAULT '('''')',
    FILE_FORMAT_EXTRAS          VARCHAR,

    -- COPY INTO options
    ON_ERROR                    VARCHAR DEFAULT 'ABORT_STATEMENT',
    MATCH_BY_COLUMN_NAME        VARCHAR,
    COPY_EXTRAS                 VARCHAR,
    PURGE_FILES                 BOOLEAN DEFAULT FALSE,

    -- Date-partitioned paths
    DATE_PARTITION              BOOLEAN DEFAULT FALSE,
    DATE_FORMAT                 VARCHAR DEFAULT '%Y%m%d',

    -- Tracking
    LAST_RUN_STATUS             VARCHAR,
    LAST_RUN_AT                 TIMESTAMP_NTZ,
    LAST_FILE_COUNT             NUMBER,
    LAST_ROW_COUNT              NUMBER,
    LAST_ERROR                  VARCHAR,
    CREATED_AT                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_FILE_INGEST_CONFIG PRIMARY KEY (CONFIG_ID)
);

CREATE TABLE IF NOT EXISTS FILE_INGESTION_LOG (
    LOG_ID                      VARCHAR DEFAULT UUID_STRING() NOT NULL,
    BATCH_ID                    VARCHAR,
    CONFIG_ID                   VARCHAR,
    JOB_NAME                    VARCHAR,
    STAGE_NAME                  VARCHAR,
    CLOUD_PATH                  VARCHAR,
    FILE_PATTERN                VARCHAR,
    FILE_TYPE                   VARCHAR,
    FILES_MATCHED               NUMBER,
    TARGET_DB                   VARCHAR,
    TARGET_SCHEMA               VARCHAR,
    TARGET_TABLE                VARCHAR,
    LOAD_MODE                   VARCHAR,
    ROWS_LOADED                 NUMBER,
    FILES_LOADED                NUMBER,
    TABLE_CREATED               BOOLEAN DEFAULT FALSE,
    STATUS                      VARCHAR,
    ERROR_MESSAGE               VARCHAR,
    FAILED_STEP                 VARCHAR,
    DURATION_SEC                NUMBER(10,2),
    RUN_START                   TIMESTAMP_NTZ,
    RUN_END                     TIMESTAMP_NTZ,
    INSERTED_AT                 TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_FILE_INGEST_LOG PRIMARY KEY (LOG_ID)
);

CREATE OR REPLACE VIEW V_FILE_INGESTION_LOG AS
SELECT
    l.BATCH_ID, l.JOB_NAME,
    l.TARGET_DB || '.' || l.TARGET_SCHEMA || '.' || l.TARGET_TABLE AS TARGET_FQN,
    l.FILE_TYPE, l.LOAD_MODE,
    l.FILES_MATCHED, l.FILES_LOADED, l.ROWS_LOADED,
    l.STATUS, l.ERROR_MESSAGE, l.DURATION_SEC,
    l.RUN_START, l.RUN_END, l.INSERTED_AT
FROM FILE_INGESTION_LOG l
ORDER BY l.INSERTED_AT DESC;


-- ═══════════════════════════════════════════════════════════════════════════════
-- SCHEMA MIGRATIONS — safely adds columns that may be missing on older deployments.
-- These are idempotent (IF NOT EXISTS) — safe to run on every deployment.
-- ═══════════════════════════════════════════════════════════════════════════════

-- MIGRATION_CONFIG additions
ALTER TABLE IF EXISTS MIGRATION_CONFIG ADD COLUMN IF NOT EXISTS SOURCE_SCHEMA VARCHAR;
ALTER TABLE IF EXISTS MIGRATION_CONFIG ADD COLUMN IF NOT EXISTS TARGET_SCHEMA VARCHAR;
ALTER TABLE IF EXISTS MIGRATION_CONFIG ADD COLUMN IF NOT EXISTS SCD_TYPE NUMBER DEFAULT 1;
ALTER TABLE IF EXISTS MIGRATION_CONFIG ADD COLUMN IF NOT EXISTS FILTER_CONDITION VARCHAR;
ALTER TABLE IF EXISTS MIGRATION_CONFIG ADD COLUMN IF NOT EXISTS CUSTOM_SQL VARCHAR;
ALTER TABLE IF EXISTS MIGRATION_CONFIG ADD COLUMN IF NOT EXISTS DELIMITER VARCHAR DEFAULT ',';
ALTER TABLE IF EXISTS MIGRATION_CONFIG ADD COLUMN IF NOT EXISTS TRIM BOOLEAN DEFAULT FALSE;
ALTER TABLE IF EXISTS MIGRATION_CONFIG ADD COLUMN IF NOT EXISTS BLOB_MODE VARCHAR DEFAULT 'binary';
ALTER TABLE IF EXISTS MIGRATION_CONFIG ADD COLUMN IF NOT EXISTS EXECUTION_MODE VARCHAR DEFAULT 'FULL';
ALTER TABLE IF EXISTS MIGRATION_CONFIG ADD COLUMN IF NOT EXISTS LAST_FAILED_STEP VARCHAR;

-- FILE_INGESTION_CONFIG additions
ALTER TABLE IF EXISTS FILE_INGESTION_CONFIG ADD COLUMN IF NOT EXISTS PURGE_FILES BOOLEAN DEFAULT FALSE;
ALTER TABLE IF EXISTS FILE_INGESTION_CONFIG ADD COLUMN IF NOT EXISTS DATE_PARTITION BOOLEAN DEFAULT FALSE;
ALTER TABLE IF EXISTS FILE_INGESTION_CONFIG ADD COLUMN IF NOT EXISTS DATE_FORMAT VARCHAR DEFAULT '%Y%m%d';
ALTER TABLE IF EXISTS FILE_INGESTION_CONFIG ADD COLUMN IF NOT EXISTS MERGE_KEYS VARCHAR;
ALTER TABLE IF EXISTS FILE_INGESTION_CONFIG ADD COLUMN IF NOT EXISTS MATCH_BY_COLUMN_NAME VARCHAR;
ALTER TABLE IF EXISTS FILE_INGESTION_CONFIG ADD COLUMN IF NOT EXISTS ESCAPE_CHARACTER VARCHAR DEFAULT '\\';
ALTER TABLE IF EXISTS FILE_INGESTION_CONFIG ADD COLUMN IF NOT EXISTS TABLE_EXISTS BOOLEAN DEFAULT TRUE;
ALTER TABLE IF EXISTS FILE_INGESTION_CONFIG ADD COLUMN IF NOT EXISTS COPY_EXTRAS VARCHAR;
ALTER TABLE IF EXISTS FILE_INGESTION_CONFIG ADD COLUMN IF NOT EXISTS FILE_FORMAT_EXTRAS VARCHAR;
ALTER TABLE IF EXISTS FILE_INGESTION_CONFIG ADD COLUMN IF NOT EXISTS WAREHOUSE VARCHAR;
ALTER TABLE IF EXISTS FILE_INGESTION_CONFIG ADD COLUMN IF NOT EXISTS NULL_IF VARCHAR DEFAULT '('''')';

ALTER TABLE IF EXISTS FILE_INGESTION_CONFIG ADD COLUMN IF NOT EXISTS LAST_ERROR VARCHAR;

-- FILE_INGESTION_LOG additions
ALTER TABLE IF EXISTS FILE_INGESTION_LOG ADD COLUMN IF NOT EXISTS TABLE_CREATED BOOLEAN;
ALTER TABLE IF EXISTS FILE_INGESTION_LOG ADD COLUMN IF NOT EXISTS LOAD_MODE VARCHAR;
ALTER TABLE IF EXISTS FILE_INGESTION_LOG ADD COLUMN IF NOT EXISTS TARGET_DB VARCHAR;
ALTER TABLE IF EXISTS FILE_INGESTION_LOG ADD COLUMN IF NOT EXISTS TARGET_SCHEMA VARCHAR;
ALTER TABLE IF EXISTS FILE_INGESTION_LOG ADD COLUMN IF NOT EXISTS TARGET_TABLE VARCHAR;

-- ═══════════════════════════════════════════════════════════════════════════════
-- ALERT RULES & LOG (Observability — Alerts & Rules tab)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS DMT_ALERT_RULES (
    RULE_ID             VARCHAR DEFAULT UUID_STRING() PRIMARY KEY,
    RULE_NAME           VARCHAR NOT NULL,
    CONDITION_TYPE      VARCHAR NOT NULL,  -- TABLE_STALE | RUN_FAILED | ROW_DRIFT_PCT | CONSECUTIVE_FAILURES
    THRESHOLD           NUMBER,            -- hours for stale, % for drift, count for consecutive
    TABLE_SCOPE         VARCHAR,           -- optional: specific table name (NULL = all)
    ACTION_TYPE         VARCHAR NOT NULL,  -- LOG_ONLY | WEBHOOK_SLACK | WEBHOOK_TEAMS | WEBHOOK_CUSTOM
    WEBHOOK_URL         VARCHAR,           -- webhook endpoint (NULL for LOG_ONLY)
    ACTIVE              BOOLEAN DEFAULT TRUE,
    LAST_TRIGGERED_AT   TIMESTAMP_NTZ,
    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS DMT_ALERT_LOG (
    ALERT_ID            VARCHAR DEFAULT UUID_STRING() PRIMARY KEY,
    RULE_ID             VARCHAR,
    RULE_NAME           VARCHAR,
    CONDITION_TYPE      VARCHAR,
    TABLE_NAME          VARCHAR,
    MESSAGE             VARCHAR,
    ACTION_TAKEN        VARCHAR,  -- logged | sent | failed
    TRIGGERED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
