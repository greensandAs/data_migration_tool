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
SELECT 'ALLOWED_SOURCES', 'mysql,teradata', 'Comma-separated list of enabled source types. Options: mysql, teradata, postgres, oracle'
WHERE NOT EXISTS (SELECT 1 FROM DMT_SETTINGS WHERE SETTING_KEY = 'ALLOWED_SOURCES');

-- ─── Connection profiles (multi-source registry) ─────────────────────────────
-- Passwords are NOT stored here — use Snowflake SECRETs or environment variables.
CREATE TABLE IF NOT EXISTS CONNECTION_PROFILES (
    PROFILE_NAME    VARCHAR        NOT NULL,
    SOURCE_TYPE     VARCHAR        NOT NULL,       -- mysql | teradata | postgres | oracle
    HOST            VARCHAR,
    PORT            NUMBER,
    USERNAME        VARCHAR,
    PASSWORD        VARCHAR,                        -- stored password (alternative to SECRET)
    AUTH_SECRET     VARCHAR,                        -- Snowflake SECRET name (optional)
    LOGMECH         VARCHAR        DEFAULT 'TD2',  -- Teradata only: TD2 | LDAP
    EXTRA_PARAMS    VARIANT,                        -- JSON: charset, ssl, tpt_path, etc.
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
    DELIMITER           VARCHAR        DEFAULT ',', -- Teradata TPT export delimiter
    TRIM                BOOLEAN        DEFAULT FALSE, -- Teradata: TRIM columns in export

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
    CONSTRAINT UQ_MIG_CONFIG UNIQUE (CONNECTION_PROFILE, SOURCE_DB, SOURCE_TABLE)
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
    ENGINE          VARCHAR,                        -- mysqlsh | connectorx | tpt | reconciler | validator
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
    COMMENT          = 'CSV format for Teradata TPT exports';

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
