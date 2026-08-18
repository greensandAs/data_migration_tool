-- File Ingestion DDL for DMT v1 — separate config for cloud file → Snowflake loads.
-- ============================================================================
-- Appended to HISTLOAD_DB.META alongside the migration tables.
--
-- Creates:
--   FILE_INGESTION_CONFIG  — per-job file ingestion settings
--   FILE_INGESTION_LOG     — run audit per ingestion job
-- ============================================================================

USE SCHEMA HISTLOAD_DB.META;

-- ─── File Ingestion Config ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS FILE_INGESTION_CONFIG (
    CONFIG_ID                   VARCHAR DEFAULT UUID_STRING() NOT NULL,
    JOB_NAME                    VARCHAR NOT NULL,
    ACTIVE                      BOOLEAN DEFAULT TRUE,

    -- Source (cloud files on an external stage)
    CLOUD_PROVIDER              VARCHAR DEFAULT 'S3',           -- S3 | AZURE | GCS
    STAGE_NAME                  VARCHAR NOT NULL,               -- FQN: DB.SCHEMA.STAGE_NAME
    CLOUD_PATH                  VARCHAR DEFAULT '',             -- Subfolder within stage
    FILE_PATTERN                VARCHAR NOT NULL,               -- Regex (e.g. '.*sales.*\.csv')
    FILE_TYPE                   VARCHAR DEFAULT 'CSV',          -- CSV | PARQUET | JSON | AVRO

    -- Target (Snowflake)
    TARGET_DB                   VARCHAR NOT NULL,
    TARGET_SCHEMA               VARCHAR DEFAULT 'RAW',
    TARGET_TABLE                VARCHAR NOT NULL,
    WAREHOUSE                   VARCHAR,                        -- Override warehouse

    -- Load behavior
    LOAD_MODE                   VARCHAR DEFAULT 'APPEND',       -- APPEND | OVERWRITE | MERGE
    TABLE_EXISTS                BOOLEAN DEFAULT TRUE,           -- FALSE = auto-create via INFER_SCHEMA
    MERGE_KEYS                  VARCHAR,                        -- Comma-separated PK cols (MERGE mode)

    -- File format options (CSV-specific)
    FIELD_DELIMITER             VARCHAR DEFAULT ',',
    FIELD_ENCLOSED_BY           VARCHAR DEFAULT '"',
    ESCAPE_CHARACTER            VARCHAR DEFAULT '\\',
    SKIP_HEADER                 NUMBER  DEFAULT 1,
    NULL_IF                     VARCHAR DEFAULT '('''')',       -- Snowflake NULL_IF list
    FILE_FORMAT_EXTRAS          VARCHAR,                        -- Additional FORMAT options

    -- COPY INTO options
    ON_ERROR                    VARCHAR DEFAULT 'ABORT_STATEMENT',
    MATCH_BY_COLUMN_NAME        VARCHAR,                        -- CASE_INSENSITIVE | CASE_SENSITIVE
    COPY_EXTRAS                 VARCHAR,                        -- Additional COPY INTO options
    PURGE_FILES                 BOOLEAN DEFAULT FALSE,

    -- Date-partitioned paths
    DATE_PARTITION              BOOLEAN DEFAULT FALSE,          -- Append YYYYMMDD to CLOUD_PATH
    DATE_FORMAT                 VARCHAR DEFAULT '%Y%m%d',

    -- Tracking (populated at runtime)
    LAST_RUN_STATUS             VARCHAR,
    LAST_RUN_AT                 TIMESTAMP_NTZ,
    LAST_FILE_COUNT             NUMBER,
    LAST_ROW_COUNT              NUMBER,
    LAST_ERROR                  VARCHAR,
    CREATED_AT                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_FILE_INGEST_CONFIG PRIMARY KEY (CONFIG_ID)
);

-- ─── File Ingestion Run Log ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS FILE_INGESTION_LOG (
    LOG_ID                      VARCHAR DEFAULT UUID_STRING() NOT NULL,
    BATCH_ID                    VARCHAR,
    CONFIG_ID                   VARCHAR,
    JOB_NAME                    VARCHAR,

    -- Source details
    STAGE_NAME                  VARCHAR,
    CLOUD_PATH                  VARCHAR,
    FILE_PATTERN                VARCHAR,
    FILE_TYPE                   VARCHAR,
    FILES_MATCHED               NUMBER,

    -- Target details
    TARGET_DB                   VARCHAR,
    TARGET_SCHEMA               VARCHAR,
    TARGET_TABLE                VARCHAR,

    -- Results
    LOAD_MODE                   VARCHAR,
    ROWS_LOADED                 NUMBER,
    FILES_LOADED                NUMBER,
    TABLE_CREATED               BOOLEAN DEFAULT FALSE,

    -- Status
    STATUS                      VARCHAR,                        -- success | failed | skipped
    ERROR_MESSAGE               VARCHAR,
    FAILED_STEP                 VARCHAR,

    -- Timing
    DURATION_SEC                NUMBER(10,2),
    RUN_START                   TIMESTAMP_NTZ,
    RUN_END                     TIMESTAMP_NTZ,
    INSERTED_AT                 TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_FILE_INGEST_LOG PRIMARY KEY (LOG_ID)
);

-- ─── Reporting view ──────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW V_FILE_INGESTION_LOG AS
SELECT
    l.BATCH_ID,
    l.JOB_NAME,
    l.TARGET_DB || '.' || l.TARGET_SCHEMA || '.' || l.TARGET_TABLE AS TARGET_FQN,
    l.FILE_TYPE,
    l.LOAD_MODE,
    l.FILES_MATCHED,
    l.FILES_LOADED,
    l.ROWS_LOADED,
    l.STATUS,
    l.ERROR_MESSAGE,
    l.DURATION_SEC,
    l.RUN_START,
    l.RUN_END,
    l.INSERTED_AT
FROM FILE_INGESTION_LOG l
ORDER BY l.INSERTED_AT DESC;
