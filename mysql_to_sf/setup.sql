-- ============================================================================
-- MySQL -> Snowflake Historical Load  |  Snowflake-side setup (idempotent)
-- 1:1 lift-and-shift. Each MySQL schema maps to a Snowflake DATABASE of the same
-- name with a single RAW schema:  <MYSQL_SCHEMA>.RAW.<table>  (created at runtime
-- by ddl_generator.py). Shared/control objects live in HISTLOAD_DB.META.
-- Creates: HISTLOAD_DB, META schema, internal stage, PARQUET + TSV file formats,
--          META.RUN_LOG and the V_RUN_LOG reporting view.
-- ============================================================================

-- ─── Control database & shared schema ───────────────────────────────────────
CREATE DATABASE IF NOT EXISTS HISTLOAD_DB;
CREATE SCHEMA   IF NOT EXISTS HISTLOAD_DB.META;
-- Per-source <MYSQL_SCHEMA>.RAW schemas/tables are created by ddl_generator.py.

-- ─── Internal stage (extractor PUTs files here) — shared, in META ───────────
CREATE STAGE IF NOT EXISTS HISTLOAD_DB.META.HISTLOAD_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT   = 'Landing stage for MySQL extracts (parquet + tsv.zst)';

-- ─── File formats (shared, in META) ─────────────────────────────────────────
-- Parquet: connectorx incremental path. USE_LOGICAL_TYPE honors INT64 micro
-- timestamps so they load into TIMESTAMP_NTZ correctly.
CREATE OR REPLACE FILE FORMAT HISTLOAD_DB.META.PARQUET_FMT
    TYPE = PARQUET
    USE_LOGICAL_TYPE = TRUE;

-- TSV+zstd: mysqlsh dumpTables full-load path (tab-delimited, NULL as \N).
CREATE FILE FORMAT IF NOT EXISTS HISTLOAD_DB.META.TSV_ZSTD_FMT
    TYPE             = CSV
    FIELD_DELIMITER  = '\t'
    COMPRESSION      = ZSTD
    NULL_IF          = ('\\N')
    EMPTY_FIELD_AS_NULL = FALSE
    SKIP_HEADER      = 0
    FIELD_OPTIONALLY_ENCLOSED_BY = NONE
    ESCAPE_UNENCLOSED_FIELD = '\\';

-- ─── Audit / run log ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS HISTLOAD_DB.META.RUN_LOG (
    RUN_ID          VARCHAR        DEFAULT UUID_STRING(),
    BATCH_ID        VARCHAR,
    SOURCE_DB       VARCHAR,
    SOURCE_TABLE    VARCHAR,
    TARGET_DB       VARCHAR,
    TARGET_TABLE    VARCHAR,
    LOAD_TYPE       VARCHAR,                 -- full | incremental | reconcile | validate
    ENGINE          VARCHAR,                 -- mysqlsh | connectorx | reconciler | validator
    ROWS_EXTRACTED  NUMBER,
    ROWS_LOADED     NUMBER,                  -- rows landed/merged into RAW
    WATERMARK_FROM  VARCHAR,
    WATERMARK_TO    VARCHAR,
    WATERMARK_TYPE  VARCHAR,                 -- time | id | NULL (interprets FROM/TO)
    STATUS          VARCHAR,                 -- success | failed | skipped | mismatch
    ERROR_MESSAGE   VARCHAR,
    FAILED_STEP     VARCHAR,
    DURATION_SEC    NUMBER(38,2),
    RUN_START       TIMESTAMP_NTZ,
    RUN_END         TIMESTAMP_NTZ,
    INSERTED_AT     TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
);

-- Add newer columns to a pre-existing RUN_LOG (no-op if already present).
ALTER TABLE HISTLOAD_DB.META.RUN_LOG ADD COLUMN IF NOT EXISTS WATERMARK_TYPE VARCHAR;

-- ─── Clean reporting view over RUN_LOG ──────────────────────────────────────
CREATE OR REPLACE VIEW HISTLOAD_DB.META.V_RUN_LOG AS
SELECT
    INSERTED_AT,
    BATCH_ID,
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
