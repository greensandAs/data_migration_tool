-- ============================================================================
-- monitoring.sql — health checks for the MySQL -> Snowflake historical load.
-- All read-only. Audit log: HISTLOAD_DB.META.RUN_LOG. Data: <SCHEMA>.RAW.<table>.
-- ============================================================================

-- ─── 1. Failed runs in the last 24h (the alert query) ───────────────────────
SELECT BATCH_ID, SOURCE_DB, SOURCE_TABLE, TARGET_DB, TARGET_TABLE, LOAD_TYPE,
       ENGINE, STATUS, ERROR_MESSAGE, RUN_START, RUN_END
FROM HISTLOAD_DB.META.RUN_LOG
WHERE STATUS = 'failed'
  AND INSERTED_AT > DATEADD('hour', -24, CURRENT_TIMESTAMP())
ORDER BY INSERTED_AT DESC;

-- ─── 2. Latest run per table (current state at a glance) ────────────────────
SELECT SOURCE_DB, TARGET_DB, TARGET_TABLE, LOAD_TYPE, ENGINE, STATUS,
       ROWS_EXTRACTED, ROWS_LOADED, WATERMARK_FROM, WATERMARK_TO, RUN_END
FROM HISTLOAD_DB.META.RUN_LOG
QUALIFY ROW_NUMBER() OVER (
            PARTITION BY SOURCE_DB, TARGET_TABLE
            ORDER BY INSERTED_AT DESC) = 1
ORDER BY SOURCE_DB, TARGET_TABLE;

-- ─── 3. Stale tables: no SUCCESSFUL run in the last 24h ─────────────────────
SELECT SOURCE_DB, TARGET_TABLE,
       MAX(CASE WHEN STATUS = 'success' THEN INSERTED_AT END) AS last_success
FROM HISTLOAD_DB.META.RUN_LOG
GROUP BY SOURCE_DB, TARGET_TABLE
HAVING last_success IS NULL
    OR last_success < DATEADD('hour', -24, CURRENT_TIMESTAMP())
ORDER BY last_success NULLS FIRST;

-- ─── 4. Validation mismatches (last 24h) ────────────────────────────────────
SELECT SOURCE_DB, SOURCE_TABLE, ROWS_EXTRACTED AS source_rows,
       ROWS_LOADED AS raw_live_rows, ERROR_MESSAGE, INSERTED_AT
FROM HISTLOAD_DB.META.RUN_LOG
WHERE LOAD_TYPE = 'validate' AND STATUS = 'mismatch'
  AND INSERTED_AT > DATEADD('hour', -24, CURRENT_TIMESTAMP())
ORDER BY INSERTED_AT DESC;

-- ─── 5. Error frequency (recurring failure modes) ───────────────────────────
SELECT LEFT(ERROR_MESSAGE, 120) AS error_prefix,
       COUNT(*)                 AS occurrences,
       MAX(INSERTED_AT)         AS last_seen
FROM HISTLOAD_DB.META.RUN_LOG
WHERE STATUS = 'failed' AND ERROR_MESSAGE IS NOT NULL
GROUP BY error_prefix
ORDER BY occurrences DESC;
