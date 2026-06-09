"""run_log.py — Write one audit row per table per run to HISTLOAD_DB.META.RUN_LOG."""
from __future__ import annotations


def write_run_log(cur, rec: dict):
    """Insert an audit record. `rec` keys map to RUN_LOG columns."""
    cur.execute(
        """
        INSERT INTO HISTLOAD_DB.META.RUN_LOG
            (BATCH_ID, SOURCE_DB, SOURCE_TABLE, TARGET_DB, TARGET_TABLE,
             LOAD_TYPE, ENGINE, ROWS_EXTRACTED, ROWS_LOADED,
             WATERMARK_FROM, WATERMARK_TO, WATERMARK_TYPE, STATUS, ERROR_MESSAGE,
             FAILED_STEP, DURATION_SEC, RUN_START, RUN_END)
        SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
               TO_TIMESTAMP_NTZ(%s), TO_TIMESTAMP_NTZ(%s)
        """,
        (
            rec.get("batch_id"), rec.get("source_db"), rec.get("source_table"),
            rec.get("target_db"), rec.get("target_table"), rec.get("load_type"),
            rec.get("engine"), rec.get("rows_extracted"), rec.get("rows_loaded"),
            rec.get("watermark_from"), rec.get("watermark_to"),
            rec.get("watermark_type"), rec.get("status"),
            rec.get("error_message"), rec.get("failed_step"),
            rec.get("duration_sec"), rec.get("run_start"), rec.get("run_end"),
        ),
    )
