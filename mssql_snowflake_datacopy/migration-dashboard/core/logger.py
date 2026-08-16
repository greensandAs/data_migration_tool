"""logger.py — LOG_TABLE and AUDIT operations for migration tracking."""
from __future__ import annotations

from datetime import datetime

from core.connections import sf_execute, sf_query


def get_next_batch_id() -> int:
    """Get the next batch ID from LOG_TABLE."""
    try:
        df = sf_query("SELECT COALESCE(MAX(BATCH_ID)+1, 10000) AS NXT FROM DATA_MIGRATION.CONTROL.LOG_TABLE")
        return int(df.iloc[0]["NXT"])
    except Exception:
        return 10000


def create_log_entry(batch_id: int, job_id: int, tbl: dict, execution_mode: str, job_start: datetime):
    """Insert initial log row at job start."""
    sf_execute(
        "INSERT INTO DATA_MIGRATION.CONTROL.LOG_TABLE "
        "(BATCH_ID, JOB_ID, MSSQL_DATABASE_NAME, MSSQL_SCHEMA_NAME, MSSQL_TABLE_NAME, "
        "SF_DATABASE_NAME, SF_SCHEMA_NAME, SF_TABLE_NAME, LOAD_TYPE, "
        "S3_PATH, EXECUTION_MODE, JOB_START_TIME) "
        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (
            batch_id, job_id,
            tbl.get("source_db"), tbl.get("source_schema", "dbo"), tbl.get("source_table"),
            tbl.get("target_db", "ANALYTICS"), tbl.get("target_schema", "PUBLIC"), tbl.get("target_table"),
            (tbl.get("load_type") or "full").upper(),
            tbl.get("cloud_path", ""), execution_mode.upper(), job_start,
        ),
    )


def update_step(batch_id: int, job_id: int, step_column: str, status: str, log_value: str = None):
    """Update a specific step's status in LOG_TABLE."""
    if log_value:
        log_col = step_column.replace("_STATUS", "_LOG")
        sf_execute(
            f"UPDATE DATA_MIGRATION.CONTROL.LOG_TABLE SET {step_column}=%s, {log_col}=%s "
            "WHERE BATCH_ID=%s AND JOB_ID=%s",
            (status, log_value[:4000], batch_id, job_id),
        )
    else:
        sf_execute(
            f"UPDATE DATA_MIGRATION.CONTROL.LOG_TABLE SET {step_column}=%s "
            "WHERE BATCH_ID=%s AND JOB_ID=%s",
            (status, batch_id, job_id),
        )


def update_export_filename(batch_id: int, job_id: int, filename: str, row_count: int):
    """Update export filename and MSSQL row count."""
    sf_execute(
        "UPDATE DATA_MIGRATION.CONTROL.LOG_TABLE SET "
        "EXPORT_FILENAME=%s, MSSQL_TABLE_COUNT=%s WHERE BATCH_ID=%s AND JOB_ID=%s",
        (filename, row_count, batch_id, job_id),
    )


def finalize_job(batch_id: int, job_id: int, status: str, job_end: datetime,
                 duration: int, row_count: int, sf_count: int,
                 rows_extracted: int = 0, rows_inserted: int = 0,
                 rows_updated: int = 0, rows_expired: int = 0):
    """Mark job as completed with final metrics."""
    sf_execute(
        "UPDATE DATA_MIGRATION.CONTROL.LOG_TABLE SET "
        "FINAL_STATUS=%s, JOB_END_TIME=%s, JOB_DURATION=%s, "
        "MSSQL_TABLE_COUNT=%s, SF_TABLE_COUNT=%s, "
        "ROWS_EXTRACTED=%s, ROWS_INSERTED=%s, ROWS_UPDATED=%s, ROWS_EXPIRED=%s, "
        "INGESTION_COMPLETED='YES' "
        "WHERE BATCH_ID=%s AND JOB_ID=%s",
        (status, job_end, duration, row_count, sf_count,
         rows_extracted, rows_inserted, rows_updated, rows_expired,
         batch_id, job_id),
    )


def update_merge_sql(batch_id: int, job_id: int, merge_sql: str):
    """Store the MERGE statement in LOG_TABLE."""
    sf_execute(
        "UPDATE DATA_MIGRATION.CONTROL.LOG_TABLE SET "
        "MERGE_STATEMENT=%s, MERGE_STATEMENT_STATUS='SUCCESS' WHERE BATCH_ID=%s AND JOB_ID=%s",
        (merge_sql[:10000], batch_id, job_id),
    )


def update_copy_sql(batch_id: int, job_id: int, copy_sql: str):
    """Store the COPY command in LOG_TABLE."""
    sf_execute(
        "UPDATE DATA_MIGRATION.CONTROL.LOG_TABLE SET "
        "COPY_COMMAND=%s, COPY_COMMAND_STATUS='SUCCESS' WHERE BATCH_ID=%s AND JOB_ID=%s",
        (copy_sql[:10000], batch_id, job_id),
    )
