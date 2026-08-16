# Step-level state machine for pipeline execution with retry-from-failure.
# Co-authored with CoCo
"""step_tracker.py — Track pipeline step execution and enable resume-from-failure.

Each table run consists of ordered steps. On failure, the step and error are
recorded. On retry, the tracker identifies the last successful step and returns
the resume point so the orchestrator skips already-completed work.
"""
from __future__ import annotations

from datetime import datetime

_TABLE = "HISTLOAD_DB.META.PIPELINE_STEP_LOG"

# Canonical step order — orchestrator executes these in sequence.
STEPS = [
    "ddl",
    "schema_drift",
    "extract",
    "upload",
    "load",
    "merge",
    "watermark",
    "validate",
]


def init_steps(cur, run_id: str, config_id: str, source_db: str,
               source_table: str, steps: list[str] | None = None):
    """Create pending step entries for a new run."""
    step_list = steps or STEPS
    for i, step_name in enumerate(step_list, 1):
        cur.execute(
            f"""INSERT INTO {_TABLE}
                (RUN_ID, CONFIG_ID, SOURCE_DB, SOURCE_TABLE, STEP_NAME, STEP_ORDER, STATUS)
            VALUES (%s, %s, %s, %s, %s, %s, 'pending')
            """,
            (run_id, config_id, source_db, source_table, step_name, i),
        )


def mark_running(cur, run_id: str, step_name: str):
    """Mark a step as currently executing."""
    cur.execute(
        f"UPDATE {_TABLE} SET STATUS = 'running', "
        "STARTED_AT = CURRENT_TIMESTAMP() "
        "WHERE RUN_ID = %s AND STEP_NAME = %s",
        (run_id, step_name),
    )


def mark_success(cur, run_id: str, step_name: str, metadata: dict | None = None):
    """Mark a step as successfully completed."""
    import json
    cur.execute(
        f"UPDATE {_TABLE} SET STATUS = 'success', "
        "ENDED_AT = CURRENT_TIMESTAMP(), "
        "METADATA = PARSE_JSON(%s) "
        "WHERE RUN_ID = %s AND STEP_NAME = %s",
        (json.dumps(metadata) if metadata else None, run_id, step_name),
    )


def mark_failed(cur, run_id: str, step_name: str, error: str):
    """Mark a step as failed with error message."""
    cur.execute(
        f"UPDATE {_TABLE} SET STATUS = 'failed', "
        "ENDED_AT = CURRENT_TIMESTAMP(), "
        "ERROR_MESSAGE = %s "
        "WHERE RUN_ID = %s AND STEP_NAME = %s",
        (str(error)[:4000], run_id, step_name),
    )


def mark_skipped(cur, run_id: str, step_name: str, reason: str | None = None):
    """Mark a step as skipped (e.g., no rows to extract)."""
    cur.execute(
        f"UPDATE {_TABLE} SET STATUS = 'skipped', "
        "ENDED_AT = CURRENT_TIMESTAMP(), "
        "ERROR_MESSAGE = %s "
        "WHERE RUN_ID = %s AND STEP_NAME = %s",
        (reason, run_id, step_name),
    )


def increment_retry(cur, run_id: str, step_name: str):
    """Bump the retry counter for a step being re-attempted."""
    cur.execute(
        f"UPDATE {_TABLE} SET RETRY_COUNT = RETRY_COUNT + 1, "
        "STATUS = 'pending', ERROR_MESSAGE = NULL "
        "WHERE RUN_ID = %s AND STEP_NAME = %s",
        (run_id, step_name),
    )


def get_steps(cur, run_id: str) -> list[dict]:
    """Return all steps for a run, ordered by STEP_ORDER."""
    cur.execute(
        f"SELECT * FROM {_TABLE} WHERE RUN_ID = %s ORDER BY STEP_ORDER",
        (run_id,),
    )
    cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_resume_point(cur, run_id: str) -> str | None:
    """Find the step to resume from after a failure.

    Returns the STEP_NAME of the first non-success step (the one that failed
    or was never reached). Returns None if all steps succeeded.
    """
    steps = get_steps(cur, run_id)
    for step in steps:
        if step["STATUS"] not in ("success", "skipped"):
            return step["STEP_NAME"]
    return None


def get_last_run_id(cur, config_id: str) -> str | None:
    """Find the most recent run_id for a given config entry."""
    cur.execute(
        f"SELECT RUN_ID FROM {_TABLE} "
        "WHERE CONFIG_ID = %s "
        "ORDER BY STARTED_AT DESC NULLS LAST "
        "LIMIT 1",
        (config_id,),
    )
    row = cur.fetchone()
    return row[0] if row else None


def can_resume(cur, run_id: str) -> bool:
    """Check if a run has a failed step that can be retried."""
    cur.execute(
        f"SELECT COUNT(*) FROM {_TABLE} "
        "WHERE RUN_ID = %s AND STATUS = 'failed'",
        (run_id,),
    )
    return cur.fetchone()[0] > 0


def get_failed_step_info(cur, run_id: str) -> dict | None:
    """Return details of the failed step for a run."""
    cur.execute(
        f"SELECT * FROM {_TABLE} "
        "WHERE RUN_ID = %s AND STATUS = 'failed' "
        "ORDER BY STEP_ORDER LIMIT 1",
        (run_id,),
    )
    cols = [desc[0] for desc in cur.description]
    row = cur.fetchone()
    return dict(zip(cols, row)) if row else None
