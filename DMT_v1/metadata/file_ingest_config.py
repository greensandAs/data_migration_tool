# CRUD operations for FILE_INGESTION_CONFIG table in Snowflake.
# Co-authored with CoCo
"""file_ingest_config.py — Manage file ingestion job configurations.

Each config defines a cloud-file-to-Snowflake ingestion job: which stage/path,
what pattern to match, what format settings, and where to load in Snowflake.
"""
from __future__ import annotations

import json
from datetime import datetime

_TABLE = "HISTLOAD_DB.META.FILE_INGESTION_CONFIG"
_LOG_TABLE = "HISTLOAD_DB.META.FILE_INGESTION_LOG"


def list_configs(cur, active_only: bool = True) -> list[dict]:
    """Return all file ingestion configs."""
    q = f"SELECT * FROM {_TABLE}"
    if active_only:
        q += " WHERE ACTIVE = TRUE"
    q += " ORDER BY JOB_NAME"
    cur.execute(q)
    cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_config(cur, config_id: str) -> dict | None:
    """Fetch a single config by ID."""
    cur.execute(f"SELECT * FROM {_TABLE} WHERE CONFIG_ID = %s", (config_id,))
    cols = [desc[0] for desc in cur.description]
    row = cur.fetchone()
    return dict(zip(cols, row)) if row else None


def get_config_by_name(cur, job_name: str) -> dict | None:
    """Fetch a single config by job name."""
    cur.execute(f"SELECT * FROM {_TABLE} WHERE JOB_NAME = %s", (job_name,))
    cols = [desc[0] for desc in cur.description]
    row = cur.fetchone()
    return dict(zip(cols, row)) if row else None


def upsert(cur, data: dict) -> str:
    """Insert or update a file ingestion config. Returns CONFIG_ID."""
    config_id = data.get("CONFIG_ID")

    if config_id:
        # Update existing
        sets = []
        vals = []
        skip = {"CONFIG_ID", "CREATED_AT"}
        for key, val in data.items():
            if key in skip:
                continue
            sets.append(f"{key} = %s")
            vals.append(val)
        sets.append("UPDATED_AT = CURRENT_TIMESTAMP()")
        vals.append(config_id)
        cur.execute(
            f"UPDATE {_TABLE} SET {', '.join(sets)} WHERE CONFIG_ID = %s", vals)
        return config_id
    else:
        # Insert new
        cols = [k for k in data.keys() if k != "CONFIG_ID"]
        vals = [data[k] for k in cols]
        placeholders = ["%s"] * len(vals)
        cur.execute(
            f"INSERT INTO {_TABLE} ({', '.join(cols)}) VALUES ({', '.join(placeholders)})",
            vals)
        # Retrieve the generated CONFIG_ID
        cur.execute(f"SELECT CONFIG_ID FROM {_TABLE} WHERE JOB_NAME = %s "
                    "ORDER BY CREATED_AT DESC LIMIT 1", (data.get("JOB_NAME"),))
        row = cur.fetchone()
        return row[0] if row else ""


def delete_config(cur, config_id: str):
    """Hard-delete a file ingestion config."""
    cur.execute(f"DELETE FROM {_TABLE} WHERE CONFIG_ID = %s", (config_id,))


def deactivate(cur, config_id: str):
    """Soft-disable a config."""
    cur.execute(
        f"UPDATE {_TABLE} SET ACTIVE = FALSE, UPDATED_AT = CURRENT_TIMESTAMP() "
        "WHERE CONFIG_ID = %s", (config_id,))


def activate(cur, config_id: str):
    """Re-enable a config."""
    cur.execute(
        f"UPDATE {_TABLE} SET ACTIVE = TRUE, UPDATED_AT = CURRENT_TIMESTAMP() "
        "WHERE CONFIG_ID = %s", (config_id,))


def update_run_status(cur, config_id: str, *,
                      status: str, file_count: int = None,
                      row_count: int = None, error: str = None):
    """Update tracking columns after a run."""
    cur.execute(
        f"UPDATE {_TABLE} SET "
        "LAST_RUN_STATUS = %s, LAST_RUN_AT = CURRENT_TIMESTAMP(), "
        "LAST_FILE_COUNT = %s, LAST_ROW_COUNT = %s, LAST_ERROR = %s, "
        "UPDATED_AT = CURRENT_TIMESTAMP() "
        "WHERE CONFIG_ID = %s",
        (status, file_count, row_count,
         str(error)[:2000] if error else None, config_id))


# ── Log table operations ──────────────────────────────────────────────────────

def write_log(cur, rec: dict):
    """Insert an audit record into FILE_INGESTION_LOG."""
    cols = list(rec.keys())
    vals = [rec[k] for k in cols]
    placeholders = ["%s"] * len(vals)
    cur.execute(
        f"INSERT INTO {_LOG_TABLE} ({', '.join(cols)}) VALUES ({', '.join(placeholders)})",
        vals)


def get_recent_logs(cur, limit: int = 50) -> list[dict]:
    """Return recent ingestion run logs."""
    cur.execute(
        f"SELECT * FROM {_LOG_TABLE} ORDER BY INSERTED_AT DESC LIMIT %s",
        (limit,))
    cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_logs_for_config(cur, config_id: str, limit: int = 20) -> list[dict]:
    """Return logs for a specific config."""
    cur.execute(
        f"SELECT * FROM {_LOG_TABLE} WHERE CONFIG_ID = %s "
        "ORDER BY INSERTED_AT DESC LIMIT %s",
        (config_id, limit))
    cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]
