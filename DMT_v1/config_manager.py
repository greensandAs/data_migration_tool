# CRUD operations for MIGRATION_CONFIG table in Snowflake.
"""config_manager.py — Read/write migration config from Snowflake tables.

Replaces the local histload_config.json pattern. All state lives in
HISTLOAD_DB.META.MIGRATION_CONFIG so it survives container restarts,
supports multi-user access, and is auditable via Time Travel.
"""
from __future__ import annotations

from datetime import datetime


_TABLE = "HISTLOAD_DB.META.MIGRATION_CONFIG"


def list_active(cur, connection_profile: str | None = None) -> list[dict]:
    """Return all active table configs, optionally filtered by profile."""
    q = f"SELECT * FROM {_TABLE} WHERE ACTIVE = TRUE"
    params = []
    if connection_profile:
        q += " AND CONNECTION_PROFILE = %s"
        params.append(connection_profile)
    q += " ORDER BY SOURCE_DB, SOURCE_TABLE"
    cur.execute(q, params)
    cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def list_all(cur, connection_profile: str | None = None) -> list[dict]:
    """Return all table configs regardless of active status."""
    q = f"SELECT * FROM {_TABLE}"
    params = []
    if connection_profile:
        q += " WHERE CONNECTION_PROFILE = %s"
        params.append(connection_profile)
    q += " ORDER BY SOURCE_DB, SOURCE_TABLE"
    cur.execute(q, params)
    cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_by_id(cur, config_id: str) -> dict | None:
    """Fetch a single config entry by CONFIG_ID."""
    cur.execute(f"SELECT * FROM {_TABLE} WHERE CONFIG_ID = %s", (config_id,))
    cols = [desc[0] for desc in cur.description]
    row = cur.fetchone()
    return dict(zip(cols, row)) if row else None


def get_by_table(cur, connection_profile: str, source_db: str,
                 source_table: str) -> dict | None:
    """Fetch config by the natural key (profile + source_db + source_table)."""
    cur.execute(
        f"SELECT * FROM {_TABLE} "
        "WHERE CONNECTION_PROFILE = %s AND SOURCE_DB = %s AND SOURCE_TABLE = %s",
        (connection_profile, source_db, source_table),
    )
    cols = [desc[0] for desc in cur.description]
    row = cur.fetchone()
    return dict(zip(cols, row)) if row else None


def upsert(cur, entry: dict) -> str:
    """Insert or update a migration config entry. Returns the CONFIG_ID."""
    existing = None
    if entry.get("CONFIG_ID"):
        existing = get_by_id(cur, entry["CONFIG_ID"])
    if not existing and entry.get("CONNECTION_PROFILE") and entry.get("SOURCE_DB"):
        existing = get_by_table(
            cur, entry["CONNECTION_PROFILE"],
            entry["SOURCE_DB"], entry.get("SOURCE_TABLE", ""))

    if existing:
        return _update(cur, existing["CONFIG_ID"], entry)
    else:
        return _insert(cur, entry)


def _insert(cur, entry: dict) -> str:
    """Insert a new config entry."""
    # MERGE_KEYS is an ARRAY column — use ARRAY_CONSTRUCT (not PARSE_JSON)
    merge_keys = entry.get("MERGE_KEYS")
    if merge_keys and isinstance(merge_keys, (list, tuple)):
        items = ", ".join(f"'{v}'" for v in merge_keys)
        merge_keys_sql = f"ARRAY_CONSTRUCT({items})"
    else:
        merge_keys_sql = "NULL"

    cur.execute(
        f"""INSERT INTO {_TABLE}
            (CONNECTION_PROFILE, SOURCE_DB, SOURCE_TABLE, TARGET_DB, TARGET_TABLE,
             LOAD_TYPE, WATERMARK_COL, WATERMARK_TYPE, PRIMARY_KEY, MERGE_KEYS,
             PARTITION_COL, PARTITION_NUM, ROWS_PER_FILE,
             STORAGE_TYPE, STORAGE_PATH, STORAGE_CREDENTIALS,
             EXECUTION_MODE, RECONCILE, ACTIVE, NOTES)
        SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, {merge_keys_sql}, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        """,
        (
            entry.get("CONNECTION_PROFILE"),
            entry.get("SOURCE_DB"),
            entry.get("SOURCE_TABLE"),
            entry.get("TARGET_DB"),
            entry.get("TARGET_TABLE"),
            entry.get("LOAD_TYPE", "full"),
            entry.get("WATERMARK_COL"),
            entry.get("WATERMARK_TYPE"),
            entry.get("PRIMARY_KEY"),
            entry.get("PARTITION_COL"),
            entry.get("PARTITION_NUM", 8),
            entry.get("ROWS_PER_FILE", 1000000),
            entry.get("STORAGE_TYPE", "internal_stage"),
            entry.get("STORAGE_PATH"),
            entry.get("STORAGE_CREDENTIALS"),
            entry.get("EXECUTION_MODE", "FULL"),
            entry.get("RECONCILE", False),
            entry.get("ACTIVE", False),
            entry.get("NOTES"),
        ),
    )
    # Retrieve the generated CONFIG_ID
    cur.execute(
        f"SELECT CONFIG_ID FROM {_TABLE} "
        "WHERE CONNECTION_PROFILE = %s AND SOURCE_DB = %s AND SOURCE_TABLE = %s",
        (entry["CONNECTION_PROFILE"], entry["SOURCE_DB"], entry["SOURCE_TABLE"]),
    )
    return cur.fetchone()[0]


def _update(cur, config_id: str, entry: dict) -> str:
    """Update an existing config entry (only non-None fields)."""
    import json as _json
    sets = []
    vals = []
    updatable = [
        "TARGET_DB", "TARGET_TABLE", "TARGET_SCHEMA", "LOAD_TYPE", "WATERMARK_COL", "WATERMARK_TYPE",
        "PRIMARY_KEY", "MERGE_KEYS", "PARTITION_COL", "PARTITION_NUM", "ROWS_PER_FILE",
        "STORAGE_TYPE", "STORAGE_PATH", "STORAGE_CREDENTIALS", "EXECUTION_MODE",
        "RECONCILE", "ACTIVE", "NOTES", "SCD_TYPE", "FILTER_CONDITION",
    ]
    merge_keys_sql = None
    # Fields that can be explicitly cleared (set to NULL)
    nullable_fields = {"FILTER_CONDITION", "STORAGE_PATH", "STORAGE_CREDENTIALS",
                       "WATERMARK_COL", "WATERMARK_TYPE", "PRIMARY_KEY", "NOTES",
                       "TARGET_SCHEMA"}
    for col in updatable:
        if col in entry:
            val = entry[col]
            # Skip None values unless the field is explicitly clearable
            if val is None:
                if col in nullable_fields:
                    sets.append(f"{col} = NULL")
                continue
            if col == "MERGE_KEYS":
                # ARRAY column — use ARRAY_CONSTRUCT with literal strings
                mk = entry[col]
                if isinstance(mk, (list, tuple)) and mk:
                    items = ", ".join(f"'{v}'" for v in mk)
                    sets.append(f"MERGE_KEYS = ARRAY_CONSTRUCT({items})")
                # Don't add to vals — handled inline
            else:
                sets.append(f"{col} = %s")
                vals.append(entry[col])

    if not sets:
        return config_id

    sets.append("UPDATED_AT = CURRENT_TIMESTAMP()")
    vals.append(config_id)
    cur.execute(
        f"UPDATE {_TABLE} SET {', '.join(sets)} WHERE CONFIG_ID = %s", vals)
    return config_id


def update_watermark(cur, config_id: str, *, status: str,
                     last_loaded_at=None, last_loaded_key=None,
                     last_run_id: str = None, last_failed_step: str = None):
    """Update run state + cursor after a pipeline execution."""
    sets = ["LAST_RUN_STATUS = %s", "UPDATED_AT = CURRENT_TIMESTAMP()"]
    vals = [status]

    if last_loaded_at is not None:
        sets.append("LAST_LOADED_AT = %s")
        vals.append(last_loaded_at)
    if last_loaded_key is not None:
        sets.append("LAST_LOADED_KEY = %s")
        vals.append(str(last_loaded_key))
    if last_run_id is not None:
        sets.append("LAST_RUN_ID = %s")
        vals.append(last_run_id)
    if last_failed_step is not None:
        sets.append("LAST_FAILED_STEP = %s")
        vals.append(last_failed_step)
    elif status == "success":
        sets.append("LAST_FAILED_STEP = NULL")

    vals.append(config_id)
    cur.execute(
        f"UPDATE {_TABLE} SET {', '.join(sets)} WHERE CONFIG_ID = %s", vals)


def delete_config(cur, config_id: str):
    """Hard-delete a config entry."""
    cur.execute(f"DELETE FROM {_TABLE} WHERE CONFIG_ID = %s", (config_id,))


def activate(cur, config_id: str):
    """Mark a table config as active."""
    cur.execute(
        f"UPDATE {_TABLE} SET ACTIVE = TRUE, UPDATED_AT = CURRENT_TIMESTAMP() "
        "WHERE CONFIG_ID = %s", (config_id,))


def deactivate(cur, config_id: str):
    """Mark a table config as inactive."""
    cur.execute(
        f"UPDATE {_TABLE} SET ACTIVE = FALSE, UPDATED_AT = CURRENT_TIMESTAMP() "
        "WHERE CONFIG_ID = %s", (config_id,))
