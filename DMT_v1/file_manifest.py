# File manifest tracking for decoupled extract/load workflows.
# Co-authored with CoCo
"""file_manifest.py — Register and query extracted files in Snowflake.

The FILE_MANIFEST table tracks every file produced by extractors. The loader
reads the manifest to find files ready for ingestion — enabling extract and
load to run at different times or even from different machines.
"""
from __future__ import annotations

import json

_TABLE = "HISTLOAD_DB.META.FILE_MANIFEST"


def register_file(cur, *, run_id: str, config_id: str, source_db: str,
                  source_table: str, file_path: str, storage_type: str,
                  file_format: str = "parquet", file_size_bytes: int = None,
                  row_count: int = None, part_number: int = None):
    """Register a newly extracted file in the manifest."""
    cur.execute(
        f"""INSERT INTO {_TABLE}
            (RUN_ID, CONFIG_ID, SOURCE_DB, SOURCE_TABLE, FILE_PATH,
             STORAGE_TYPE, FILE_FORMAT, FILE_SIZE_BYTES, ROW_COUNT, PART_NUMBER)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (run_id, config_id, source_db, source_table, file_path,
         storage_type, file_format, file_size_bytes, row_count, part_number),
    )


def register_files(cur, *, run_id: str, config_id: str, source_db: str,
                   source_table: str, files: list[dict]):
    """Bulk-register multiple files. Each dict needs: file_path, storage_type, and
    optionally file_format, file_size_bytes, row_count, part_number."""
    for i, f in enumerate(files):
        register_file(
            cur, run_id=run_id, config_id=config_id,
            source_db=source_db, source_table=source_table,
            file_path=f["file_path"],
            storage_type=f.get("storage_type", "local"),
            file_format=f.get("file_format", "parquet"),
            file_size_bytes=f.get("file_size_bytes"),
            row_count=f.get("row_count"),
            part_number=f.get("part_number", i),
        )


def mark_uploaded(cur, manifest_id: str):
    """Mark a file as uploaded to cloud/stage."""
    cur.execute(
        f"UPDATE {_TABLE} SET STATUS = 'uploaded', "
        "UPLOADED_AT = CURRENT_TIMESTAMP() "
        "WHERE MANIFEST_ID = %s", (manifest_id,))


def mark_loaded(cur, manifest_id: str):
    """Mark a file as consumed by the loader (COPY INTO)."""
    cur.execute(
        f"UPDATE {_TABLE} SET STATUS = 'loaded', "
        "LOADED_AT = CURRENT_TIMESTAMP() "
        "WHERE MANIFEST_ID = %s", (manifest_id,))


def mark_failed(cur, manifest_id: str):
    """Mark a file as failed during load."""
    cur.execute(
        f"UPDATE {_TABLE} SET STATUS = 'failed' "
        "WHERE MANIFEST_ID = %s", (manifest_id,))


def get_files_for_run(cur, run_id: str, config_id: str) -> list[dict]:
    """Return all files for a specific run + config."""
    cur.execute(
        f"SELECT * FROM {_TABLE} "
        "WHERE RUN_ID = %s AND CONFIG_ID = %s "
        "ORDER BY PART_NUMBER",
        (run_id, config_id),
    )
    cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_pending_files(cur, config_id: str) -> list[dict]:
    """Return files that have been extracted/uploaded but not yet loaded.
    Used by LOAD_ONLY mode to pick up files from a prior extract."""
    cur.execute(
        f"SELECT * FROM {_TABLE} "
        "WHERE CONFIG_ID = %s AND STATUS IN ('extracted', 'uploaded') "
        "ORDER BY RUN_ID DESC, PART_NUMBER",
        (config_id,),
    )
    cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_latest_run_files(cur, config_id: str) -> list[dict]:
    """Return files from the most recent run for a config."""
    cur.execute(
        f"SELECT * FROM {_TABLE} "
        "WHERE CONFIG_ID = %s AND RUN_ID = ("
        f"  SELECT MAX(RUN_ID) FROM {_TABLE} WHERE CONFIG_ID = %s"
        ") ORDER BY PART_NUMBER",
        (config_id, config_id),
    )
    cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def cleanup_old_manifests(cur, config_id: str, keep_runs: int = 5):
    """Delete manifest entries older than the last N runs (housekeeping)."""
    cur.execute(
        f"""DELETE FROM {_TABLE}
        WHERE CONFIG_ID = %s
          AND RUN_ID NOT IN (
              SELECT DISTINCT RUN_ID FROM {_TABLE}
              WHERE CONFIG_ID = %s
              ORDER BY RUN_ID DESC
              LIMIT %s
          )
        """,
        (config_id, config_id, keep_runs),
    )
