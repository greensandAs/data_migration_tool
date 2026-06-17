"""watermark.py — Watermark / cursor persistence.

Snowflake (MAX of the watermark column in RAW) is the source of truth.
The local histload_config.json holds a cached copy per table:
  * time mode -> last_loaded_at  holds the timestamp cursor.
  * id mode   -> last_loaded_key holds the numeric PK cursor, and last_loaded_at
                 is just a tracking timestamp of the last successful run.
"""
from __future__ import annotations

import json
import os
import tempfile


def update_table_state(config_path: str, source_db: str, source_table: str, *,
                       status: str, last_loaded_at=None, last_loaded_key=None):
    """Persist run status + cursor field(s) for one table (atomic write).

    Only non-None fields are written. Matches on BOTH source_db and source_table
    so tables with the same name in different schemas don't collide.
    """
    with open(config_path) as f:
        cfg = json.load(f)

    for t in cfg["tables"]:
        if t.get("source_db") == source_db and t.get("source_table") == source_table:
            t["last_run_status"] = status
            if last_loaded_at is not None:
                t["last_loaded_at"] = last_loaded_at
            if last_loaded_key is not None:
                t["last_loaded_key"] = last_loaded_key
            break

    d = os.path.dirname(os.path.abspath(config_path)) or "."
    fd, tmp = tempfile.mkstemp(dir=d, suffix=".tmp")
    with os.fdopen(fd, "w") as f:
        json.dump(cfg, f, indent=2)
    os.replace(tmp, config_path)
