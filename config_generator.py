"""config_generator.py — Build histload_config.json from a MySQL schema.

Connection settings are read exclusively from the environment (.env) and are
never written into histload_config.json (no source/snowflake blocks). Only
export_dir + tables are persisted.

Usage:  python config_generator.py <mysql_schema> [out_path]
"""
from __future__ import annotations

import json
import os
import sys

import mysql.connector

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

CONFIG_PATH = "histload_config.json"
# Columns commonly used as an incremental watermark, in preference order.
_WM_CANDIDATES = ("updated_at", "modified_at", "last_modified", "updated_on",
                  "created_at", "created_on")


def _list_tables(cur, schema):
    cur.execute(
        "SELECT TABLE_NAME FROM information_schema.tables "
        "WHERE TABLE_SCHEMA=%s AND TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME",
        (schema,),
    )
    return [r[0] for r in cur.fetchall()]


def _primary_key_cols(cur, schema, table):
    cur.execute(
        "SELECT COLUMN_NAME FROM information_schema.key_column_usage "
        "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND CONSTRAINT_NAME='PRIMARY' "
        "ORDER BY ORDINAL_POSITION",
        (schema, table),
    )
    return [r[0] for r in cur.fetchall()]


def _watermark_col(cur, schema, table):
    cur.execute(
        "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.columns "
        "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s",
        (schema, table),
    )
    cols = {r[0].lower(): str(r[1]).lower() for r in cur.fetchall()}
    for cand in _WM_CANDIDATES:
        if cand in cols and cols[cand] in ("datetime", "timestamp", "date"):
            return cols[cand]  # original-case lookup below
    return None


def build_table_entry(cur, schema, table):
    pk_cols = _primary_key_cols(cur, schema, table)
    wm = _watermark_col(cur, schema, table)
    pk = pk_cols[0].upper() if pk_cols else None
    entry = {
        "source_db": schema,
        "source_table": table,
        "target_table": table.upper(),
        "primary_key": pk,
        "load_type": "incremental" if wm else "full",
        "watermark_col": wm.upper() if wm else None,
        "last_loaded_at": None,
        "partition_col": pk if (pk_cols and len(pk_cols) == 1) else None,
        "partition_num": 8 if pk_cols else 1,
        "reconcile": False,
        "active": True,
        "last_run_status": None,
        "rows_per_file": 1000000,
    }
    if len(pk_cols) > 1:
        entry["merge_keys"] = [c.upper() for c in pk_cols]
    if not pk_cols:
        entry["_review"] = "no primary key — full load only (no incremental MERGE)"
        entry["load_type"] = "full"
    return entry


def main():
    schema = sys.argv[1] if len(sys.argv) > 1 else "test"
    out_path = sys.argv[2] if len(sys.argv) > 2 else CONFIG_PATH

    cfg = None
    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        try:
            with open(out_path) as f:
                cfg = json.load(f)
        except json.JSONDecodeError:
            print(f"WARNING: {out_path} is not valid JSON — rebuilding.")
            cfg = None
    if cfg is None:
        cfg = {}
    cfg.pop("source", None)
    cfg.pop("snowflake", None)
    cfg.setdefault("export_dir", "./export")

    con = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
    )
    cur = con.cursor()
    try:
        tables = _list_tables(cur, schema)
        if not tables:
            print(f"No BASE TABLE found in schema '{schema}'.")
            return 1
        entries = [build_table_entry(cur, schema, t) for t in tables]
    finally:
        cur.close()
        con.close()

    existing = cfg.get("tables", [])
    other_schema = [t for t in existing if t.get("source_db") != schema]
    existing_here = {t["source_table"]: t for t in existing
                     if t.get("source_db") == schema}

    merged, added, kept = list(other_schema), 0, 0
    for e in entries:
        if e["source_table"] in existing_here:
            merged.append(existing_here[e["source_table"]])  # preserve tuning
            kept += 1
        else:
            merged.append(e)
            added += 1
    cfg["tables"] = merged

    d = os.path.dirname(os.path.abspath(out_path)) or "."
    import tempfile
    fd, tmp = tempfile.mkstemp(dir=d, suffix=".tmp")
    with os.fdopen(fd, "w") as f:
        json.dump(cfg, f, indent=2)
    os.replace(tmp, out_path)
    print(f"Config written to {out_path} (schema {schema}: {added} added, {kept} kept)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
