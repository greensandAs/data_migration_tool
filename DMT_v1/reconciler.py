# Soft-delete reconciliation via Snowflake-side anti-join.
# Co-authored with CoCo
"""reconciler.py — Delete reconciliation (soft-delete) via anti-join.

Extracts ONLY the key columns from MySQL, loads them into a transient table,
then soft-deletes RAW rows whose key is NOT in that set — all set-based in
Snowflake (no per-key Python objects, memory bounded).
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import connectorx as cx
import pyarrow.parquet as pq

import loader
from ddl_generators import RAW_SCHEMA, target_db
from extractors.mysql_incremental import MySQLIncrementalExtractor


def reconcile_table(cur, mysql_conn, config: dict, src_cfg: dict,
                    sf_cfg: dict = None, export_dir: str = "./export") -> dict:
    """Soft-delete RAW rows whose key no longer exists in MySQL.

    Returns {"deleted": n, "skipped": reason|None}.
    """
    keys = loader.merge_keys(config)
    if not keys:
        return {"deleted": 0, "skipped": "no primary key"}

    db = config.get("TARGET_DB") or target_db(config["SOURCE_DB"])
    fqn = loader.raw_table(config)
    keys_tbl = f"{db}.{RAW_SCHEMA}.{config.get('TARGET_TABLE', config['SOURCE_TABLE'].upper())}_RECON_KEYS"
    key_cols = ", ".join(f'"{k}"' for k in keys)

    # 1. Extract MySQL key set -> Parquet
    out_dir = Path(export_dir) / config["SOURCE_TABLE"] / "reconcile"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    inc = MySQLIncrementalExtractor()
    uri = inc._mysql_uri(src_cfg, config["SOURCE_DB"])
    key_select = ", ".join(f"`{k}` AS `{k.upper()}`" for k in keys)
    query = f"SELECT {key_select} FROM `{config['SOURCE_DB']}`.`{config['SOURCE_TABLE']}`"
    print(f"   reconcile key query: {query[:100]}...")

    arrow = cx.read_sql(uri, query, return_type="arrow")
    print(f"   MySQL keys: {arrow.num_rows:,}")

    fp = out_dir / f"keys_{stamp}.parquet"
    pq.write_table(arrow, fp, compression="snappy")

    # 2. Load keys into transient table
    cur.execute(
        f"CREATE OR REPLACE TEMPORARY TABLE {keys_tbl} AS "
        f"SELECT {key_cols} FROM {fqn} WHERE 1=0")
    loader.clear_stage_safe(cur, config, "reconcile")
    loader.put_file(cur, fp, config, "reconcile")
    cur.execute(
        f"COPY INTO {keys_tbl}\n"
        f"FROM '{loader.stage_path(config, 'reconcile')}/'\n"
        f"FILE_FORMAT = (FORMAT_NAME = {loader.PARQUET_FMT})\n"
        f"MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE\n"
        f"ON_ERROR = ABORT_STATEMENT\n"
        f"PURGE = TRUE"
    )

    # 3. Set-based soft-delete via anti-join
    on_clause = " AND ".join(f't."{k}" = k."{k}"' for k in keys)
    cur.execute(
        f'UPDATE {fqn} t '
        f'SET "_IS_DELETED" = TRUE, "_DELETED_AT" = CURRENT_TIMESTAMP() '
        f'WHERE COALESCE(t."_IS_DELETED", FALSE) = FALSE '
        f'AND NOT EXISTS (SELECT 1 FROM {keys_tbl} k WHERE {on_clause})'
    )
    deleted = cur.rowcount or 0

    # Cleanup
    try:
        cur.execute(f"DROP TABLE IF EXISTS {keys_tbl}")
    except Exception:
        pass
    try:
        import shutil
        shutil.rmtree(out_dir, ignore_errors=True)
    except Exception:
        pass

    return {"deleted": deleted, "skipped": None}
