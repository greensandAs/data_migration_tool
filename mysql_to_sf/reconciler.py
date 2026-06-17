"""reconciler.py — Delete reconciliation (soft-delete), Snowflake-side anti-join.

Instead of pulling the entire key set of both MySQL and Snowflake into memory and
diffing in Python (OOM risk on large tables), this:

  1. Extracts ONLY the (composite) key columns from MySQL via connectorx -> parquet
     (key columns are tiny vs full rows).
  2. PUTs + COPYs them into a transient Snowflake table with the same key types.
  3. Soft-deletes RAW rows whose key is NOT in that set, via a single set-based
     UPDATE ... WHERE NOT EXISTS — no per-key Python objects, memory bounded.

Invoked via `python orchestrator.py --reconcile`. Run on its own cadence — it
scans the full key set. Tables without a key are skipped.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import connectorx as cx
import pyarrow.parquet as pq

import extractor_incremental
import loader

# MySQL types whose key values transit losslessly as text (see extractor notes).
_SF_MAX_NUMERIC_PRECISION = 38


def _key_projection(mysql_conn, tbl: dict, keys: list) -> str:
    """SELECT list for the key columns, normalized the same way as the main load
    (decimal>38 -> CHAR, datetime/timestamp -> session-local string) and aliased
    UPPERCASE so they match the RAW column names on COPY (MATCH_BY_COLUMN_NAME)."""
    cur = mysql_conn.cursor()
    cur.execute(
        "SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION "
        "FROM information_schema.columns "
        "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s",
        (tbl["source_db"], tbl["source_table"]),
    )
    types = {r[0].upper(): (str(r[1]).lower(), r[2]) for r in cur.fetchall()}
    cur.close()

    parts = []
    for k in keys:
        dtype, prec = types.get(k.upper(), ("", None))
        if dtype in ("decimal", "numeric") and prec and int(prec) > _SF_MAX_NUMERIC_PRECISION:
            parts.append(f"CAST(`{k}` AS CHAR) AS `{k.upper()}`")
        elif dtype in ("datetime", "timestamp"):
            parts.append(f"DATE_FORMAT(`{k}`, '%Y-%m-%d %H:%i:%s.%f') AS `{k.upper()}`")
        else:
            parts.append(f"`{k}` AS `{k.upper()}`")
    return ", ".join(parts)


def reconcile_table(cur, mysql_conn, tbl: dict, src_cfg: dict, export_dir: str,
                    sf_cfg: dict = None) -> dict:
    """Soft-delete RAW rows whose (composite) key no longer exists in MySQL.

    Returns {"deleted": n, "skipped": reason|None}.
    """
    keys = loader.merge_keys(tbl)
    if not keys:
        return {"deleted": 0, "skipped": "no primary key"}

    db = loader.target_db(tbl["source_db"])
    target = loader.raw_table(tbl)
    keys_tbl = f"{db}.RAW.{tbl['target_table']}_RECON_KEYS"
    key_cols = ", ".join(f'"{k}"' for k in keys)

    # 1. Extract MySQL key set (key columns only) -> parquet file(s).
    out_dir = Path(export_dir) / tbl["source_table"] / "reconcile"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    proj = _key_projection(mysql_conn, tbl, keys)
    uri = extractor_incremental._mysql_uri(src_cfg, tbl["source_db"])
    query = f"SELECT {proj} FROM `{tbl['source_db']}`.`{tbl['source_table']}`"
    print(f"   key query: {query}")
    arrow = cx.read_sql(uri, query, return_type="arrow")
    print(f"   MySQL keys: {arrow.num_rows:,}")

    rows_per_file = int(tbl.get("rows_per_file", 1_000_000) or 0)
    files = []
    if rows_per_file and arrow.num_rows > rows_per_file:
        import pyarrow as pa
        for i, batch in enumerate(arrow.to_batches(max_chunksize=rows_per_file)):
            fp = out_dir / f"keys_{stamp}_part{i:04d}.parquet"
            pq.write_table(pa.Table.from_batches([batch]), fp, compression="snappy")
            files.append(fp)
    else:
        fp = out_dir / f"keys_{stamp}.parquet"
        pq.write_table(arrow, fp, compression="snappy")
        files.append(fp)

    # 2. Transient keys table with the SAME key column types as RAW, then COPY.
    cur.execute(
        f"CREATE OR REPLACE TEMPORARY TABLE {keys_tbl} AS "
        f"SELECT {key_cols} FROM {target} WHERE 1=0")
    loader.clear_stage_safe(cur, tbl, "reconcile")
    if sf_cfg and len(files) > 1:
        loader.put_files_parallel(sf_cfg, files, tbl, "reconcile")
    else:
        for fp in files:
            loader.put_file(cur, fp, tbl, "reconcile")
    cur.execute(
        f"COPY INTO {keys_tbl}\n"
        f"FROM {loader.stage_path(tbl, 'reconcile')}/\n"
        f"FILE_FORMAT = (FORMAT_NAME = {loader.PARQUET_FMT})\n"
        f"MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE\n"
        f"ON_ERROR = ABORT_STATEMENT\n"
        f"PURGE = TRUE"
    )

    # 3. Set-based soft-delete via anti-join (no per-key Python objects).
    on_clause = " AND ".join(f't."{k}" = k."{k}"' for k in keys)
    cur.execute(
        f'UPDATE {target} t '
        f'SET "_IS_DELETED" = TRUE, "_DELETED_AT" = CURRENT_TIMESTAMP() '
        f'WHERE COALESCE(t."_IS_DELETED", FALSE) = FALSE '
        f'AND NOT EXISTS (SELECT 1 FROM {keys_tbl} k WHERE {on_clause})'
    )
    deleted = cur.rowcount or 0

    # Cleanup: drop the transient table + local key files.
    try:
        cur.execute(f"DROP TABLE IF EXISTS {keys_tbl}")
    except Exception:  # noqa: BLE001
        pass
    try:
        import shutil
        shutil.rmtree(out_dir, ignore_errors=True)
    except Exception:  # noqa: BLE001
        pass

    return {"deleted": deleted, "skipped": None}
