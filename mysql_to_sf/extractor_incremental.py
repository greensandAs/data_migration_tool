"""extractor_incremental.py — Incremental engine using connectorx.

Reads only rows in the watermark window (last_loaded_at, source_now - lag] via a
single SQL query, returns Arrow, and writes Snappy Parquet. Optional partitioned
parallel reads for large windows (integer partition column only).
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote_plus

import connectorx as cx
import mysql.connector
import pyarrow as pa
import pyarrow.parquet as pq

LAG_MINUTES = 1  # exclude in-flight rows near "now"
DEFAULT_ROWS_PER_FILE = 1_000_000

# MySQL DECIMAL allows precision up to 65, but Snowflake NUMBER maxes at 38 and
# connectorx's Arrow decimal128 also caps at 38. Columns wider than this are
# read as text (CAST AS CHAR) so they land losslessly in a VARCHAR column.
SF_MAX_NUMERIC_PRECISION = 38

# Integer column types eligible for connectorx partitioned (parallel) reads.
_INT_TYPES = {"tinyint", "smallint", "mediumint", "int", "integer", "bigint"}


def _mysql_uri(src_cfg: dict, db: str) -> str:
    return (f'mysql://{quote_plus(src_cfg["user"])}:{quote_plus(src_cfg["password"])}'
            f'@{src_cfg["host"]}:{src_cfg["port"]}/{db}')


def _is_int_column(mysql_conn, db: str, table: str, col: str) -> bool:
    """True only if `col` is an integer type (safe for connectorx partition_on)."""
    try:
        cur = mysql_conn.cursor()
        cur.execute(
            "SELECT DATA_TYPE FROM information_schema.columns "
            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND UPPER(COLUMN_NAME)=UPPER(%s)",
            (db, table, col),
        )
        row = cur.fetchone()
        cur.close()
    except Exception:  # noqa: BLE001 — on doubt, don't partition
        return False
    return bool(row) and str(row[0]).lower() in _INT_TYPES


def _select_list(mysql_conn, db: str, table: str) -> str:
    """Explicit column projection for the incremental read.

      * decimal/numeric wider than Snowflake's 38 precision -> CAST AS CHAR.
      * datetime/timestamp -> DATE_FORMAT to a session-local string so connectorx
        does NOT implicitly convert a MySQL TIMESTAMP to UTC (keeps the watermark
        in one timezone end-to-end).

    Returns "*" if column metadata can't be read.
    """
    try:
        cur = mysql_conn.cursor()
        cur.execute(
            "SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION "
            "FROM information_schema.columns "
            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION",
            (db, table),
        )
        rows = cur.fetchall()
        cur.close()
    except Exception as e:  # noqa: BLE001
        print(f"   (column probe failed, using SELECT *: {e})")
        return "*"

    if not rows:
        return "*"
    parts = []
    for name, dtype, prec in rows:
        dt = str(dtype).lower()
        if (dt in ("decimal", "numeric")
                and prec is not None and int(prec) > SF_MAX_NUMERIC_PRECISION):
            parts.append(f"CAST(`{name}` AS CHAR) AS `{name}`")
        elif dt in ("datetime", "timestamp"):
            parts.append(
                f"DATE_FORMAT(`{name}`, '%Y-%m-%d %H:%i:%s.%f') AS `{name}`")
        else:
            parts.append(f"`{name}`")
    return ", ".join(parts)


def _source_ceiling(mysql_conn) -> str:
    """Window upper bound (wm_to) in the SOURCE database's clock (NOW() - lag)."""
    try:
        cur = mysql_conn.cursor()
        cur.execute("SELECT NOW()")
        now = cur.fetchone()[0]
        cur.close()
    except Exception as e:  # noqa: BLE001
        print(f"   (source clock probe failed, using host clock: {e})")
        now = datetime.now()
    return (now - timedelta(minutes=LAG_MINUTES)).strftime("%Y-%m-%d %H:%M:%S")


def _numeric_ceiling(mysql_conn, db: str, table: str, col: str):
    """Current MAX(col) in the source as a numeric string (None if empty)."""
    try:
        cur = mysql_conn.cursor()
        cur.execute(f"SELECT MAX(`{col}`) FROM `{db}`.`{table}`")
        v = cur.fetchone()[0]
        cur.close()
    except Exception as e:  # noqa: BLE001
        print(f"   (max-id probe failed: {e})")
        return None
    return str(v) if v is not None else None


def resolve_cursor(tbl: dict, mysql_conn):
    """Return (cursor_col, wm_type) for an incremental load.

    cursor_col : the column tracked as the watermark — `watermark_col` if set,
                 else the single `primary_key` (id-style fallback).
    wm_type    : explicit tbl['watermark_type'] ('id'|'time') wins; otherwise
                 auto — 'id' when the cursor column is an integer, else 'time'.

    This lets a table be configured for id mode with just primary_key +
    watermark_type='id' (no separate watermark_col needed).
    """
    explicit = tbl.get("watermark_type")
    wm_col = tbl.get("watermark_col")
    pk = tbl.get("primary_key")
    if explicit == "id":
        col = wm_col or pk
        return (col.upper() if col else None), "id"
    if explicit == "time":
        return (wm_col.upper() if wm_col else None), "time"
    # Auto-detect from the column type.
    if wm_col:
        wt = ("id" if _is_int_column(mysql_conn, tbl["source_db"],
                                     tbl["source_table"], wm_col) else "time")
        return wm_col.upper(), wt
    if pk and _is_int_column(mysql_conn, tbl["source_db"], tbl["source_table"], pk):
        return pk.upper(), "id"
    return None, "time"


def watermark_type(tbl: dict, mysql_conn) -> str:
    """Backward-compatible: just the wm_type from resolve_cursor()."""
    return resolve_cursor(tbl, mysql_conn)[1]


def extract_incremental_connectorx(tbl: dict, src_cfg: dict, export_dir: str, mysql_conn):
    """Extract the incremental delta for one table.

    Returns (parquet_files, row_count, watermark_to).

    Two watermark modes:
      * time : WHERE wm > last AND wm <= NOW()-lag  (captures inserts + updates
               that bump the timestamp column).
      * id   : WHERE pk > last AND pk <= MAX(pk)     (monotonic integer key —
               captures INSERTS ONLY; updates to existing rows are not seen).
    """
    out_dir = Path(export_dir) / tbl["source_table"] / "incremental"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    wm_col, wm_type = resolve_cursor(tbl, mysql_conn)
    if not wm_col:
        raise ValueError(
            f"{tbl['source_table']}: incremental needs watermark_col or primary_key")
    # id mode reads/writes the numeric cursor in last_loaded_key; time mode uses
    # last_loaded_at (a timestamp cursor).
    wm_from = (tbl.get("last_loaded_key") if wm_type == "id"
               else tbl.get("last_loaded_at"))

    if wm_type == "id":
        wm_to = _numeric_ceiling(mysql_conn, tbl["source_db"], tbl["source_table"], wm_col)
        print("   watermark mode: id (integer key — INSERTS only, no updates)")
        if wm_to is None:
            print("   source table empty — skipping.")
            return [], 0, wm_from
        # Numeric comparison (no quotes); high bound = current MAX(pk).
        if wm_from:
            where = f"`{wm_col}` > {wm_from} AND `{wm_col}` <= {wm_to}"
        else:
            where = f"`{wm_col}` <= {wm_to}"
    else:
        wm_to = _source_ceiling(mysql_conn)
        if wm_from:
            where = (f"`{wm_col}` > '{wm_from}' AND `{wm_col}` <= '{wm_to}'")
        else:
            where = "1=1"  # first incremental run -> pull everything up to wm_to
            if wm_col:
                where = f"`{wm_col}` <= '{wm_to}'"

    select_list = _select_list(mysql_conn, tbl["source_db"], tbl["source_table"])
    query = (f"SELECT {select_list} FROM `{tbl['source_db']}`.`{tbl['source_table']}` "
             f"WHERE {where}")
    print(f"   query: {query}")

    uri = _mysql_uri(src_cfg, tbl["source_db"])

    read_kwargs = {"return_type": "arrow"}
    # Partitioned parallel read only when partition_col is an INTEGER column.
    pcol = tbl.get("partition_col")
    if pcol and int(tbl.get("partition_num", 1)) > 1:
        if _is_int_column(mysql_conn, tbl["source_db"], tbl["source_table"], pcol):
            read_kwargs["partition_on"] = pcol
            read_kwargs["partition_num"] = int(tbl["partition_num"])
        else:
            print(f"   partition skipped: '{pcol}' is not an integer column "
                  f"(single-threaded read)")

    arrow_table = cx.read_sql(uri, query, **read_kwargs)
    rows = arrow_table.num_rows
    if rows == 0:
        print(f"   no new rows since {wm_from} — skipping.")
        return [], 0, wm_to

    rows_per_file = int(tbl.get("rows_per_file", DEFAULT_ROWS_PER_FILE) or 0)
    files = []
    if rows_per_file and rows > rows_per_file:
        for i, batch in enumerate(arrow_table.to_batches(max_chunksize=rows_per_file)):
            part = out_dir / f"{tbl['source_table']}_{stamp}_part{i:04d}.parquet"
            pq.write_table(pa.Table.from_batches([batch]), part, compression="snappy")
            files.append(part)
        print(f"   extracted {rows:,} rows -> {len(files)} parquet files "
              f"(~{rows_per_file:,}/file)")
    else:
        out_file = out_dir / f"{tbl['source_table']}_{stamp}.parquet"
        pq.write_table(arrow_table, out_file, compression="snappy")
        files.append(out_file)
        print(f"   extracted {rows:,} rows -> {out_file.name}")

    return files, rows, wm_to
