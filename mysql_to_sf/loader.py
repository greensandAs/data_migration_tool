"""loader.py — Snowflake load layer for the 1:1 historical pipeline.

Namespace: each MySQL schema -> a Snowflake DATABASE of the same name with a
single RAW schema:  <MYSQL_SCHEMA>.RAW.<table>. Shared objects (stage, file
formats, RUN_LOG) live in HISTLOAD_DB.META.

FULL (mysqlsh / TSV+zstd):
  PUT data files -> stage, then TRUNCATE + COPY INTO the table using an explicit
  column projection (CSV has no MATCH_BY_COLUMN_NAME, so order matters).

INCREMENTAL (connectorx / Parquet):
  PUT parquet -> stage, COPY INTO a transient temp table (MATCH_BY_COLUMN_NAME),
  then MERGE INTO the table on the (composite) key.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import snowflake.connector

CONTROL_DB = "HISTLOAD_DB"
RAW_SCHEMA = "RAW"
STAGE = "@HISTLOAD_DB.META.HISTLOAD_STAGE"
PARQUET_FMT = "HISTLOAD_DB.META.PARQUET_FMT"
TSV_FMT = "HISTLOAD_DB.META.TSV_ZSTD_FMT"


def get_sf_conn(sf_cfg: dict):
    return snowflake.connector.connect(**sf_cfg)


def target_db(source_db: str) -> str:
    """Snowflake database name for a MySQL schema (1:1, uppercased)."""
    return source_db.strip().upper()


def raw_table(tbl: dict) -> str:
    return f"{target_db(tbl['source_db'])}.{RAW_SCHEMA}.{tbl['target_table']}"


def merge_keys(tbl: dict) -> list:
    """Resolve MERGE/dedupe key(s): merge_keys list if present, else primary_key."""
    keys = tbl.get("merge_keys")
    if keys:
        return [str(k).upper() for k in keys]
    pk = tbl.get("primary_key")
    return [str(pk).upper()] if pk else []


def _stage_path(tbl: dict, subdir: str) -> str:
    return f"{STAGE}/{target_db(tbl['source_db'])}/{tbl['target_table']}/{subdir}"


def stage_path(tbl: dict, subdir: str) -> str:
    """Public accessor for a table's stage subdir path."""
    return _stage_path(tbl, subdir)


def clear_stage_safe(cur, tbl: dict, subdir: str) -> None:
    try:
        cur.execute(f"REMOVE {_stage_path(tbl, subdir)}/")
    except Exception:  # noqa: BLE001
        pass


def put_file(cur, local_file, tbl: dict, subdir: str):
    stage_path = _stage_path(tbl, subdir)
    local = Path(local_file).resolve().as_posix()
    cur.execute(
        f"PUT 'file://{local}' {stage_path}/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
    for row in cur.fetchall():
        print(f"   PUT {row[0]} -> {row[6] if len(row) > 6 else row[-1]}")


DEFAULT_PUT_PARALLEL = 4


def put_files_parallel(sf_cfg: dict, files: list, tbl: dict, subdir: str,
                       max_workers: int = None):
    """PUT multiple files to stage in parallel using separate connections.

    Each thread opens its own Snowflake connection+cursor (connections are not
    thread-safe). Returns the count of files successfully uploaded.
    """
    if not files:
        return 0
    workers = max_workers or int(tbl.get("put_parallel", DEFAULT_PUT_PARALLEL))
    workers = min(workers, len(files))

    if workers <= 1:
        conn = get_sf_conn(sf_cfg)
        cur = conn.cursor()
        try:
            for fp in files:
                put_file(cur, fp, tbl, subdir)
        finally:
            cur.close()
            conn.close()
        return len(files)

    stage = _stage_path(tbl, subdir)
    uploaded = 0
    errors = []

    def _upload_one(local_file):
        conn = get_sf_conn(sf_cfg)
        cur = conn.cursor()
        try:
            local = Path(local_file).resolve().as_posix()
            cur.execute(
                f"PUT 'file://{local}' {stage}/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
            for row in cur.fetchall():
                print(f"   PUT {row[0]} -> {row[6] if len(row) > 6 else row[-1]}")
        finally:
            cur.close()
            conn.close()

    print(f"   parallel PUT: {len(files)} file(s) with {workers} threads")
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_upload_one, fp): fp for fp in files}
        for fut in as_completed(futures):
            try:
                fut.result()
                uploaded += 1
            except Exception as e:  # noqa: BLE001
                errors.append((futures[fut], e))
                print(f"   PUT FAILED {futures[fut]}: {e}")

    if errors:
        raise RuntimeError(
            f"Parallel PUT: {len(errors)}/{len(files)} file(s) failed. "
            f"First error: {errors[0][1]}")
    return uploaded


def copy_into_full(cur, tbl: dict, columns, batch_id: str = None):
    """Full load via TSV data files (explicit column order).

    Audit columns are populated inline: _SRC_FILE = METADATA$FILENAME,
    _BATCH_ID = the run id. _LOAD_TS keeps its CURRENT_TIMESTAMP default.

    Default: TRUNCATE + COPY INTO the live table (brief empty window mid-load).
    With tbl["atomic_full"]=True: COPY into a side table then ALTER TABLE SWAP
    WITH, so readers see the complete old copy until an instant cutover (no gap).
    """
    db = target_db(tbl["source_db"])
    target = raw_table(tbl)
    stage_path = _stage_path(tbl, "full")
    bid = (batch_id or "").replace("'", "''")
    # Project business cols ($1..$n) + audit cols (_SRC_FILE, _BATCH_ID).
    col_list = (", ".join(f'"{name}"' for name, _ in columns)
                + ', "_SRC_FILE", "_BATCH_ID"')
    select_list = (", ".join(f"${i + 1}" for i in range(len(columns)))
                   + f", METADATA$FILENAME, '{bid}'")

    def _copy(into):
        cur.execute(
            f"COPY INTO {into} ({col_list})\n"
            f"FROM (SELECT {select_list} FROM {stage_path}/)\n"
            f"PATTERN = '.*\\.tsv\\.zst'\n"
            f"FILE_FORMAT = (FORMAT_NAME = {TSV_FMT})\n"
            f"ON_ERROR = ABORT_STATEMENT\n"
            f"PURGE = TRUE"
        )
        return _copy_rows_loaded(cur)

    if tbl.get("atomic_full"):
        load_tbl = f"{db}.{RAW_SCHEMA}.{tbl['target_table']}_LOAD"
        print(f"   CREATE {load_tbl} (LIKE target)")
        cur.execute(f"CREATE OR REPLACE TABLE {load_tbl} LIKE {target}")
        print(f"   COPY INTO {load_tbl} (full, TSV)")
        rows = _copy(load_tbl)
        print(f"   SWAP {target} <-> {load_tbl} (atomic cutover)")
        cur.execute(f"ALTER TABLE {target} SWAP WITH {load_tbl}")
        cur.execute(f"DROP TABLE IF EXISTS {load_tbl}")  # old copy
        return rows

    print(f"   TRUNCATE {target}")
    cur.execute(f"TRUNCATE TABLE IF EXISTS {target}")
    print(f"   COPY INTO {target} (full, TSV)")
    return _copy(target)


def copy_into_merge(cur, tbl: dict, batch_id: str = None):
    """COPY parquet into temp table, then MERGE into target on the (composite) key.

    _SRC_FILE is captured during COPY (INCLUDE_METADATA = METADATA$FILENAME) and
    _BATCH_ID is stamped (literal) in the MERGE, on both INSERT and UPDATE.
    """
    db = target_db(tbl["source_db"])
    target = raw_table(tbl)
    tmp = f"{db}.{RAW_SCHEMA}.{tbl['target_table']}_STAGE_TMP"
    stage_path = _stage_path(tbl, "incremental")
    bid = (batch_id or "").replace("'", "''")
    keys = merge_keys(tbl)
    if not keys:
        raise ValueError(
            f"{tbl['source_table']}: incremental MERGE requires primary_key or merge_keys")
    key_set = {k.upper() for k in keys}

    print(f"   CREATE TEMP {tmp}")
    cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE {tmp} LIKE {target}")

    print(f"   COPY INTO {tmp} (parquet)")
    cur.execute(
        f"COPY INTO {tmp}\n"
        f"FROM {stage_path}/\n"
        f"FILE_FORMAT = (FORMAT_NAME = {PARQUET_FMT})\n"
        f"MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE\n"
        f'INCLUDE_METADATA = ("_SRC_FILE" = METADATA$FILENAME)\n'
        f"ON_ERROR = ABORT_STATEMENT\n"
        f"PURGE = TRUE"
    )

    cur.execute(
        f"SELECT COLUMN_NAME FROM {db}.INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION",
        (RAW_SCHEMA, tbl["target_table"]),
    )
    cols = [r[0] for r in cur.fetchall() if not r[0].startswith("_")]
    # Carry audit cols too: _SRC_FILE from the staged file, _BATCH_ID as a literal.
    set_parts = [f't."{c}" = s."{c}"' for c in cols if c.upper() not in key_set]
    set_parts += ['t."_SRC_FILE" = s."_SRC_FILE"', f't."_BATCH_ID" = \'{bid}\'']
    set_clause = ", ".join(set_parts)
    insert_cols = ", ".join(f'"{c}"' for c in cols) + ', "_SRC_FILE", "_BATCH_ID"'
    insert_vals = ", ".join(f's."{c}"' for c in cols) + f', s."_SRC_FILE", \'{bid}\''

    # Dedupe the staged delta to one row per key so the MERGE is deterministic.
    part_by = ", ".join(f'"{k}"' for k in keys)
    wm = tbl.get("watermark_col")
    order_by = f'"{wm}" DESC NULLS LAST' if wm else part_by
    source = (f"(SELECT * FROM {tmp} "
              f"QUALIFY ROW_NUMBER() OVER (PARTITION BY {part_by} "
              f"ORDER BY {order_by}) = 1)")
    on_clause = " AND ".join(f't."{k}" = s."{k}"' for k in keys)

    print(f"   MERGE INTO {target} on {', '.join(keys)}")
    cur.execute(
        f"MERGE INTO {target} t USING {source} s "
        f"ON {on_clause}"
        f" WHEN MATCHED THEN UPDATE SET {set_clause}"
        f" WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )
    return _rows_loaded(cur)


def _rows_loaded(cur) -> int:
    try:
        return int(cur.rowcount) if cur.rowcount and cur.rowcount > 0 else 0
    except Exception:  # noqa: BLE001
        return 0


def _copy_rows_loaded(cur) -> int:
    """Sum the ROWS_LOADED column from a COPY INTO result set."""
    try:
        cols = [d[0].lower() for d in cur.description]
        idx = cols.index("rows_loaded")
        return sum(int(r[idx]) for r in cur.fetchall() if r[idx] is not None)
    except Exception:  # noqa: BLE001
        return _rows_loaded(cur)


def current_max_watermark(cur, tbl: dict, wm_col: str = None):
    """Read MAX(watermark) from the target — Snowflake is the source of truth.

    wm_col overrides tbl["watermark_col"] (e.g. the primary key for id mode).
    Returned as VARCHAR (via TO_VARCHAR) so the connector never converts a large
    TIMESTAMP/NUMBER to a C int, and because it is reused as a string literal in
    the next MySQL query.
    """
    wm = wm_col or tbl.get("watermark_col")
    if not wm:
        return None
    cur.execute(f'SELECT TO_VARCHAR(MAX("{wm}")) FROM {raw_table(tbl)}')
    val = cur.fetchone()[0]
    return str(val) if val is not None else None
