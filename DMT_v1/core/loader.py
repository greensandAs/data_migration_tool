# Shared Snowflake COPY INTO and MERGE logic for all source types.
"""loader.py — Snowflake COPY INTO + MERGE logic (shared across all sources).

Handles:
  - Full loads: TRUNCATE + COPY INTO (or atomic CREATE+SWAP)
  - Incremental loads: COPY INTO staging + MERGE into RAW
  - Parallel PUT via separate connections
  - Watermark retrieval from RAW (source of truth)
"""
from __future__ import annotations

from pathlib import Path

import snowflake.connector

from ddl_generators import RAW_SCHEMA, AUDIT_COLS, target_db

PARQUET_FMT = "HISTLOAD_DB.META.PARQUET_FMT"
TSV_ZSTD_FMT = "HISTLOAD_DB.META.TSV_ZSTD_FMT"
CSV_FMT = "HISTLOAD_DB.META.CSV_FMT"
STAGE = "HISTLOAD_DB.META.DMT_STAGE"
EXT_S3_STAGE = "HISTLOAD_DB.META.DMT_EXT_S3"
EXT_AZURE_STAGE = "HISTLOAD_DB.META.DMT_EXT_AZURE"


def ext_stage_path(config: dict, sub: str, source_type: str = "mysql") -> str:
    """External stage path for a table's files.

    STORAGE_PATH stores the stage name (e.g., 'DMT_EXT_S3').
    Returns: @HISTLOAD_DB.META.<stage_name>/dmt/<source_type>/<connection>/<schema>/<table>/<sub>/
    """
    stage_name = config.get("STORAGE_PATH") or ""
    if not stage_name:
        # Fallback to default stage names
        storage_type = config.get("STORAGE_TYPE", "internal_stage")
        if storage_type == "s3":
            stage_name = "DMT_EXT_S3"
        elif storage_type == "azure":
            stage_name = "DMT_EXT_AZURE"
        else:
            stage_name = "DMT_STAGE"

    # Build fully qualified stage reference
    fq_stage = f"HISTLOAD_DB.META.{stage_name}" if "." not in stage_name else stage_name

    # The upload path structure: dmt/<source_type>/<connection>/<schema>/<table>/<sub>/
    conn_name = config.get("CONNECTION_PROFILE", "default")
    path = f"@{fq_stage}/dmt/{source_type}/{conn_name}/{config['SOURCE_DB']}/{config['SOURCE_TABLE']}/{sub}"
    return path


def get_sf_conn(sf_cfg: dict):
    """Create a Snowflake connection from config dict."""
    return snowflake.connector.connect(**sf_cfg)


def raw_table(config: dict) -> str:
    """Fully qualified RAW table name.

    For Teradata: uses TARGET_SCHEMA (resolved by ddl_generators.teradata) and _RAW suffix.
    For MySQL: uses RAW schema and table name as-is.
    """
    db = config.get("TARGET_DB") or target_db(config["SOURCE_DB"])
    schema = config.get("TARGET_SCHEMA") or RAW_SCHEMA
    tbl = config.get("TARGET_TABLE") or config["SOURCE_TABLE"].upper()
    return f"{db}.{schema}.{tbl}"


def stage_path(config: dict, sub: str = "") -> str:
    """Stage path for a table's files: @STAGE/source_db/source_table/sub."""
    parts = [f"@{STAGE}", config["SOURCE_DB"], config["SOURCE_TABLE"]]
    if sub:
        parts.append(sub)
    return "/".join(parts)


def merge_keys(config: dict) -> list[str]:
    """Return the effective merge/dedup key columns (uppercase)."""
    mk = config.get("MERGE_KEYS")
    if mk:
        return [k.upper() for k in mk] if isinstance(mk, list) else [mk.upper()]
    pk = config.get("PRIMARY_KEY")
    return [pk.upper()] if pk else []


def clear_stage_safe(cur, config: dict, sub: str):
    """Remove files from a stage path (idempotent)."""
    cur.execute(f"REMOVE '{stage_path(config, sub)}/'")


def put_file(cur, local_path: Path, config: dict, sub: str):
    """PUT a single file to the table's stage path."""
    # Use forward slashes for Snowflake PUT (Windows compat)
    file_uri = str(local_path.resolve()).replace("\\", "/")
    cur.execute(
        f"PUT 'file://{file_uri}' '{stage_path(config, sub)}/' "
        "AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    )


def put_files_parallel(sf_cfg: dict, files: list[Path], config: dict, sub: str,
                       max_workers: int = 4):
    """PUT multiple files in parallel using dedicated connections per thread."""
    from concurrent.futures import ThreadPoolExecutor

    target = stage_path(config, sub)

    def _put_one(fp):
        conn = snowflake.connector.connect(**sf_cfg)
        try:
            cur = conn.cursor()
            # Use forward slashes for Snowflake PUT (Windows compat)
            file_uri = str(fp.resolve()).replace("\\", "/")
            cur.execute(
                f"PUT 'file://{file_uri}' '{target}/' "
                "AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
            cur.close()
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=min(max_workers, len(files))) as ex:
        list(ex.map(_put_one, files))
    print(f"   PUT complete: {len(files)} file(s) -> {target}/")


def copy_into_full(cur, config: dict, columns: list[tuple],
                   batch_id: str) -> int:
    """Full load: TRUNCATE + COPY INTO from TSV files (mysqlsh format).

    TSV files have only business columns (no audit columns), so we must
    specify the column list explicitly to avoid count mismatch.
    Returns rows loaded.
    """
    fqn = raw_table(config)
    sp = stage_path(config, "full")

    # Build column list (business columns only, matching TSV column order)
    col_list = ", ".join(f'"{name}"' for name, _ in columns)

    cur.execute(f"TRUNCATE TABLE IF EXISTS {fqn}")
    cur.execute(
        f"COPY INTO {fqn} ({col_list})\n"
        f"FROM '{sp}/'\n"
        f"FILE_FORMAT = (FORMAT_NAME = {TSV_ZSTD_FMT})\n"
        f"PATTERN = '.*\\.tsv\\.zst'\n"
        f"ON_ERROR = ABORT_STATEMENT\n"
        f"PURGE = TRUE"
    )
    # COPY INTO returns: (file, status, rows_parsed, rows_loaded, ...)
    # Row count is in column index 3 (rows_loaded) or we sum all rows
    result = cur.fetchall()
    rows = sum(int(r[3]) for r in result) if result else 0
    # Update audit columns for the batch
    cur.execute(
        f'UPDATE {fqn} SET "_BATCH_ID" = %s, "_LOAD_TS" = CURRENT_TIMESTAMP() '
        f'WHERE "_BATCH_ID" IS NULL', (batch_id,))
    print(f"   COPY full: {rows} rows into {fqn}")
    return rows


def copy_into_merge(cur, config: dict, batch_id: str,
                    file_format: str = "parquet",
                    stage_override: str | None = None) -> int:
    """Incremental load: COPY INTO temp table + MERGE/INSERT into RAW.

    Routes by SCD_TYPE:
      0 = Append only (always INSERT, no key matching)
      1 = Upsert (MERGE on primary key — default)
      2 = History (close current record + INSERT new version)

    Args:
        stage_override: If provided, use this stage path instead of the internal stage.

    Returns rows merged/inserted.
    """
    fqn = raw_table(config)
    sp = stage_override or stage_path(config, "incremental")
    keys = merge_keys(config)
    scd_type = int(config.get("SCD_TYPE") or 1)

    if file_format == "parquet":
        fmt = PARQUET_FMT
        pattern = ".*\\.parquet"
        match_by = "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"
    elif file_format == "csv":
        fmt = CSV_FMT
        pattern = ".*\\.csv.*"
        match_by = ""
    else:
        fmt = TSV_ZSTD_FMT
        pattern = ".*\\.tsv\\.zst"
        match_by = ""

    # Create transient staging table
    stg_table = f"{fqn}__STG"
    cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE {stg_table} LIKE {fqn}")
    # Remove audit cols from staging (they'll be set on merge)
    from ddl_generators import SCD2_COLS
    drop_cols = [name for name, _ in AUDIT_COLS] + [name for name, _ in SCD2_COLS]
    for ac_name in drop_cols:
        try:
            cur.execute(f'ALTER TABLE {stg_table} DROP COLUMN IF EXISTS "{ac_name}"')
        except Exception:
            pass

    cur.execute(
        f"COPY INTO {stg_table}\n"
        f"FROM '{sp}/'\n"
        f"FILE_FORMAT = (FORMAT_NAME = {fmt})\n"
        f"PATTERN = '{pattern}'\n"
        f"{match_by}\n"
        f"ON_ERROR = ABORT_STATEMENT\n"
        f"PURGE = TRUE"
    )

    # Get business columns (non-audit, non-SCD2) from target table
    cur.execute(f"SELECT COLUMN_NAME FROM {fqn.split('.')[0]}.INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_SCHEMA = '{RAW_SCHEMA}' "
                f"AND TABLE_NAME = '{config.get('TARGET_TABLE') or config['SOURCE_TABLE'].upper()}' "
                f"AND COLUMN_NAME NOT LIKE '\\_%' ESCAPE '\\\\' "
                f"ORDER BY ORDINAL_POSITION")
    biz_cols = [r[0] for r in cur.fetchall()]
    col_list = ", ".join(f'"{c}"' for c in biz_cols)

    if scd_type == 0:
        # SCD0: Append only — always INSERT, no dedup
        insert_cols = col_list + ', "_LOAD_TS", "_BATCH_ID", "_IS_DELETED"'
        insert_vals = ", ".join(f's."{c}"' for c in biz_cols)
        insert_vals += f", CURRENT_TIMESTAMP(), '{batch_id}', FALSE"
        cur.execute(
            f"INSERT INTO {fqn} ({insert_cols})\n"
            f"SELECT {insert_vals} FROM {stg_table} s"
        )
        rows = cur.rowcount or 0
        print(f"   SCD0 APPEND: {rows} rows into {fqn}")

    elif scd_type == 2:
        # SCD2: Close current records + insert new versions
        if not keys:
            raise ValueError("SCD Type 2 requires PRIMARY_KEY to be set in config")

        on_clause = " AND ".join(f't."{k}" = s."{k}"' for k in keys)

        # Step 1: Close matched records (expire current version)
        cur.execute(
            f'UPDATE {fqn} t SET '
            f'  t."_VALID_TO" = CURRENT_TIMESTAMP(), '
            f'  t."_IS_CURRENT" = FALSE, '
            f'  t."_LOAD_TS" = CURRENT_TIMESTAMP(), '
            f'  t."_BATCH_ID" = \'{batch_id}\' '
            f'FROM {stg_table} s '
            f'WHERE {on_clause} AND t."_IS_CURRENT" = TRUE'
        )
        closed = cur.rowcount or 0

        # Step 2: Insert all staging rows as new current versions
        insert_cols = (col_list +
                       ', "_LOAD_TS", "_BATCH_ID", "_IS_DELETED"'
                       ', "_VALID_FROM", "_VALID_TO", "_IS_CURRENT"')
        insert_vals = ", ".join(f's."{c}"' for c in biz_cols)
        insert_vals += (f", CURRENT_TIMESTAMP(), '{batch_id}', FALSE"
                        f", CURRENT_TIMESTAMP(), NULL, TRUE")
        cur.execute(
            f"INSERT INTO {fqn} ({insert_cols})\n"
            f"SELECT {insert_vals} FROM {stg_table} s"
        )
        rows = cur.rowcount or 0
        print(f"   SCD2 HISTORY: closed {closed}, inserted {rows} new versions into {fqn}")

    else:
        # SCD1: Upsert (default) — MERGE on primary key
        if not keys:
            # No key — append only (same as SCD0 fallback)
            insert_cols = col_list + ', "_LOAD_TS", "_BATCH_ID", "_IS_DELETED"'
            insert_vals = ", ".join(f's."{c}"' for c in biz_cols)
            insert_vals += f", CURRENT_TIMESTAMP(), '{batch_id}', FALSE"
            cur.execute(
                f"INSERT INTO {fqn} ({insert_cols})\n"
                f"SELECT {insert_vals} FROM {stg_table} s"
            )
            rows = cur.rowcount or 0
        else:
            on_clause = " AND ".join(f't."{k}" = s."{k}"' for k in keys)
            update_set = ", ".join(f't."{c}" = s."{c}"' for c in biz_cols)
            update_set += (f', t."_LOAD_TS" = CURRENT_TIMESTAMP()'
                           f', t."_BATCH_ID" = \'{batch_id}\''
                           f', t."_IS_DELETED" = FALSE'
                           f', t."_DELETED_AT" = NULL')
            insert_cols = col_list + ', "_LOAD_TS", "_BATCH_ID", "_IS_DELETED"'
            insert_vals = ", ".join(f's."{c}"' for c in biz_cols)
            insert_vals += f", CURRENT_TIMESTAMP(), '{batch_id}', FALSE"

            cur.execute(
                f"MERGE INTO {fqn} t USING {stg_table} s ON {on_clause}\n"
                f"WHEN MATCHED THEN UPDATE SET {update_set}\n"
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
            )
            rows = cur.rowcount or 0
        print(f"   SCD1 MERGE/INSERT: {rows} rows into {fqn}")

    cur.execute(f"DROP TABLE IF EXISTS {stg_table}")
    return rows


def current_max_watermark(cur, config: dict, wm_col: str) -> str | None:
    """Read MAX(watermark_col) from RAW (source of truth for cursor)."""
    fqn = raw_table(config)
    cur.execute(
        f'SELECT MAX("{wm_col.upper()}") FROM {fqn} '
        f'WHERE COALESCE("_IS_DELETED", FALSE) = FALSE')
    val = cur.fetchone()[0]
    if val is None:
        return None
    return str(val)


def list_stage_files(cur, config: dict, sub: str = "") -> list[dict]:
    """List files currently on the stage for a table. Returns list of
    {name, size, md5, last_modified} dicts. Empty list if none."""
    sp = stage_path(config, sub)
    try:
        cur.execute(f"LIST '{sp}/'")
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception:
        return []


def check_stage_before_load(cur, config: dict, sub: str, run_context: dict = None) -> list[dict]:
    """Check stage for files before loading. If empty, log audit and return [].

    Args:
        cur: Snowflake cursor
        config: table config dict
        sub: stage subdirectory ('full' or 'incremental')
        run_context: optional dict with batch_id, run_id for audit logging

    Returns:
        List of stage files. If empty, an audit record is written.
    """
    files = list_stage_files(cur, config, sub)

    if files:
        print(f"   Stage files ({sub}): {len(files)} file(s) found")
        for f in files[:5]:  # Show first 5
            name = f.get("name", "?")
            size = f.get("size", 0)
            print(f"     • {name} ({size:,} bytes)")
        if len(files) > 5:
            print(f"     ... and {len(files) - 5} more")
    else:
        print(f"   Stage ({sub}): NO FILES — nothing to load")
        # Write audit record if context provided
        if run_context:
            import run_log
            run_log.write_run_log(cur, {
                "batch_id": run_context.get("batch_id"),
                "config_id": run_context.get("config_id"),
                "connection_profile": run_context.get("connection_profile"),
                "source_db": config.get("SOURCE_DB"),
                "source_table": config.get("SOURCE_TABLE"),
                "target_db": config.get("TARGET_DB") or target_db(config["SOURCE_DB"]),
                "target_table": config.get("TARGET_TABLE"),
                "load_type": sub,
                "engine": "loader",
                "rows_extracted": 0,
                "rows_loaded": 0,
                "status": "skipped",
                "error_message": f"No files found on stage ({sub})",
                "failed_step": "load",
                "duration_sec": 0,
                "run_start": run_context.get("run_start"),
                "run_end": run_context.get("run_start"),
            })

    return files
