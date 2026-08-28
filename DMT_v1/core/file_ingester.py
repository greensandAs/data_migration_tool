# Cloud file → Snowflake ingestion engine.
# Co-authored with CoCo
"""file_ingester.py — Cloud file ingestion pipeline (no source database needed).

Supports loading CSV, Parquet, JSON, and Avro files from external stages
(S3, Azure Blob, GCS) into Snowflake tables with:
  - Auto table creation via INFER_SCHEMA()
  - Dynamic FILE FORMAT creation per job
  - Pattern-based file matching
  - APPEND / OVERWRITE / MERGE load modes
  - Pre-load file counting for validation
"""
from __future__ import annotations

import re
import time
import uuid
from datetime import datetime

from metadata import file_ingest_config


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _sanitize_sql_string(value: str) -> str:
    """Escape single quotes to prevent SQL injection in string literals."""
    if not value:
        return value
    return value.replace("'", "''")


def _stage_path(config: dict) -> str:
    """Build the full @stage/path reference."""
    stage = config["STAGE_NAME"]
    path = (config.get("CLOUD_PATH") or "").strip("/")

    if config.get("DATE_PARTITION"):
        date_fmt = config.get("DATE_FORMAT") or "%Y%m%d"
        date_str = datetime.now().strftime(date_fmt)
        path = f"{path}/{date_str}" if path else date_str

    if path:
        return f"@{stage}/{path}/"
    return f"@{stage}/"


def _build_file_format_sql(config: dict, fmt_name: str) -> str:
    """Generate CREATE FILE FORMAT DDL from config settings."""
    file_type = (config.get("FILE_TYPE") or "CSV").upper()

    if file_type == "PARQUET":
        extras = config.get("FILE_FORMAT_EXTRAS") or ""
        return (f"CREATE OR REPLACE TEMPORARY FILE FORMAT {fmt_name}\n"
                f"  TYPE = 'PARQUET'\n"
                f"  {extras};")

    if file_type == "JSON":
        extras = config.get("FILE_FORMAT_EXTRAS") or ""
        return (f"CREATE OR REPLACE TEMPORARY FILE FORMAT {fmt_name}\n"
                f"  TYPE = 'JSON'\n"
                f"  STRIP_OUTER_ARRAY = TRUE\n"
                f"  {extras};")

    if file_type == "AVRO":
        extras = config.get("FILE_FORMAT_EXTRAS") or ""
        return (f"CREATE OR REPLACE TEMPORARY FILE FORMAT {fmt_name}\n"
                f"  TYPE = 'AVRO'\n"
                f"  {extras};")

    # CSV (default) — sanitize all user-provided values
    delimiter = _sanitize_sql_string(config.get("FIELD_DELIMITER") or ",")
    enclosed = _sanitize_sql_string(config.get("FIELD_ENCLOSED_BY") or "")
    escape = _sanitize_sql_string(config.get("ESCAPE_CHARACTER") or "")
    skip = int(config.get("SKIP_HEADER") or 1)
    null_if = config.get("NULL_IF") or "('')"
    extras = config.get("FILE_FORMAT_EXTRAS") or ""

    parts = [
        f"CREATE OR REPLACE TEMPORARY FILE FORMAT {fmt_name}",
        f"  TYPE = 'CSV'",
        f"  FIELD_DELIMITER = '{delimiter}'",
    ]
    if enclosed:
        parts.append(f"  FIELD_OPTIONALLY_ENCLOSED_BY = '{enclosed}'")
    if escape:
        parts.append(f"  ESCAPE = '{escape}'")
    parts.append(f"  SKIP_HEADER = {skip}")
    parts.append(f"  NULL_IF = {null_if}")
    if extras:
        parts.append(f"  {extras}")
    return "\n".join(parts) + ";"


def _count_staged_files(cur, stage_path: str, pattern: str,
                        fmt_name: str) -> tuple[int, int]:
    """Count files and rows available on stage matching the pattern."""
    safe_pattern = _sanitize_sql_string(pattern)
    try:
        cur.execute(
            f"SELECT COUNT(DISTINCT METADATA$FILENAME) AS FILES, COUNT(*) AS ROWS\n"
            f"FROM {stage_path}\n"
            f"(FILE_FORMAT => '{fmt_name}', PATTERN => '{safe_pattern}')")
        row = cur.fetchone()
        return (int(row[0] or 0), int(row[1] or 0))
    except Exception:
        try:
            cur.execute(f"LIST {stage_path}")
            rows = cur.fetchall()
            # Use regex matching instead of naive string replace
            try:
                pat = re.compile(pattern)
                matched = [r for r in rows if pat.search(str(r[0]))]
            except re.error:
                matched = rows
            return (len(matched), 0)
        except Exception:
            return (0, 0)


def _infer_and_create_table(cur, config: dict, stage_path: str,
                            pattern: str, fmt_name: str) -> str:
    """Create target table by inferring schema from staged files."""
    target_fqn = (f"{config['TARGET_DB']}.{config.get('TARGET_SCHEMA', 'RAW')}"
                  f".{config['TARGET_TABLE']}")

    cur.execute(f"CREATE DATABASE IF NOT EXISTS {config['TARGET_DB']}")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS "
                f"{config['TARGET_DB']}.{config.get('TARGET_SCHEMA', 'RAW')}")

    # INFER_SCHEMA — scans files at the location to detect schema
    cur.execute(
        f"SELECT COLUMN_NAME, TYPE, NULLABLE\n"
        f"FROM TABLE(INFER_SCHEMA(\n"
        f"  LOCATION => '{stage_path}'\n"
        f"  , FILE_FORMAT => '{fmt_name}'\n"
        f"))")
    columns = cur.fetchall()

    if not columns:
        raise RuntimeError(
            f"INFER_SCHEMA returned no columns from {stage_path} "
            f"with pattern '{pattern}'. Check stage path and file format.")

    col_defs = []
    for col_name, col_type, nullable in columns:
        null_str = "" if nullable else " NOT NULL"
        col_defs.append(f'  "{col_name}" {col_type}{null_str}')

    col_defs.append('  "_LOAD_TS" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()')
    col_defs.append('  "_SRC_FILE" VARCHAR')
    col_defs.append('  "_BATCH_ID" VARCHAR')

    ddl = (f"CREATE TABLE IF NOT EXISTS {target_fqn} (\n"
           f"{',\n'.join(col_defs)}\n);")

    cur.execute(ddl)
    print(f"   table created: {target_fqn} ({len(columns)} cols + audit)")
    return target_fqn


def _build_copy_into(config: dict, target_fqn: str, stage_path: str,
                     pattern: str, fmt_name: str) -> str:
    """Build the COPY INTO statement."""
    on_error = config.get("ON_ERROR") or "ABORT_STATEMENT"
    match_by = config.get("MATCH_BY_COLUMN_NAME")
    purge = config.get("PURGE_FILES", False)
    extras = config.get("COPY_EXTRAS") or ""
    file_type = (config.get("FILE_TYPE") or "CSV").upper()
    safe_pattern = _sanitize_sql_string(pattern)

    parts = [
        f"COPY INTO {target_fqn}",
        f"FROM {stage_path}",
        f"FILE_FORMAT = (FORMAT_NAME = '{fmt_name}')",
        f"PATTERN = '{safe_pattern}'",
        f"ON_ERROR = {on_error}",
    ]

    if match_by:
        parts.append(f"MATCH_BY_COLUMN_NAME = {match_by}")
    elif file_type in ("PARQUET", "AVRO", "JSON"):
        parts.append("MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE")

    if purge:
        parts.append("PURGE = TRUE")
    if extras:
        parts.append(extras)

    return "\n".join(parts) + ";"


def _execute_merge(cur, config: dict, target_fqn: str, stage_path: str,
                   pattern: str, fmt_name: str) -> tuple[int, int]:
    """Execute MERGE INTO for deduplication/upsert loads.

    Loads into a temp staging table, then MERGEs into the target.
    Requires MERGE_KEYS in config (comma-separated column names).
    Returns (files_loaded, rows_merged).
    """
    merge_keys = config.get("MERGE_KEYS") or config.get("PRIMARY_KEY") or ""
    if isinstance(merge_keys, list):
        key_cols = [k.strip().upper() for k in merge_keys if k.strip()]
    else:
        key_cols = [k.strip().upper() for k in str(merge_keys).split(",") if k.strip()]

    if not key_cols:
        raise ValueError(
            "MERGE mode requires MERGE_KEYS or PRIMARY_KEY in job config. "
            "Set comma-separated key columns for deduplication.")

    # Create temp staging table
    staging_fqn = f"{target_fqn}__DMT_STAGING"
    cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE {staging_fqn} LIKE {target_fqn}")

    # COPY INTO staging
    safe_pattern = _sanitize_sql_string(pattern)
    file_type = (config.get("FILE_TYPE") or "CSV").upper()
    match_by = config.get("MATCH_BY_COLUMN_NAME")

    copy_parts = [
        f"COPY INTO {staging_fqn}",
        f"FROM {stage_path}",
        f"FILE_FORMAT = (FORMAT_NAME = '{fmt_name}')",
        f"PATTERN = '{safe_pattern}'",
        f"ON_ERROR = {config.get('ON_ERROR', 'ABORT_STATEMENT')}",
    ]
    if match_by:
        copy_parts.append(f"MATCH_BY_COLUMN_NAME = {match_by}")
    elif file_type in ("PARQUET", "AVRO", "JSON"):
        copy_parts.append("MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE")

    cur.execute("\n".join(copy_parts) + ";")
    copy_results = cur.fetchall()
    files_loaded = len(copy_results) if copy_results else 0

    # Get columns from target (exclude audit cols for merge SET)
    cur.execute(f"SHOW COLUMNS IN TABLE {target_fqn}")
    all_cols_raw = cur.fetchall()
    col_name_idx = next(i for i, d in enumerate(cur.description) if d[0] == "column_name")
    all_cols = [r[col_name_idx].strip('"') for r in all_cols_raw]
    data_cols = [c for c in all_cols if c not in ("_LOAD_TS", "_SRC_FILE", "_BATCH_ID")]

    # Build MERGE
    on_clause = " AND ".join(f'tgt."{k}" = stg."{k}"' for k in key_cols)
    update_cols = [c for c in data_cols if c not in key_cols]
    update_set = ", ".join(f'tgt."{c}" = stg."{c}"' for c in update_cols)
    insert_cols = ", ".join(f'"{c}"' for c in data_cols)
    insert_vals = ", ".join(f'stg."{c}"' for c in data_cols)

    merge_sql = (
        f"MERGE INTO {target_fqn} tgt\n"
        f"USING {staging_fqn} stg\n"
        f"ON {on_clause}\n"
        f"WHEN MATCHED THEN UPDATE SET {update_set}\n"
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});"
    )
    cur.execute(merge_sql)

    # Get merge row count
    cur.execute(f"SELECT COUNT(*) FROM {staging_fqn}")
    rows_merged = cur.fetchone()[0] or 0

    # Cleanup staging
    cur.execute(f"DROP TABLE IF EXISTS {staging_fqn}")

    return files_loaded, rows_merged


def run_ingestion(sf_conn, config: dict, batch_id: str = None,
                  skip_config_update: bool = False) -> dict:
    """Execute a single file ingestion job.

    Args:
        skip_config_update: If True, skip updating FILE_INGESTION_CONFIG status
                           (used for ad-hoc uploads that don't have a config row).
    """
    batch_id = batch_id or uuid.uuid4().hex[:12]
    config_id = config["CONFIG_ID"]
    job_name = config.get("JOB_NAME", "unknown")
    # Unique format name per job to avoid collisions in batch runs
    job_uid = uuid.uuid4().hex[:8]
    t0 = time.monotonic()
    run_start = _now()

    cur = sf_conn.cursor()
    result = {
        "BATCH_ID": batch_id,
        "CONFIG_ID": config_id,
        "JOB_NAME": job_name,
        "STAGE_NAME": config.get("STAGE_NAME"),
        "CLOUD_PATH": config.get("CLOUD_PATH"),
        "FILE_PATTERN": config.get("FILE_PATTERN"),
        "FILE_TYPE": config.get("FILE_TYPE"),
        "TARGET_DB": config.get("TARGET_DB"),
        "TARGET_SCHEMA": config.get("TARGET_SCHEMA", "RAW"),
        "TARGET_TABLE": config.get("TARGET_TABLE"),
        "LOAD_MODE": config.get("LOAD_MODE", "APPEND"),
        "STATUS": "running",
        "RUN_START": run_start,
    }

    fmt_name = f"DMT_INGEST_FMT_{job_uid}"

    try:
        stage_path = _stage_path(config)
        pattern = config.get("FILE_PATTERN", ".*")
        target_fqn = (f"{config['TARGET_DB']}.{config.get('TARGET_SCHEMA', 'RAW')}"
                      f".{config['TARGET_TABLE']}")
        load_mode = (config.get("LOAD_MODE") or "APPEND").upper()

        # Step 1: Create temporary file format
        fmt_sql = _build_file_format_sql(config, fmt_name)
        print(f"   [{job_name}] creating file format...")
        cur.execute(fmt_sql)

        # Step 2: Count source files
        print(f"   [{job_name}] scanning {stage_path} for '{pattern}'...")
        file_count, src_rows = _count_staged_files(cur, stage_path, pattern, fmt_name)
        result["FILES_MATCHED"] = file_count

        if file_count == 0:
            result["STATUS"] = "skipped"
            result["ERROR_MESSAGE"] = "No files matched pattern"
            result["FILES_LOADED"] = 0
            result["ROWS_LOADED"] = 0
            print(f"   [{job_name}] no files found - skipping")
            return result

        print(f"   [{job_name}] {file_count} file(s) matched")

        # Step 3: Create table if needed
        table_created = False
        if not config.get("TABLE_EXISTS", True):
            _infer_and_create_table(cur, config, stage_path, pattern, fmt_name)
            table_created = True
            result["TABLE_CREATED"] = True

        # Step 4: Execute based on load mode
        if load_mode == "MERGE":
            # MERGE mode: load into staging → MERGE INTO target
            print(f"   [{job_name}] MERGE mode: loading via staging table...")
            files_loaded, total_rows = _execute_merge(
                cur, config, target_fqn, stage_path, pattern, fmt_name)

        elif load_mode == "OVERWRITE":
            # OVERWRITE: load into temp → swap (atomic, safe from data loss)
            print(f"   [{job_name}] OVERWRITE: loading into staging for atomic swap...")
            swap_table = f"{target_fqn}__DMT_SWAP"
            cur.execute(f"CREATE OR REPLACE TABLE {swap_table} LIKE {target_fqn}")
            # COPY INTO swap table
            copy_sql = _build_copy_into(config, swap_table, stage_path, pattern, fmt_name)
            cur.execute(copy_sql)
            copy_results = cur.fetchall()
            files_loaded, total_rows = _parse_copy_results(cur, copy_results, swap_table)
            # Atomic swap only if COPY succeeded
            if files_loaded > 0:
                cur.execute(f"ALTER TABLE {swap_table} SWAP WITH {target_fqn}")
                cur.execute(f"DROP TABLE IF EXISTS {swap_table}")
                print(f"   [{job_name}] swap complete")
            else:
                cur.execute(f"DROP TABLE IF EXISTS {swap_table}")
                raise RuntimeError("COPY INTO produced 0 files — swap aborted")

        else:
            # APPEND (default): straight COPY INTO
            copy_sql = _build_copy_into(config, target_fqn, stage_path, pattern, fmt_name)
            print(f"   [{job_name}] running COPY INTO {target_fqn}...")
            cur.execute(copy_sql)
            copy_results = cur.fetchall()
            files_loaded, total_rows = _parse_copy_results(cur, copy_results, target_fqn)

        result["FILES_LOADED"] = files_loaded
        result["ROWS_LOADED"] = total_rows
        result["STATUS"] = "success"

        print(f"   [{job_name}] loaded {total_rows:,} rows from "
              f"{files_loaded} file(s)")

        # Step 5: Update config tracking (skip for ad-hoc uploads)
        if not skip_config_update:
            file_ingest_config.update_run_status(
                cur, config_id, status=result["STATUS"],
                file_count=files_loaded, row_count=total_rows)
        sf_conn.commit()

    except Exception as e:
        result["STATUS"] = "failed"
        result["ERROR_MESSAGE"] = str(e)[:2000]
        result["FAILED_STEP"] = "ingestion"
        print(f"   [{job_name}] FAILED: {e}")

        try:
            if not skip_config_update:
                file_ingest_config.update_run_status(
                    cur, config_id, status="failed", error=str(e))
            sf_conn.commit()
        except Exception:
            pass

    finally:
        result["RUN_END"] = _now()
        result["DURATION_SEC"] = round(time.monotonic() - t0, 2)

        try:
            file_ingest_config.write_log(cur, result)
            sf_conn.commit()
        except Exception as le:
            print(f"   (log write failed: {le})")

        try:
            cur.execute(f"DROP FILE FORMAT IF EXISTS {fmt_name}")
        except Exception:
            pass

        cur.close()

    return result


def _parse_copy_results(cur, copy_results: list, target_fqn: str) -> tuple[int, int]:
    """Parse COPY INTO result rows. Returns (files_loaded, total_rows)."""
    total_rows = 0
    files_loaded = 0
    for row in copy_results:
        if len(row) >= 4:
            status = str(row[1]).upper() if row[1] else ""
            if "LOADED" in status or "LOAD" in status:
                files_loaded += 1
                total_rows += int(row[3] or 0)
        else:
            files_loaded += 1

    # Fallback: if parsing failed, count from table
    if total_rows == 0 and files_loaded == 0 and copy_results:
        files_loaded = len(copy_results)
        try:
            cur.execute(f"SELECT COUNT(*) FROM {target_fqn}")
            total_rows = cur.fetchone()[0] or 0
        except Exception:
            pass

    return files_loaded, total_rows


def run_all(sf_conn, batch_id: str = None,
            only_job: str = None) -> list[dict]:
    """Run all active file ingestion jobs (or a single named job)."""
    batch_id = batch_id or uuid.uuid4().hex[:12]
    cur = sf_conn.cursor()
    configs = file_ingest_config.list_configs(cur, active_only=True)
    cur.close()

    if only_job:
        configs = [c for c in configs if c["JOB_NAME"] == only_job
                   or c["CONFIG_ID"] == only_job]

    if not configs:
        print("   no active file ingestion jobs found")
        return []

    print(f"==== File Ingestion Batch {batch_id} "
          f"({len(configs)} job(s)) ====")

    results = []
    for config in configs:
        r = run_ingestion(sf_conn, config, batch_id=batch_id)
        results.append(r)

    ok = sum(1 for r in results if r["STATUS"] == "success")
    failed = sum(1 for r in results if r["STATUS"] == "failed")
    skipped = sum(1 for r in results if r["STATUS"] == "skipped")
    print(f"==== Batch complete: {ok} success, {failed} failed, "
          f"{skipped} skipped ====")

    return results
