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

import time
import uuid
from datetime import datetime

from metadata import file_ingest_config


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _stage_path(config: dict) -> str:
    """Build the full @stage/path reference."""
    stage = config["STAGE_NAME"]
    path = (config.get("CLOUD_PATH") or "").strip("/")

    # Handle date-partitioned paths
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

    # CSV (default)
    delimiter = config.get("FIELD_DELIMITER") or ","
    enclosed = config.get("FIELD_ENCLOSED_BY") or ""
    escape = config.get("ESCAPE_CHARACTER") or ""
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
    """Count files and rows available on stage matching the pattern.

    Returns (file_count, approx_row_count). Row count may be 0 for binary formats.
    """
    try:
        cur.execute(
            f"SELECT COUNT(DISTINCT METADATA$FILENAME) AS FILES, COUNT(*) AS ROWS\n"
            f"FROM {stage_path}\n"
            f"(FILE_FORMAT => '{fmt_name}', PATTERN => '{pattern}')")
        row = cur.fetchone()
        return (int(row[0] or 0), int(row[1] or 0))
    except Exception:
        # Fallback: just list files
        try:
            cur.execute(f"LIST {stage_path}")
            rows = cur.fetchall()
            matched = [r for r in rows if pattern.replace(".*", "") in str(r[0])]
            return (len(matched), 0)
        except Exception:
            return (0, 0)


def _infer_and_create_table(cur, config: dict, stage_path: str,
                            pattern: str, fmt_name: str) -> str:
    """Create target table by inferring schema from staged files.

    Uses Snowflake's INFER_SCHEMA() table function.
    """
    target_fqn = (f"{config['TARGET_DB']}.{config.get('TARGET_SCHEMA', 'RAW')}"
                  f".{config['TARGET_TABLE']}")
    file_type = (config.get("FILE_TYPE") or "CSV").upper()

    # INFER_SCHEMA works with Parquet, CSV, Avro, ORC, JSON
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {config['TARGET_DB']}")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS "
                f"{config['TARGET_DB']}.{config.get('TARGET_SCHEMA', 'RAW')}")

    # Use INFER_SCHEMA to get column definitions
    cur.execute(
        f"SELECT COLUMN_NAME, TYPE, NULLABLE\n"
        f"FROM TABLE(INFER_SCHEMA(\n"
        f"  LOCATION => '{stage_path}'\n"
        f"  , FILE_FORMAT => '{fmt_name}'\n"
        f"  , FILES => ''\n"
        f"))")
    columns = cur.fetchall()

    if not columns:
        raise RuntimeError(
            f"INFER_SCHEMA returned no columns from {stage_path} "
            f"with pattern files. Check stage path and file format.")

    # Build CREATE TABLE from inferred columns
    col_defs = []
    for col_name, col_type, nullable in columns:
        null_str = "" if nullable else " NOT NULL"
        col_defs.append(f'  "{col_name}" {col_type}{null_str}')

    # Add audit columns
    col_defs.append('  "_LOAD_TS" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()')
    col_defs.append('  "_SRC_FILE" VARCHAR')
    col_defs.append('  "_BATCH_ID" VARCHAR')

    ddl = (f"CREATE TABLE IF NOT EXISTS {target_fqn} (\n"
           f"{','.join(col_defs)}\n);")

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

    parts = [
        f"COPY INTO {target_fqn}",
        f"FROM {stage_path}",
        f"FILE_FORMAT = (FORMAT_NAME = '{fmt_name}')",
        f"PATTERN = '{pattern}'",
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


def run_ingestion(sf_conn, config: dict, batch_id: str = None) -> dict:
    """Execute a single file ingestion job.

    Returns a result dict with status, rows_loaded, files, etc.
    """
    batch_id = batch_id or uuid.uuid4().hex[:12]
    config_id = config["CONFIG_ID"]
    job_name = config.get("JOB_NAME", "unknown")
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

    try:
        stage_path = _stage_path(config)
        pattern = config.get("FILE_PATTERN", ".*")
        target_fqn = (f"{config['TARGET_DB']}.{config.get('TARGET_SCHEMA', 'RAW')}"
                      f".{config['TARGET_TABLE']}")
        load_mode = (config.get("LOAD_MODE") or "APPEND").upper()

        # Step 1: Create temporary file format
        fmt_name = f"DMT_INGEST_FMT_{batch_id}"
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

        # Step 4: Handle OVERWRITE mode (truncate before load)
        if load_mode == "OVERWRITE" and not table_created:
            print(f"   [{job_name}] OVERWRITE: truncating {target_fqn}")
            cur.execute(f"TRUNCATE TABLE IF EXISTS {target_fqn}")

        # Step 5: COPY INTO
        copy_sql = _build_copy_into(config, target_fqn, stage_path, pattern, fmt_name)
        print(f"   [{job_name}] running COPY INTO {target_fqn}...")
        cur.execute(copy_sql)

        # Parse COPY INTO results
        copy_results = cur.fetchall()
        total_rows = 0
        files_loaded = 0
        errors = []
        for row in copy_results:
            # COPY INTO returns: file, status, rows_parsed, rows_loaded, ...
            if len(row) >= 4:
                status = str(row[1]).upper() if row[1] else ""
                if "LOADED" in status or "LOAD" in status:
                    files_loaded += 1
                    total_rows += int(row[3] or 0)
                elif "ERROR" in status:
                    errors.append(str(row))
            else:
                files_loaded += 1

        # Fallback: if we couldn't parse, count from result
        if total_rows == 0 and files_loaded == 0 and copy_results:
            files_loaded = len(copy_results)
            # Get actual count from table
            try:
                cur.execute(f"SELECT COUNT(*) FROM {target_fqn}")
                total_rows = cur.fetchone()[0] or 0
            except Exception:
                pass

        result["FILES_LOADED"] = files_loaded
        result["ROWS_LOADED"] = total_rows
        result["STATUS"] = "success"

        if errors:
            result["ERROR_MESSAGE"] = "; ".join(errors[:3])
            result["STATUS"] = "partial"

        print(f"   [{job_name}] loaded {total_rows:,} rows from "
              f"{files_loaded} file(s)")

        # Step 6: Update config tracking
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
            file_ingest_config.update_run_status(
                cur, config_id, status="failed", error=str(e))
            sf_conn.commit()
        except Exception:
            pass

    finally:
        result["RUN_END"] = _now()
        result["DURATION_SEC"] = round(time.monotonic() - t0, 2)

        # Write log
        try:
            file_ingest_config.write_log(cur, result)
            sf_conn.commit()
        except Exception as le:
            print(f"   (log write failed: {le})")

        # Cleanup temp file format
        try:
            cur.execute(f"DROP FILE FORMAT IF EXISTS {fmt_name}")
        except Exception:
            pass

        cur.close()

    return result


def run_all(sf_conn, batch_id: str = None,
            only_job: str = None) -> list[dict]:
    """Run all active file ingestion jobs (or a single named job).

    Returns list of result dicts.
    """
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

    # Summary
    ok = sum(1 for r in results if r["STATUS"] == "success")
    failed = sum(1 for r in results if r["STATUS"] == "failed")
    skipped = sum(1 for r in results if r["STATUS"] == "skipped")
    print(f"==== Batch complete: {ok} success, {failed} failed, "
          f"{skipped} skipped ====")

    return results
