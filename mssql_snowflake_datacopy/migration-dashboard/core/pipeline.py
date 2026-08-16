"""pipeline.py — Orchestrator: ties extract → upload → load → audit."""
from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from core.config import save_config
from core.connections import mssql_count
from core.extract import bcp_export, build_cdc_condition, split_and_gzip
from core.load import execute_load
from core.logger import (
    create_log_entry, finalize_job, get_next_batch_id,
    update_copy_sql, update_export_filename, update_merge_sql, update_step,
)
from core.upload import cleanup_local, move_to_processed, upload_to_blob


def run_single_table(tbl: dict, mode: str, batch_id: int, job_id: int,
                     progress_cb=None, status_cb=None) -> dict:
    """Run the full pipeline for a single table.

    Args:
        tbl: Table config dict
        mode: FULL / EXPORT / LOAD
        batch_id: Current batch
        job_id: Job index
        progress_cb: callable(float) to report progress 0.0-1.0
        status_cb: callable(str) to report status text

    Returns: {table, status, duration, row_count, sf_count, logs}
    """
    def _progress(val):
        if progress_cb:
            progress_cb(val)

    def _status(msg):
        if status_cb:
            status_cb(msg)

    src_table = tbl["source_table"]
    job_start = datetime.now(timezone.utc).replace(tzinfo=None)  # UTC, timezone-naive for MSSQL compat
    logs = []
    row_count = 0
    sf_count = 0
    load_result = None

    # Create log entry
    try:
        create_log_entry(batch_id, job_id, tbl, mode, job_start)
    except Exception as e:
        return _result(src_table, "FAILED", 0, 0, 0, [f"Log entry failed: {e}"])

    # ── EXTRACT PHASE ─────────────────────────────────────────────────────────
    if mode in ("FULL", "EXPORT"):
        _progress(0.1)
        _status(f"[{src_table}] Building CDC condition...")

        # Build export condition
        condition = build_cdc_condition(tbl, job_start)
        logs.append(f"Condition: {condition[:100]}")

        # BCP Export
        _progress(0.2)
        _status(f"[{src_table}] BCP export...")

        export_dir = Path(os.getenv("EXPORT_DIR", "./export")).resolve()
        export_dir.mkdir(parents=True, exist_ok=True)
        ts = job_start.strftime("%Y%m%d_%H%M%S")
        filename = f"{tbl['source_db']}_{tbl.get('source_schema','dbo')}_{src_table}_{ts}.csv"
        filepath = export_dir / filename

        bcp_result = bcp_export(tbl, filepath, condition)
        row_count = bcp_result["row_count"]

        if bcp_result["returncode"] not in (0, 4):
            logs.append(f"BCP FAILED: {bcp_result['stderr'][:300]}")
            update_step(batch_id, job_id, "BCP_EXPORT_STATUS", "FAILED", bcp_result["stderr"])
            return _finalize(tbl, batch_id, job_id, job_start, "FAILED", row_count, 0, logs)

        logs.append(f"BCP: {row_count} rows → {filename}")
        update_step(batch_id, job_id, "BCP_EXPORT_STATUS", "SUCCESS")
        update_export_filename(batch_id, job_id, filename, row_count)

        # Split + GZip
        _progress(0.35)
        _status(f"[{src_table}] Compressing...")

        split_dir = export_dir / "chunks"
        split_dir.mkdir(exist_ok=True)
        chunk_files = split_and_gzip(filepath, split_dir)
        logs.append(f"Compressed: {len(chunk_files)} chunk(s)")

        # Upload to Azure Blob
        _progress(0.5)
        _status(f"[{src_table}] Uploading to Azure Blob...")

        upload_result = upload_to_blob(chunk_files, tbl["target_table"])
        if upload_result["returncode"] != 0:
            logs.append(f"Upload FAILED: {upload_result['log']}")
            update_step(batch_id, job_id, "S3_UPLOAD_STATUS", "FAILED", upload_result["log"])
            cleanup_local(chunk_files + [filepath])
            return _finalize(tbl, batch_id, job_id, job_start, "FAILED", row_count, 0, logs)

        logs.append(f"Upload: {upload_result['uploaded_count']} file(s) → Blob/{tbl['target_table']}/")
        update_step(batch_id, job_id, "S3_UPLOAD_STATUS", "SUCCESS")

        # Cleanup local files
        cleanup_local(chunk_files + [filepath])

    # ── LOAD PHASE ────────────────────────────────────────────────────────────
    if mode in ("FULL", "LOAD"):
        _progress(0.65)
        _status(f"[{src_table}] Loading into Snowflake...")

        load_result = execute_load(tbl)
        logs.extend(load_result["logs"])
        sf_count = load_result["sf_count"]

        # In LOAD mode, BCP doesn't run so use rows_loaded from COPY INTO
        if mode == "LOAD" and row_count == 0:
            row_count = load_result.get("rows_loaded", 0)

        if load_result["returncode"] != 0:
            update_step(batch_id, job_id, "COPY_COMMAND_STATUS", "FAILED", "\n".join(load_result["logs"]))
            return _finalize(tbl, batch_id, job_id, job_start, "FAILED", row_count, sf_count, logs)

        # Update log with COPY/MERGE info
        if load_result.get("merge_sql"):
            update_merge_sql(batch_id, job_id, load_result["merge_sql"])
        update_step(batch_id, job_id, "COPY_COMMAND_STATUS", "SUCCESS")

        # Move files to processed/
        _progress(0.85)
        _status(f"[{src_table}] Moving to processed/...")
        mv_result = move_to_processed(tbl["target_table"])
        logs.append(mv_result["log"])

    # ── FINALIZE ──────────────────────────────────────────────────────────────
    _progress(1.0)

    # Get actual MSSQL total count for accurate logging
    try:
        mssql_total = mssql_count(
            tbl["source_db"],
            tbl.get("source_schema", "dbo"),
            tbl["source_table"],
        )
    except Exception:
        mssql_total = row_count  # fallback to BCP count

    return _finalize(tbl, batch_id, job_id, job_start, "SUCCESS", mssql_total, sf_count, logs,
                     rows_extracted=row_count,
                     rows_inserted=load_result.get("rows_inserted", 0) if load_result else 0,
                     rows_updated=load_result.get("rows_updated", 0) if load_result else 0,
                     rows_expired=load_result.get("rows_expired", 0) if load_result else 0)


def _finalize(tbl, batch_id, job_id, job_start, status, row_count, sf_count, logs,
              rows_extracted=0, rows_inserted=0, rows_updated=0, rows_expired=0) -> dict:
    """Finalize a job: update LOG_TABLE and config."""
    job_end = datetime.now(timezone.utc).replace(tzinfo=None)
    duration = int((job_end - job_start).total_seconds())

    try:
        finalize_job(batch_id, job_id, status, job_end, duration, row_count, sf_count,
                     rows_extracted=rows_extracted, rows_inserted=rows_inserted,
                     rows_updated=rows_updated, rows_expired=rows_expired)
    except Exception:
        pass

    # Update local config status
    tbl["last_run_status"] = status.lower()
    tbl["last_loaded_at"] = job_end.strftime("%Y-%m-%d %H:%M:%S")

    return {
        "table": tbl["source_table"],
        "status": status,
        "duration": duration,
        "row_count": row_count,
        "sf_count": sf_count,
        "rows_extracted": rows_extracted,
        "rows_inserted": rows_inserted,
        "rows_updated": rows_updated,
        "rows_expired": rows_expired,
        "logs": logs,
    }


def _result(table, status, duration, row_count, sf_count, logs):
    return {"table": table, "status": status, "duration": duration,
            "row_count": row_count, "sf_count": sf_count, "logs": logs}


# ─── Parallel Execution ───────────────────────────────────────────────────────

def run_batch_parallel(tables: list[dict], mode: str, batch_id: int, max_workers: int = 4) -> list[dict]:
    """Run multiple tables in parallel using ThreadPoolExecutor.

    Returns list of result dicts (no progress/status callbacks in parallel mode).
    """
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(run_single_table, tbl, mode, batch_id, idx + 1): tbl
            for idx, tbl in enumerate(tables)
        }
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                tbl = futures[future]
                results.append(_result(tbl["source_table"], "FAILED", 0, 0, 0, [str(e)]))
    return results
