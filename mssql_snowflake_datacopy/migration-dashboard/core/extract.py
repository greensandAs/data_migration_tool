"""extract.py — BCP export, CDC condition builder, file splitting + gzip."""
from __future__ import annotations

import gzip
import os
import subprocess
import time
from datetime import datetime
from pathlib import Path

from core.connections import mssql_query, sf_query


# ─── CDC Condition Builder ────────────────────────────────────────────────────

def get_last_cdc_value(source_db: str, source_table: str, cdc_type: str) -> str | None:
    """Get last successful CDC value (timestamp or ID) from LOG_TABLE."""
    if cdc_type == "TIMESTAMP":
        sql = (
            "SELECT MAX(JOB_START_TIME) FROM DATA_MIGRATION.CONTROL.LOG_TABLE "
            "WHERE MSSQL_DATABASE_NAME=%s AND MSSQL_TABLE_NAME=%s AND FINAL_STATUS='SUCCESS'"
        )
    elif cdc_type == "ID":
        sql = (
            "SELECT MAX(MSSQL_TABLE_COUNT) FROM DATA_MIGRATION.CONTROL.LOG_TABLE "
            "WHERE MSSQL_DATABASE_NAME=%s AND MSSQL_TABLE_NAME=%s AND FINAL_STATUS='SUCCESS'"
        )
    else:
        return None

    try:
        df = sf_query(sql, (source_db, source_table))
        if df is not None and not df.empty:
            val = df.iloc[0, 0]
            return str(val) if val else None
    except Exception:
        pass
    return None


def build_cdc_condition(tbl: dict, export_start_time: datetime) -> str:
    """Build WHERE condition based on CDC config. Returns SQL condition string."""
    load_type = tbl.get("load_type", "full")
    cdc_columns = tbl.get("cdc_columns")
    cdc_type = (tbl.get("cdc_type") or "TIMESTAMP").upper()
    scd_type = tbl.get("scd_type", 0)
    filter_condition = tbl.get("filter_condition")

    if load_type == "full":
        return "1=1"

    if load_type == "filter":
        return filter_condition if filter_condition else "1=1"

    # INCREMENTAL
    if not cdc_columns:
        return "1=1"

    last_value = get_last_cdc_value(tbl["source_db"], tbl["source_table"], cdc_type)

    if cdc_type == "TIMESTAMP":
        start_ts = last_value or "1900-01-01 00:00:00.000"
        end_ts = str(export_start_time)
        cols = [c.strip() for c in cdc_columns.split(",")]
        parts = []
        for col in cols:
            parts.append(
                f"(({col} >= TRY_CAST('{start_ts}' AS DATETIME2)) "
                f"AND ({col} < TRY_CAST('{end_ts}' AS DATETIME2)))"
            )
        return "(" + " OR ".join(parts) + ")"

    elif cdc_type == "ID":
        last_id = int(last_value) if last_value else 0
        col = cdc_columns.split(",")[0].strip()
        return f"CAST({col} AS INTEGER) > {last_id}"

    return "1=1"


# ─── Column Name Detection ────────────────────────────────────────────────────

def get_column_names(database: str, schema: str, table: str) -> list[str]:
    """Get column names from MSSQL INFORMATION_SCHEMA."""
    rows = mssql_query(
        database,
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION",
        (schema, table),
    )
    return [r[0] for r in rows]


def get_column_metadata(database: str, schema: str, table: str) -> list[dict]:
    """Get full column metadata from MSSQL INFORMATION_SCHEMA.

    Returns list of dicts: {name, data_type, max_length, precision, scale, is_nullable}
    """
    rows = mssql_query(
        database,
        "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, "
        "NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE "
        "FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION",
        (schema, table),
    )
    return [
        {
            "name": r[0],
            "data_type": r[1].lower(),
            "max_length": r[2],
            "precision": r[3],
            "scale": r[4],
            "is_nullable": r[5] == "YES",
        }
        for r in rows
    ]


# ─── BCP Export ───────────────────────────────────────────────────────────────

def bcp_export(tbl: dict, filepath: Path, condition: str = "1=1") -> dict:
    """Export data from MSSQL via BCP. Returns {returncode, stdout, row_count, cmd}."""
    src_db = tbl["source_db"]
    src_schema = tbl.get("source_schema", "dbo")
    src_table = tbl["source_table"]
    delimiter = tbl.get("delimiter", "|")
    custom_sql = tbl.get("custom_sql")

    server = os.getenv("MSSQL_SERVER", "")
    user = os.getenv("MSSQL_USER", "")
    password = os.getenv("MSSQL_PASSWORD", "")

    if custom_sql:
        # Custom SQL with queryout
        bcp_cmd = (
            f'bcp "{custom_sql}" queryout "{filepath}" '
            f'-S {server} -d {src_db} -U {user} -P {password} '
            f'-c -t "{delimiter}" -C 65001'
        )
    elif condition != "1=1":
        # Filtered export with queryout
        query = f"SELECT * FROM [{src_schema}].[{src_table}] WHERE {condition}"
        bcp_cmd = (
            f'bcp "{query}" queryout "{filepath}" '
            f'-S {server} -d {src_db} -U {user} -P {password} '
            f'-c -t "{delimiter}" -C 65001'
        )
    else:
        # Full table export
        bcp_cmd = (
            f'bcp "{src_db}.{src_schema}.{src_table}" out "{filepath}" '
            f'-S {server} -U {user} -P {password} '
            f'-c -t "{delimiter}" -C 65001'
        )

    proc = subprocess.run(bcp_cmd, shell=True, capture_output=True, text=True, timeout=3600)

    row_count = 0
    if proc.returncode in (0, 4):
        try:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                row_count = sum(1 for _ in f)
        except Exception:
            pass

    return {
        "returncode": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "row_count": row_count,
        "cmd": bcp_cmd,
    }


# ─── File Splitting + GZip ────────────────────────────────────────────────────

def split_and_gzip(filepath: Path, output_dir: Path, chunk_size_mb: int = 512) -> list[Path]:
    """Split a large file into gzip chunks. Returns list of chunk file paths."""
    chunk_size = chunk_size_mb * 1024 * 1024
    file_size = filepath.stat().st_size

    if file_size <= chunk_size:
        # Single file — just gzip it
        gz_path = output_dir / (filepath.name + ".gz")
        with open(filepath, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
            f_out.write(f_in.read())
        return [gz_path]

    # Split into chunks
    base_name = filepath.stem
    chunks = []
    file_index = 1

    with open(filepath, "rb") as f:
        while True:
            start_pos = f.tell()
            chunk = f.read(chunk_size)
            if not chunk:
                break

            # Find last newline to avoid splitting mid-row
            last_newline = chunk.rfind(b"\n")
            if last_newline == -1:
                data = chunk
            else:
                data = chunk[: last_newline + 1]
                f.seek(start_pos + last_newline + 1)

            chunk_name = f"{base_name}_part{file_index}.csv.gz"
            chunk_path = output_dir / chunk_name

            with gzip.open(chunk_path, "wb") as gz:
                gz.write(data)

            chunks.append(chunk_path)
            file_index += 1

    return chunks
