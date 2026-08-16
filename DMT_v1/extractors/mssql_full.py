# MSSQL full-load extraction via BCP (bulk copy program).
"""extractors.mssql_full — Full-load engine using BCP.

Uses Microsoft's `bcp` command-line tool for fast bulk export to pipe-delimited
CSV files, then splits large files and gzip-compresses them for efficient
Snowflake ingestion via COPY INTO.
"""
from __future__ import annotations

import gzip
import os
import subprocess
from pathlib import Path

from extractors import BaseExtractor, ExtractionResult


class MSSQLFullExtractor(BaseExtractor):

    @property
    def source_type(self) -> str:
        return "mssql"

    def extract_full(self, config: dict, src_cfg: dict,
                     output_dir: str | Path) -> ExtractionResult:
        """Run BCP export for full table extraction."""
        out_dir = Path(output_dir) / "full"
        if out_dir.exists():
            for p in sorted(out_dir.glob("**/*"), reverse=True):
                p.unlink() if p.is_file() else p.rmdir()
            out_dir.rmdir()
        out_dir.mkdir(parents=True, exist_ok=True)

        src_db = config["SOURCE_DB"]
        src_schema = config.get("SOURCE_SCHEMA") or "dbo"
        src_table = config["SOURCE_TABLE"]
        delimiter = config.get("DELIMITER") or "|"
        custom_sql = config.get("CUSTOM_SQL")
        filter_condition = config.get("FILTER_CONDITION")

        server = src_cfg.get("host", "")
        user = src_cfg.get("user", "")
        password = src_cfg.get("password", "")

        filename = f"{src_db}_{src_schema}_{src_table}.csv"
        filepath = out_dir / filename

        # Build BCP command
        if custom_sql:
            bcp_cmd = (
                f'bcp "{custom_sql}" queryout "{filepath}" '
                f'-S {server} -d {src_db} -U {user} -P {password} '
                f'-c -t "{delimiter}" -C 65001'
            )
        elif filter_condition and filter_condition.strip() != "1=1":
            query = f"SELECT * FROM [{src_schema}].[{src_table}] WHERE {filter_condition}"
            bcp_cmd = (
                f'bcp "{query}" queryout "{filepath}" '
                f'-S {server} -d {src_db} -U {user} -P {password} '
                f'-c -t "{delimiter}" -C 65001'
            )
        else:
            bcp_cmd = (
                f'bcp "{src_db}.{src_schema}.{src_table}" out "{filepath}" '
                f'-S {server} -U {user} -P {password} '
                f'-c -t "{delimiter}" -C 65001'
            )

        print(f"   BCP: {src_db}.{src_schema}.{src_table} → {filepath.name}")
        proc = subprocess.run(
            bcp_cmd, shell=True, capture_output=True, text=True, timeout=7200)

        if proc.returncode not in (0, 4):
            err = proc.stderr.strip() or proc.stdout.strip()
            print(f"   ❌ BCP failed (rc={proc.returncode}): {err[:200]}")
            return ExtractionResult(
                files=[], row_count=0, engine="bcp",
                skipped=True, skip_reason=f"BCP error: {err[:500]}")

        # Count rows
        row_count = 0
        if filepath.exists():
            try:
                with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                    row_count = sum(1 for _ in f)
            except Exception:
                pass

        if row_count == 0:
            print("   ⚠️ BCP produced 0 rows — skipping")
            return ExtractionResult(
                files=[], row_count=0, engine="bcp",
                skipped=True, skip_reason="No rows extracted")

        # Split and gzip
        gz_files = _split_and_gzip(filepath, out_dir)

        # Remove uncompressed original
        try:
            filepath.unlink(missing_ok=True)
        except Exception:
            pass

        print(f"   ✅ BCP: {row_count} rows → {len(gz_files)} file(s)")
        return ExtractionResult(
            files=gz_files, row_count=row_count,
            file_format="csv_gzip", engine="bcp")

    def extract_incremental(self, config: dict, src_cfg: dict,
                            output_dir: str | Path,
                            source_conn=None) -> ExtractionResult:
        raise NotImplementedError("Use MSSQLIncrementalExtractor for incremental loads")


def _split_and_gzip(filepath: Path, output_dir: Path,
                    chunk_size_mb: int = 512) -> list[Path]:
    """Split a large file into gzip chunks at row boundaries."""
    chunk_size = chunk_size_mb * 1024 * 1024
    file_size = filepath.stat().st_size

    if file_size <= chunk_size:
        gz_path = output_dir / (filepath.name + ".gz")
        with open(filepath, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
            while True:
                block = f_in.read(8 * 1024 * 1024)
                if not block:
                    break
                f_out.write(block)
        return [gz_path]

    # Multi-part split
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
                data = chunk[:last_newline + 1]
                f.seek(start_pos + last_newline + 1)

            chunk_name = f"{base_name}_part{file_index}.csv.gz"
            chunk_path = output_dir / chunk_name

            with gzip.open(chunk_path, "wb") as gz:
                gz.write(data)

            chunks.append(chunk_path)
            file_index += 1

    return chunks
