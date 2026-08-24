# MSSQL full-load extraction via BCP (bulk copy program).
"""extractors.mssql_full — Full-load engine using BCP.

Uses Microsoft's `bcp` command-line tool for fast bulk export to pipe-delimited
CSV files, then splits large files and gzip-compresses them for efficient
Snowflake ingestion via COPY INTO.

The password is passed on stdin (never in argv) — see extractors.mssql_common.
"""
from __future__ import annotations

import gzip
from pathlib import Path

from extractors import BaseExtractor, ExtractionResult
from extractors import mssql_common


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
        delimiter = str(config.get("DELIMITER") or ",")
        if delimiter == r"\t":
            delimiter = "\t"
        if len(delimiter) != 1:
            raise ValueError(f"Invalid DELIMITER: {delimiter!r}")
        custom_sql = config.get("CUSTOM_SQL")
        filter_condition = config.get("FILTER_CONDITION")

        server = mssql_common.server_spec(src_cfg.get("host", ""),
                                          src_cfg.get("port"))
        user = src_cfg.get("user", "")
        password = src_cfg.get("password", "")

        filename = f"{src_db}_{src_schema}_{src_table}.csv"
        filepath = out_dir / filename

        # Choose bcp mode. "queryout" is required whenever we need a WHERE
        # clause or a custom SELECT; "out" copies the whole table.
        if custom_sql:
            source_spec, mode, database = custom_sql, "queryout", src_db
        elif filter_condition and filter_condition.strip() != "1=1":
            source_spec = (f"SELECT * FROM [{src_schema}].[{src_table}] "
                           f"WHERE {filter_condition}")
            mode, database = "queryout", src_db
        else:
            source_spec = f"{src_db}.{src_schema}.{src_table}"
            mode, database = "out", None

        args = mssql_common.build_bcp_args(
            source_spec=source_spec, mode=mode, filepath=filepath,
            server=server, user=user, delimiter=delimiter, database=database)

        print(f"   BCP: {src_db}.{src_schema}.{src_table} -> {filepath.name}")
        proc = mssql_common.run_bcp(args, password)

        if proc.returncode not in mssql_common.BCP_OK_RETURNCODES:
            err = mssql_common.bcp_error(proc)
            print(f"   ERROR: BCP failed (rc={proc.returncode}): {err[:200]}")
            return ExtractionResult(
                files=[], row_count=0, engine="bcp",
                skipped=True, skip_reason=f"BCP error: {err[:500]}")

        row_count = mssql_common.count_lines(filepath)

        if row_count == 0:
            print("   WARNING: BCP produced 0 rows — skipping")
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

        print(f"   BCP Complete: {row_count} rows -> {len(gz_files)} file(s)")
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
