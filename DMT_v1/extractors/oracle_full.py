# Oracle full-load extraction via oracledb with streaming Parquet output.
# Co-authored with CoCo
"""extractors.oracle_full — Full-load engine using oracledb.

Two extraction strategies based on table size:
  - Small/Medium (< 500 MB): Single streaming fetch with parallel hint
  - Large (>= 500 MB) with numeric PK: Parallel range scans via ThreadPoolExecutor

Output: Snappy-compressed Parquet files for efficient Snowflake COPY INTO.

Key performance features:
  - arraysize=10000 (100x fewer network round-trips than default)
  - prefetchrows eliminates initial probe round-trip
  - Streaming fetchmany(500K) — bounded memory regardless of table size
  - PyArrow columnar conversion — no Python csv.writer bottleneck
  - Optional parallel range scans — N connections reading different ID ranges
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq

from extractors import BaseExtractor, ExtractionResult
from extractors.oracle_common import (
    tuned_cursor, estimate_table_bytes, find_numeric_pk,
    build_parallel_ranges, parallel_hint, oracle_connect,
    rows_to_arrow, DEFAULT_BATCH_ROWS, PARALLEL_THRESHOLD_BYTES,
    PARALLEL_DEGREE,
)


class OracleFullExtractor(BaseExtractor):

    @property
    def source_type(self) -> str:
        return "oracle"

    def extract_full(self, config: dict, src_cfg: dict,
                     output_dir: str | Path) -> ExtractionResult:
        """Full extraction of an Oracle table.

        Automatically chooses between single-stream and parallel-range
        extraction based on table size and PK availability.
        """
        out_dir = Path(output_dir) / "full"
        if out_dir.exists():
            for p in sorted(out_dir.glob("**/*"), reverse=True):
                p.unlink() if p.is_file() else p.rmdir()
            out_dir.rmdir()
        out_dir.mkdir(parents=True, exist_ok=True)

        schema = config["SOURCE_DB"]
        table = config["SOURCE_TABLE"]
        filter_cond = config.get("FILTER_CONDITION")
        partition_num = int(config.get("PARTITION_NUM") or PARALLEL_DEGREE)

        conn = oracle_connect(src_cfg)
        try:
            # Estimate size to decide extraction strategy
            table_bytes = estimate_table_bytes(conn, schema, table)
            use_parallel = (
                table_bytes >= PARALLEL_THRESHOLD_BYTES
                and not filter_cond  # parallel ranges don't combine well with custom filters
            )

            if use_parallel:
                pk_col = find_numeric_pk(conn, schema, table)
                if pk_col:
                    print(f"   oracle full: parallel range scan on {pk_col} "
                          f"({partition_num} partitions, ~{table_bytes // (1024*1024)} MB)")
                    return self._extract_parallel(
                        config, src_cfg, out_dir, schema, table,
                        pk_col, partition_num)
                else:
                    print(f"   oracle full: no numeric PK — streaming with parallel hint "
                          f"(~{table_bytes // (1024*1024)} MB)")

            # Single-stream extraction (small table or no numeric PK)
            return self._extract_streaming(conn, config, out_dir, schema, table, filter_cond)
        finally:
            conn.close()

    def _extract_streaming(self, conn, config: dict, out_dir: Path,
                           schema: str, table: str,
                           filter_cond: str | None) -> ExtractionResult:
        """Single-connection streaming extraction with parallel hint."""
        hint = parallel_hint()
        custom_sql = config.get("CUSTOM_SQL")

        if custom_sql:
            query = custom_sql.strip()
        else:
            where = f" WHERE {filter_cond}" if filter_cond else ""
            query = f"SELECT {hint} * FROM {schema}.{table}{where}"

        print(f"   oracle query: {query[:120]}...")

        cursor = tuned_cursor(conn)
        cursor.execute(query)
        col_names = [desc[0] for desc in cursor.description]
        col_types = [desc[1] for desc in cursor.description]

        batch_size = int(config.get("ROWS_PER_FILE") or DEFAULT_BATCH_ROWS)
        files = []
        total_rows = 0
        part = 0
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break

            arrow_table = rows_to_arrow(rows, col_names, col_types)
            fp = out_dir / f"full_{stamp}_part{part:04d}.parquet"
            pq.write_table(arrow_table, fp, compression="snappy")
            files.append(fp)
            total_rows += len(rows)
            part += 1

            if part % 5 == 0:
                print(f"   ... {total_rows:,} rows extracted ({part} files)")

        cursor.close()
        print(f"   full extract complete: {total_rows:,} rows -> {len(files)} file(s)")

        return ExtractionResult(
            files=files,
            row_count=total_rows,
            file_format="parquet",
            engine="oracledb",
        )

    def _extract_parallel(self, config: dict, src_cfg: dict, out_dir: Path,
                          schema: str, table: str,
                          pk_col: str, n_parts: int) -> ExtractionResult:
        """Parallel range-partitioned extraction using multiple connections.

        Each worker opens its own Oracle connection and fetches a non-overlapping
        ID range, writing Parquet files independently.
        """
        conn = oracle_connect(src_cfg)
        try:
            ranges = build_parallel_ranges(conn, schema, table, pk_col, n_parts)
        finally:
            conn.close()

        if not ranges:
            # Fallback to streaming
            conn = oracle_connect(src_cfg)
            try:
                return self._extract_streaming(conn, config, out_dir, schema, table, None)
            finally:
                conn.close()

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        all_files = []
        total_rows = 0
        batch_size = int(config.get("ROWS_PER_FILE") or DEFAULT_BATCH_ROWS)
        errors = []

        def _extract_range(range_idx: int, condition: str):
            """Worker: extract one range into Parquet files."""
            range_conn = oracle_connect(src_cfg)
            range_files = []
            range_rows = 0
            try:
                hint = parallel_hint(2)  # lighter parallelism per worker
                query = (f"SELECT {hint} * FROM {schema}.{table} "
                         f"WHERE {condition}")

                cur = tuned_cursor(range_conn)
                cur.execute(query)
                col_names = [desc[0] for desc in cur.description]
                col_types = [desc[1] for desc in cur.description]

                part = 0
                while True:
                    rows = cur.fetchmany(batch_size)
                    if not rows:
                        break
                    arrow_table = rows_to_arrow(rows, col_names, col_types)
                    fp = out_dir / f"full_{stamp}_r{range_idx:02d}_p{part:04d}.parquet"
                    pq.write_table(arrow_table, fp, compression="snappy")
                    range_files.append(fp)
                    range_rows += len(rows)
                    part += 1
                cur.close()
            finally:
                range_conn.close()
            return range_files, range_rows

        # Run parallel workers (cap at n_parts concurrent connections)
        max_workers = min(n_parts, 8)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_extract_range, i, cond): i
                for i, cond in enumerate(ranges)
            }
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    r_files, r_rows = future.result()
                    all_files.extend(r_files)
                    total_rows += r_rows
                    print(f"   range {idx+1}/{n_parts}: {r_rows:,} rows")
                except Exception as e:
                    errors.append(f"range {idx}: {e}")
                    print(f"   range {idx+1}/{n_parts} FAILED: {e}")

        if errors and not all_files:
            raise RuntimeError(
                f"All parallel ranges failed: {'; '.join(errors)}")

        # Sort files by name for deterministic order
        all_files.sort(key=lambda p: p.name)
        print(f"   parallel extract complete: {total_rows:,} rows -> "
              f"{len(all_files)} file(s)")

        return ExtractionResult(
            files=all_files,
            row_count=total_rows,
            file_format="parquet",
            engine="oracledb",
        )

    def extract_incremental(self, config: dict, src_cfg: dict,
                            output_dir: str | Path,
                            source_conn=None) -> ExtractionResult:
        """Full extractor does not support incremental — delegate."""
        raise NotImplementedError(
            "Use OracleIncrementalExtractor for incremental loads")
