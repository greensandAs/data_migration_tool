# Oracle incremental extraction via oracledb with CDC condition filtering.
# Co-authored with CoCo
"""extractors.oracle_incremental — Incremental extraction using oracledb.

Builds a WHERE condition based on the watermark (timestamp or ID cursor),
then fetches only the delta rows using optimised array fetch into Parquet.

Supports two cursor modes:
  - time: WHERE cdc_col >= TO_TIMESTAMP(last, fmt) (captures inserts + updates)
  - id:   WHERE pk_col > last_loaded_key (captures inserts only)

Multi-column CDC: comma-separated columns produce an OR condition so any
column advancing triggers extraction (e.g., CREATED_AT, UPDATED_AT).

Performance: arraysize=10000, streaming fetchmany, Snappy Parquet output.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq

from extractors import BaseExtractor, ExtractionResult
from extractors.oracle_common import (
    tuned_cursor, oracle_connect, parallel_hint,
    rows_to_arrow, DEFAULT_BATCH_ROWS,
)


class OracleIncrementalExtractor(BaseExtractor):

    @property
    def source_type(self) -> str:
        return "oracle"

    def extract_full(self, config: dict, src_cfg: dict,
                     output_dir: str | Path) -> ExtractionResult:
        raise NotImplementedError(
            "Use OracleFullExtractor for full loads")

    def extract_incremental(self, config: dict, src_cfg: dict,
                            output_dir: str | Path,
                            source_conn=None) -> ExtractionResult:
        """Extract delta rows via oracledb -> Parquet files."""
        wm_col = config.get("WATERMARK_COL") or config.get("CDC_COLUMNS")
        wm_type = (config.get("WATERMARK_TYPE") or config.get("CDC_TYPE", "time")).lower()

        # Normalise watermark type labels
        if wm_type in ("timestamp", "datetime"):
            wm_type = "time"

        if not wm_col:
            return ExtractionResult(
                skipped=True,
                skip_reason="no watermark/CDC column configured",
                engine="oracledb")

        cursor_value = (config.get("LAST_LOADED_KEY") if wm_type == "id"
                        else config.get("LAST_LOADED_AT"))

        query, has_window = self._build_query(config, wm_col, wm_type, cursor_value)
        if not has_window:
            return ExtractionResult(
                skipped=True,
                skip_reason="no cursor value (first run = full)",
                engine="oracledb")

        # Connect
        conn = source_conn or oracle_connect(src_cfg)
        close_conn = source_conn is None

        try:
            print(f"   oracle incr query: {query[:120]}...")

            cursor = tuned_cursor(conn)
            cursor.execute(query)
            col_names = [desc[0] for desc in cursor.description]
            col_types = [desc[1] for desc in cursor.description]

            # Streaming fetch → Parquet
            out_dir = Path(output_dir) / "incremental"
            out_dir.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            batch_size = int(config.get("ROWS_PER_FILE") or DEFAULT_BATCH_ROWS)

            files = []
            total_rows = 0
            part = 0
            max_wm_value = None

            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break

                # Track max watermark from fetched data
                max_wm_value = self._update_max_watermark(
                    rows, col_names, wm_col, wm_type, max_wm_value)

                arrow_table = rows_to_arrow(rows, col_names, col_types)
                fp = out_dir / f"incr_{stamp}_part{part:04d}.parquet"
                pq.write_table(arrow_table, fp, compression="snappy")
                files.append(fp)
                total_rows += len(rows)
                part += 1

            cursor.close()

            if total_rows == 0:
                return ExtractionResult(
                    skipped=True, row_count=0,
                    skip_reason="no new rows in CDC window",
                    engine="oracledb")

            # Format watermark for storage
            wm_to = self._format_watermark(max_wm_value, wm_type)

            print(f"   oracle incr: {total_rows:,} rows -> {len(files)} file(s)"
                  f" (wm_to={wm_to})")

            return ExtractionResult(
                files=files,
                row_count=total_rows,
                watermark_to=wm_to,
                file_format="parquet",
                engine="oracledb",
            )
        finally:
            if close_conn:
                conn.close()

    def _build_query(self, config: dict, wm_col: str, wm_type: str,
                     cursor_value) -> tuple[str, bool]:
        """Build the SELECT with WHERE clause for the delta window.

        Supports:
          - CUSTOM_SQL: full override, used as-is
          - FILTER_CONDITION: static filter applied every run
          - Watermark: incremental delta window
          - Multi-column CDC (comma-separated): OR across columns
        """
        # Custom SQL takes full precedence
        custom_sql = config.get("CUSTOM_SQL")
        if custom_sql:
            return custom_sql.strip(), True

        schema = config["SOURCE_DB"]
        table = config["SOURCE_TABLE"]
        hint = parallel_hint(4)
        base = f"SELECT {hint} * FROM {schema}.{table}"
        conditions = []

        # Static filter (applied every run)
        filter_cond = config.get("FILTER_CONDITION")
        if filter_cond:
            conditions.append(f"({filter_cond})")

        # Watermark window
        if cursor_value is not None:
            cdc_cols = [c.strip() for c in wm_col.split(",") if c.strip()]

            if wm_type == "id":
                col = cdc_cols[0]
                conditions.append(f"{col} > {cursor_value}")
            else:
                # Timestamp: OR across multiple CDC columns
                ts_conditions = []
                for col in cdc_cols:
                    # Use TO_TIMESTAMP with a format that handles common patterns
                    ts_conditions.append(
                        f"{col} > TO_TIMESTAMP('{cursor_value}', "
                        f"'YYYY-MM-DD HH24:MI:SS.FF3')")
                if len(ts_conditions) == 1:
                    conditions.append(ts_conditions[0])
                else:
                    conditions.append(f"({' OR '.join(ts_conditions)})")

        if conditions:
            return f"{base} WHERE {' AND '.join(conditions)}", True
        return base, False

    def _update_max_watermark(self, rows: list, col_names: list[str],
                              wm_col: str, wm_type: str,
                              current_max) -> object:
        """Track the maximum watermark value across fetched batches."""
        target_col = wm_col.split(",")[0].strip()
        col_idx = None
        for i, name in enumerate(col_names):
            if name.upper() == target_col.upper():
                col_idx = i
                break
        if col_idx is None:
            return current_max

        for row in rows:
            val = row[col_idx]
            if val is not None:
                if current_max is None or val > current_max:
                    current_max = val

        return current_max

    def _format_watermark(self, value, wm_type: str) -> str | None:
        """Format the watermark value for storage in MIGRATION_CONFIG."""
        if value is None:
            return None
        if wm_type == "id":
            return str(value)
        # Timestamp types
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return str(value)
