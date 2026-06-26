# Teradata incremental extraction via teradatasql + Arrow/Parquet.
"""extractors.teradata_incremental — Incremental extraction using teradatasql.

Reads only the delta window (rows newer than the last watermark) via the
teradatasql connector, then writes Snappy-compressed Parquet files for
efficient Snowflake ingestion.

Supports two cursor modes:
  - time: WHERE cdc_col > last_loaded_at (captures inserts + updates)
  - id:   WHERE cdc_col > last_loaded_key (captures inserts only)
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from extractors import BaseExtractor, ExtractionResult


class TeradataIncrementalExtractor(BaseExtractor):

    @property
    def source_type(self) -> str:
        return "teradata"

    def extract_full(self, config: dict, src_cfg: dict,
                     output_dir: str | Path) -> ExtractionResult:
        raise NotImplementedError(
            "Use TeradataFullExtractor for full loads")

    def extract_incremental(self, config: dict, src_cfg: dict,
                            output_dir: str | Path,
                            source_conn=None) -> ExtractionResult:
        """Extract delta rows via teradatasql -> Parquet files."""
        import teradatasql
        import pyarrow as pa
        import pyarrow.parquet as pq

        wm_col = config.get("WATERMARK_COL") or config.get("CDC_COLUMNS")
        wm_type = config.get("WATERMARK_TYPE") or config.get("CDC_TYPE", "time")
        if wm_type and wm_type.upper() == "TIMESTAMP":
            wm_type = "time"
        elif wm_type and wm_type.upper() == "ID":
            wm_type = "id"

        if not wm_col:
            return ExtractionResult(skipped=True,
                                    skip_reason="no watermark/CDC column configured",
                                    engine="teradatasql")

        cursor_value = (config.get("LAST_LOADED_KEY") if wm_type == "id"
                        else config.get("LAST_LOADED_AT"))

        query, has_window = self._build_query(config, wm_col, wm_type, cursor_value)
        if not has_window:
            return ExtractionResult(skipped=True,
                                    skip_reason="no cursor value (first run = full)",
                                    engine="teradatasql")

        # Connect to Teradata
        td_conn = source_conn
        if not td_conn:
            td_conn = teradatasql.connect(
                host=src_cfg["host"],
                user=src_cfg["user"],
                password=src_cfg.get("password", ""),
                logmech=src_cfg.get("logmech", "TD2"))

        print(f"   teradatasql query: {query[:120]}...")

        cur = td_conn.cursor()
        cur.execute(query)

        # Fetch all rows into Arrow table
        columns_desc = cur.description
        col_names = [d[0] for d in columns_desc]
        rows_data = cur.fetchall()
        cur.close()

        if not source_conn:
            td_conn.close()

        total_rows = len(rows_data)
        print(f"   rows fetched: {total_rows:,}")

        if total_rows == 0:
            return ExtractionResult(skipped=True, row_count=0,
                                    skip_reason="no new rows",
                                    engine="teradatasql")

        # Convert to Arrow table
        arrays = []
        for col_idx in range(len(col_names)):
            col_data = [row[col_idx] for row in rows_data]
            arrays.append(pa.array(col_data))
        arrow_table = pa.table(
            {name: arr for name, arr in zip(col_names, arrays)})

        # Write Parquet file(s)
        out_dir = Path(output_dir) / "incremental"
        out_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        rows_per_file = int(config.get("ROWS_PER_FILE") or 1_000_000)

        files = []
        if total_rows > rows_per_file:
            for i, batch in enumerate(arrow_table.to_batches(max_chunksize=rows_per_file)):
                fp = out_dir / f"incr_{stamp}_part{i:04d}.parquet"
                pq.write_table(pa.Table.from_batches([batch]), fp, compression="snappy")
                files.append(fp)
        else:
            fp = out_dir / f"incr_{stamp}.parquet"
            pq.write_table(arrow_table, fp, compression="snappy")
            files.append(fp)

        # Determine watermark_to
        wm_to = self._extract_max_watermark(rows_data, col_names, wm_col, wm_type)

        return ExtractionResult(
            files=files,
            row_count=total_rows,
            watermark_to=wm_to,
            file_format="parquet",
            engine="teradatasql",
        )

    def _build_query(self, config: dict, wm_col: str, wm_type: str,
                     cursor_value) -> tuple[str, bool]:
        """Build the SELECT with WHERE clause for the delta window."""
        db = config["SOURCE_DB"]
        table = config["SOURCE_TABLE"]
        base = f"SELECT * FROM {db}.{table}"
        conditions = []

        # Static filter
        filter_cond = config.get("FILTER_CONDITION")
        if filter_cond:
            conditions.append(f"({filter_cond})")

        # Watermark window
        if cursor_value is not None:
            # Handle multiple CDC columns (comma-separated)
            cdc_cols = [c.strip() for c in wm_col.split(",") if c.strip()]
            if wm_type == "id":
                col = cdc_cols[0]
                conditions.append(f"CAST({col} AS INTEGER) > {cursor_value}")
            else:
                # Timestamp: OR across multiple CDC columns
                ts_conditions = []
                for col in cdc_cols:
                    ts_conditions.append(
                        f"({col} > CAST('{cursor_value}' AS TIMESTAMP))")
                if len(ts_conditions) == 1:
                    conditions.append(ts_conditions[0])
                else:
                    conditions.append(f"({' OR '.join(ts_conditions)})")

        if conditions:
            return f"{base} WHERE {' AND '.join(conditions)}", True
        return base, False

    def _extract_max_watermark(self, rows: list, col_names: list,
                               wm_col: str, wm_type: str) -> str | None:
        """Get the maximum watermark value from fetched rows."""
        # Use first CDC column for watermark
        target_col = wm_col.split(",")[0].strip()
        col_idx = None
        for i, name in enumerate(col_names):
            if name.upper() == target_col.upper():
                col_idx = i
                break
        if col_idx is None:
            return None

        max_val = None
        for row in rows:
            val = row[col_idx]
            if val is not None:
                if max_val is None or val > max_val:
                    max_val = val

        if max_val is None:
            return None
        if wm_type == "id":
            return str(max_val)
        if isinstance(max_val, datetime):
            return max_val.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return str(max_val)
