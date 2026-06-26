# MySQL incremental extraction via connectorx + Arrow/Parquet.
"""extractors.mysql_incremental — Incremental extraction using connectorx.

Reads only the delta window (rows newer than the last watermark) via Arrow,
then writes Snappy-compressed Parquet files for efficient Snowflake ingestion.

Supports two cursor modes:
  - time: WHERE watermark_col > last_loaded_at (captures inserts + updates)
  - id:   WHERE primary_key > last_loaded_key (captures inserts only)
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus

import pyarrow.parquet as pq

from extractors import BaseExtractor, ExtractionResult


class MySQLIncrementalExtractor(BaseExtractor):

    @property
    def source_type(self) -> str:
        return "mysql"

    def extract_full(self, config: dict, src_cfg: dict,
                     output_dir: str | Path) -> ExtractionResult:
        raise NotImplementedError(
            "Use MySQLFullExtractor for full loads")

    def extract_incremental(self, config: dict, src_cfg: dict,
                            output_dir: str | Path,
                            source_conn=None) -> ExtractionResult:
        """Extract delta rows via connectorx -> Parquet files."""
        import connectorx as cx

        wm_col, wm_type = self._resolve_cursor(config, source_conn)
        if not wm_col:
            return ExtractionResult(skipped=True,
                                    skip_reason="no watermark column configured",
                                    engine="connectorx")

        cursor_value = (config.get("LAST_LOADED_KEY") if wm_type == "id"
                        else config.get("LAST_LOADED_AT"))

        query, has_window = self._build_query(config, wm_col, wm_type, cursor_value)
        if not has_window:
            return ExtractionResult(skipped=True,
                                    skip_reason="no cursor value (first run = full)",
                                    engine="connectorx")

        uri = self._mysql_uri(src_cfg, config["SOURCE_DB"])
        print(f"   connectorx query: {query[:120]}...")

        arrow = cx.read_sql(uri, query, return_type="arrow")
        rows = arrow.num_rows
        print(f"   rows fetched: {rows:,}")

        if rows == 0:
            return ExtractionResult(skipped=True, row_count=0,
                                    skip_reason="no new rows",
                                    engine="connectorx")

        # Write Parquet file(s), split by rows_per_file
        # output_dir is already: ./export/<source_type>/<connection>/<table>
        out_dir = Path(output_dir) / "incremental"
        out_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        rows_per_file = int(config.get("ROWS_PER_FILE") or 1_000_000)

        files = []
        if rows > rows_per_file:
            import pyarrow as pa
            for i, batch in enumerate(arrow.to_batches(max_chunksize=rows_per_file)):
                fp = out_dir / f"incr_{stamp}_part{i:04d}.parquet"
                pq.write_table(pa.Table.from_batches([batch]), fp, compression="snappy")
                files.append(fp)
        else:
            fp = out_dir / f"incr_{stamp}.parquet"
            pq.write_table(arrow, fp, compression="snappy")
            files.append(fp)

        # Determine watermark_to from the fetched data
        wm_to = self._extract_max_watermark(arrow, wm_col, wm_type)

        return ExtractionResult(
            files=files,
            row_count=rows,
            watermark_to=wm_to,
            file_format="parquet",
            engine="connectorx",
        )

    def _resolve_cursor(self, config: dict, mysql_conn) -> tuple[str | None, str | None]:
        """Determine the effective cursor column and type."""
        wm_col = config.get("WATERMARK_COL")
        wm_type = config.get("WATERMARK_TYPE")

        if wm_type == "id" and not wm_col:
            wm_col = config.get("PRIMARY_KEY")

        if not wm_col:
            return None, None

        # Auto-detect type from MySQL metadata if not explicitly set
        if not wm_type and mysql_conn:
            cur = mysql_conn.cursor()
            cur.execute(
                "SELECT DATA_TYPE FROM information_schema.columns "
                "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s",
                (config["SOURCE_DB"], config["SOURCE_TABLE"], wm_col.lower()),
            )
            row = cur.fetchone()
            cur.close()
            if row:
                dtype = str(row[0]).lower()
                if dtype in ("datetime", "timestamp", "date"):
                    wm_type = "time"
                elif dtype in ("int", "integer", "bigint", "smallint", "mediumint"):
                    wm_type = "id"

        return wm_col, wm_type or "time"

    def _build_query(self, config: dict, wm_col: str, wm_type: str,
                     cursor_value) -> tuple[str, bool]:
        """Build the SELECT with WHERE clause for the delta window.

        Supports:
          - CUSTOM_SQL: full override, used as-is
          - FILTER_CONDITION: static filter applied every run (combined with watermark)
          - Watermark: incremental delta window
        """
        # Custom SQL takes full precedence
        custom_sql = config.get("CUSTOM_SQL")
        if custom_sql:
            return custom_sql.strip(), True

        db = config["SOURCE_DB"]
        table = config["SOURCE_TABLE"]
        base = f"SELECT * FROM `{db}`.`{table}`"
        conditions = []

        # Static filter (applied every run regardless of watermark)
        filter_cond = config.get("FILTER_CONDITION")
        if filter_cond:
            conditions.append(f"({filter_cond})")

        # Watermark window (incremental delta)
        if cursor_value is not None:
            if wm_type == "id":
                conditions.append(f"`{wm_col}` > {cursor_value}")
            else:
                conditions.append(f"`{wm_col}` > '{cursor_value}'")

        if conditions:
            return f"{base} WHERE {' AND '.join(conditions)}", True
        return base, False

    def _mysql_uri(self, src_cfg: dict, database: str) -> str:
        """Build connectorx MySQL URI."""
        user = quote_plus(src_cfg["user"])
        pwd = quote_plus(src_cfg.get("password", ""))
        host = src_cfg["host"]
        port = src_cfg["port"]
        return f"mysql://{user}:{pwd}@{host}:{port}/{database}?zeroDateTimeBehavior=convertToNull"

    def _extract_max_watermark(self, arrow_table, wm_col: str,
                               wm_type: str) -> str | None:
        """Get the maximum watermark value from the extracted Arrow table."""
        import pyarrow.compute as pc

        # Column names in Arrow may be lowercase
        col_name = None
        for name in arrow_table.column_names:
            if name.lower() == wm_col.lower():
                col_name = name
                break
        if not col_name:
            return None

        col = arrow_table.column(col_name)
        max_val = pc.max(col).as_py()
        if max_val is None:
            return None

        if wm_type == "id":
            return str(max_val)
        else:
            if isinstance(max_val, datetime):
                return max_val.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            return str(max_val)
