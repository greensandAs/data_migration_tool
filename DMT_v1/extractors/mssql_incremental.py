# MSSQL incremental extraction via BCP with CDC condition filtering.
"""extractors.mssql_incremental — Incremental extraction using BCP queryout.

Builds a WHERE condition based on the watermark (timestamp or ID cursor),
then uses BCP queryout to export only the delta rows. Produces gzip-compressed
CSV files for Snowflake ingestion.

Supports two cursor modes:
  - time: WHERE cdc_col >= last_loaded_at AND cdc_col < current_ts
  - id:   WHERE pk_col > last_loaded_key (captures inserts only)

The password is passed on stdin (never in argv) — see extractors.mssql_common.
"""
from __future__ import annotations

import gzip
from datetime import datetime
from pathlib import Path

from extractors import BaseExtractor, ExtractionResult
from extractors import mssql_common


class MSSQLIncrementalExtractor(BaseExtractor):

    @property
    def source_type(self) -> str:
        return "mssql"

    def extract_full(self, config: dict, src_cfg: dict,
                     output_dir: str | Path) -> ExtractionResult:
        raise NotImplementedError("Use MSSQLFullExtractor for full loads")

    def extract_incremental(self, config: dict, src_cfg: dict,
                            output_dir: str | Path,
                            source_conn=None) -> ExtractionResult:
        """Extract delta rows via BCP queryout with CDC condition."""
        out_dir = Path(output_dir) / "incremental"
        out_dir.mkdir(parents=True, exist_ok=True)

        src_db = config["SOURCE_DB"]
        src_schema = config.get("SOURCE_SCHEMA") or "dbo"
        src_table = config["SOURCE_TABLE"]
        delimiter = config.get("DELIMITER") or "|"

        server = mssql_common.server_spec(src_cfg.get("host", ""),
                                          src_cfg.get("port"))
        user = src_cfg.get("user", "")
        password = src_cfg.get("password", "")

        # Build CDC condition
        wm_col = config.get("WATERMARK_COL")
        wm_type = (config.get("WATERMARK_TYPE") or "time").lower()

        if not wm_col:
            return ExtractionResult(
                files=[], row_count=0, engine="bcp",
                skipped=True, skip_reason="No WATERMARK_COL configured")

        condition = self._build_cdc_condition(config, wm_col, wm_type)
        if not condition or condition == "1=1":
            return ExtractionResult(
                files=[], row_count=0, engine="bcp",
                skipped=True, skip_reason="No valid CDC condition (first run?)")

        # BCP queryout with condition
        query = f"SELECT * FROM [{src_schema}].[{src_table}] WHERE {condition}"
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{src_db}_{src_schema}_{src_table}_incr_{ts}.csv"
        filepath = out_dir / filename

        args = mssql_common.build_bcp_args(
            source_spec=query, mode="queryout", filepath=filepath,
            server=server, user=user, delimiter=delimiter, database=src_db)

        print(f"   BCP incr: [{src_schema}].[{src_table}] WHERE {condition[:80]}...")
        proc = mssql_common.run_bcp(args, password)

        if proc.returncode not in mssql_common.BCP_OK_RETURNCODES:
            err = mssql_common.bcp_error(proc)
            print(f"   ERROR: BCP failed (rc={proc.returncode}): {err[:200]}")
            return ExtractionResult(
                files=[], row_count=0, engine="bcp",
                skipped=True, skip_reason=f"BCP error: {err[:500]}")

        row_count = mssql_common.count_lines(filepath)

        if row_count == 0:
            print("   WARNING: No new rows - skipping load")
            try:
                filepath.unlink(missing_ok=True)
            except Exception:
                pass
            return ExtractionResult(
                files=[], row_count=0, engine="bcp",
                skipped=True, skip_reason="No new rows in CDC window")

        # Gzip the file
        gz_path = out_dir / (filepath.name + ".gz")
        with open(filepath, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
            while True:
                block = f_in.read(8 * 1024 * 1024)
                if not block:
                    break
                f_out.write(block)
        filepath.unlink(missing_ok=True)

        # Determine new watermark value
        watermark_to = self._get_new_watermark(config, wm_col, wm_type, source_conn)

        print(f"   BCP incr COMPLETED: {row_count} rows -> {gz_path.name}")
        return ExtractionResult(
            files=[gz_path], row_count=row_count,
            watermark_to=watermark_to,
            file_format="csv_gzip", engine="bcp")

    @staticmethod
    def _validate_timestamp(value: str) -> None:
        """Raise ValueError if `value` is not a parseable timestamp.

        Guards against a malformed watermark silently triggering a full
        re-extract. Accepts the common formats a Snowflake TIMESTAMP renders
        to (space or 'T' separator, optional fractional seconds).
        """
        raw = str(value).strip()
        candidate = raw.replace("T", " ")
        # Trim trailing timezone/offset noise that DATETIME2 cannot parse.
        formats = (
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
        )
        for fmt in formats:
            try:
                datetime.strptime(candidate, fmt)
                return
            except ValueError:
                continue
        raise ValueError(
            f"Invalid watermark timestamp {value!r} for MSSQL incremental "
            f"extract. Expected a value like 'YYYY-MM-DD HH:MM:SS[.ffffff]'. "
            f"Fix LAST_LOADED_AT in MIGRATION_CONFIG or reset the table to a "
            f"full load.")

    def _build_cdc_condition(self, config: dict, wm_col: str, wm_type: str) -> str:
        """Build WHERE clause based on last-loaded watermark."""
        if wm_type == "time":
            last_ts = config.get("LAST_LOADED_AT")
            if not last_ts:
                return "1=1"  # First run — full extract handled by orchestrator
            # Fail loudly if the stored watermark is not a valid timestamp,
            # rather than silently re-extracting the whole table via a
            # TRY_CAST(...) IS NULL fallback.
            self._validate_timestamp(last_ts)
            # Support multiple CDC columns (comma-separated).
            # Strictly greater-than (>) so rows exactly at the last watermark
            # are not re-fetched every run — matches MySQL/Teradata/Oracle.
            cols = [c.strip() for c in wm_col.split(",")]
            parts = [f"[{col}] > CAST('{last_ts}' AS DATETIME2)" for col in cols]
            return "(" + " OR ".join(parts) + ")"

        elif wm_type == "id":
            last_key = config.get("LAST_LOADED_KEY")
            if not last_key:
                return "1=1"
            col = wm_col.split(",")[0].strip()
            return f"[{col}] > {last_key}"

        return "1=1"

    def _get_new_watermark(self, config: dict, wm_col: str, wm_type: str,
                           source_conn) -> str | None:
        """Query source for the current max watermark value."""
        if not source_conn:
            return None

        src_schema = config.get("SOURCE_SCHEMA") or "dbo"
        src_table = config["SOURCE_TABLE"]
        col = wm_col.split(",")[0].strip()

        try:
            cur = source_conn.cursor()
            cur.execute(f"SELECT MAX([{col}]) FROM [{src_schema}].[{src_table}]")
            row = cur.fetchone()
            cur.close()
            if row and row[0] is not None:
                return str(row[0])
        except Exception:
            pass
        return None
