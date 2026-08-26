# Teradata full-load extraction via TPT (Teradata Parallel Transporter).
"""extractors.teradata_full — Full-load engine using TPT (tbuild).

Generates TPT export scripts dynamically, executes via `tbuild`, and returns
the resulting CSV files for Snowflake ingestion.
"""
from __future__ import annotations

import os
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path

from extractors import BaseExtractor, ExtractionResult


def _write_private(path: Path, content: str):
    """Write a file readable only by its owner (0600).

    O_CREAT's mode applies only when the file is newly created, so an explicit
    chmod follows to cover the pre-existing-file case. chmod is best-effort:
    it has limited meaning on Windows, where tbuild may also run.
    """
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        handle = os.fdopen(fd, "w", encoding="utf-8")
    except Exception:
        os.close(fd)
        raise
    with handle:
        handle.write(content)
    try:
        os.chmod(str(path), 0o600)
    except OSError:
        pass


def _shred(path: Path):
    """Delete a credential-bearing file, best-effort and never raising."""
    try:
        path.unlink(missing_ok=True)
    except OSError as e:
        print(f"   ⚠️ could not remove {path.name} (contains credentials): {e}")


class TeradataFullExtractor(BaseExtractor):

    @property
    def source_type(self) -> str:
        return "teradata"

    def extract_full(self, config: dict, src_cfg: dict,
                     output_dir: str | Path) -> ExtractionResult:
        """Run TPT export for full table extraction."""
        out_dir = Path(output_dir) / "full"
        if out_dir.exists():
            for p in sorted(out_dir.glob("**/*"), reverse=True):
                p.unlink() if p.is_file() else p.rmdir()
            out_dir.rmdir()
        out_dir.mkdir(parents=True, exist_ok=True)

        td_host = src_cfg["host"]
        td_user = src_cfg["user"]
        td_password = src_cfg.get("password", "")

        source_db = config["SOURCE_DB"]
        source_table = config["SOURCE_TABLE"]
        delimiter = config.get("DELIMITER", ",")
        trim_cols = config.get("TRIM", False)
        instance_count = config.get("TPT_INSTANCES", 4)
        filter_cond = config.get("FILTER_CONDITION")

        # Build column list from DBC.ColumnsV (via pre-fetched columns or query)
        columns = config.get("_columns")  # May be pre-populated by orchestrator
        if not columns:
            from ddl_generators.teradata import get_teradata_columns
            import teradatasql
            td_conn = teradatasql.connect(
                host=td_host, user=td_user, password=td_password,
                logmech=src_cfg.get("logmech", "TD2"))
            columns = get_teradata_columns(td_conn, source_db, source_table)
            td_conn.close()

        # Build SELECT statement
        # Always CAST to VARCHAR to match the DEFINE SCHEMA (all VARCHAR(64000)).
        # TPT validates that the Export Operator's output types match the schema;
        # without the CAST, INTEGER/DATE/DECIMAL columns cause TPT02639.
        col_names = [name for name, _ in columns]
        if trim_cols:
            select_cols = ", ".join(
                f"TRIM(CAST(\"{c}\" AS VARCHAR(64000))) AS \"{c}\"" for c in col_names)
        else:
            select_cols = ", ".join(
                f"CAST(\"{c}\" AS VARCHAR(64000)) AS \"{c}\"" for c in col_names)

        # Build WHERE condition
        condition = "(1=1)"
        if filter_cond:
            condition = f"({filter_cond})"

        select_stmt = (f"SELECT {select_cols} FROM {source_db}.{source_table} "
                       f"WHERE {condition}")
        # Escape single quotes for TPT script
        tpt_query = select_stmt.replace("'", "''")

        # Generate TPT script
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M")
        job_name = f"{source_db}_{source_table}_TPT_JOB"
        export_filename = f"{source_db}_{source_table}_TPT_{timestamp_str}.csv"
        schema_name = f"TPT_SCH_{source_table}"

        tpt_script = self._generate_tpt_script(
            job_name=job_name,
            schema_name=schema_name,
            col_names=col_names,
            export_dir=str(out_dir),
            export_filename=export_filename,
            delimiter=delimiter,
            td_host=td_host,
            td_user=td_user,
            td_password=td_password,
            select_stmt=tpt_query,
            instance_count=instance_count,
        )

        # Write TPT script to a private file. It embeds the Teradata password,
        # so it must not be world-readable and must not outlive the job.
        tpt_script_path = out_dir / f"{job_name}.tpt"
        _write_private(tpt_script_path, tpt_script)
        print(f"   TPT script: {tpt_script_path} (0600)")

        # Execute tbuild — the script is removed even if the job fails.
        job_checkpoint = export_filename.replace(".csv", "")
        cmd = ["tbuild", "-f", str(tpt_script_path), "-j", job_checkpoint,
               "-e", "UTF-8", "-C"]
        print(f"   running: {' '.join(cmd)}")

        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
        finally:
            _shred(tpt_script_path)

        if result.returncode != 0:
            raise RuntimeError(
                f"TPT export failed (rc={result.returncode}):\n{result.stderr or result.stdout}")

        # Parse row count from stdout
        row_count = self._parse_row_count(result.stdout)
        print(f"   TPT export complete: {row_count:,} rows exported")

        # Collect output CSV files (TPT splits by FileSizeMax)
        data_files = sorted(out_dir.glob(f"*{export_filename}*"))
        if not data_files:
            # TPT may append sequence numbers
            data_files = sorted(out_dir.glob("*.csv*"))
        if not data_files:
            raise RuntimeError(
                f"TPT produced no output files in {out_dir}")

        print(f"   output files: {len(data_files)}")
        return ExtractionResult(
            files=data_files,
            row_count=row_count,
            file_format="csv",
            engine="tpt",
        )

    def extract_incremental(self, config: dict, src_cfg: dict,
                            output_dir: str | Path,
                            source_conn=None) -> ExtractionResult:
        """Full extractor does not support incremental — delegate to
        TeradataIncrementalExtractor instead."""
        raise NotImplementedError(
            "Use TeradataIncrementalExtractor for incremental loads")

    def _generate_tpt_script(self, *, job_name: str, schema_name: str,
                             col_names: list[str], export_dir: str,
                             export_filename: str, delimiter: str,
                             td_host: str, td_user: str, td_password: str,
                             select_stmt: str, instance_count: int) -> str:
        """Generate TPT export script content."""
        # Schema definition (all columns as VARCHAR for export)
        # Quote column names to avoid conflicts with TPT reserved words (e.g. ID, DATE, TYPE)
        schema_cols = "\n".join(
            f"        {',' if i > 0 else ' '}\"{col}\" VARCHAR(64000)"
            for i, col in enumerate(col_names)
        )

        # TPT string literals escape a single quote by doubling it. Without
        # this, a credential containing an apostrophe produces a broken script.
        td_host_esc = (td_host or "").replace("'", "''")
        td_user_esc = (td_user or "").replace("'", "''")
        td_password_esc = (td_password or "").replace("'", "''")

        return f"""USING CHARACTER SET UTF8
DEFINE JOB {job_name}
DESCRIPTION 'DMT Full Export'
(
    DEFINE SCHEMA {schema_name} (
{schema_cols}
    );

    DEFINE OPERATOR FILE_WRITER_OPERATOR
    DESCRIPTION 'TPT Data Connector Consumer'
    TYPE DATACONNECTOR CONSUMER
    SCHEMA {schema_name}
    ATTRIBUTES
    (
        VARCHAR PrivateLogName = '{job_name}_log',
        VARCHAR DirectoryPath = '{export_dir}',
        VARCHAR FileName = '{export_filename}',
        VARCHAR IndicatorMode = 'N',
        VARCHAR OpenMode = 'Write',
        VARCHAR Format = 'Delimited',
        VARCHAR TextDelimiter = '{delimiter}',
        VARCHAR FileSizeMax = '52428800',
        VARCHAR QuotedData = 'Yes'
    );

    DEFINE OPERATOR EXPORT_OPERATOR
    DESCRIPTION 'TPT Export Operator'
    TYPE EXPORT
    SCHEMA {schema_name}
    ATTRIBUTES
    (
        VARCHAR PrivateLogName = '{job_name}_log',
        INTEGER MaxSessions = 16,
        INTEGER MinSessions = 1,
        VARCHAR TdpId = '{td_host_esc}',
        VARCHAR UserName = '{td_user_esc}',
        VARCHAR UserPassword = '{td_password_esc}',
        VARCHAR SelectStmt = '{select_stmt}'
    );

    APPLY TO OPERATOR (FILE_WRITER_OPERATOR[{instance_count}])
    SELECT * FROM OPERATOR (EXPORT_OPERATOR[{instance_count}]);
);
"""

    def _parse_row_count(self, stdout: str) -> int:
        """Parse 'Total Rows Exported: N' from TPT stdout."""
        try:
            idx = stdout.index("Total Rows Exported: ")
            remaining = stdout[idx + 21:]
            count_str = remaining.split("\n")[0].strip()
            return int(count_str.replace(",", ""))
        except (ValueError, IndexError):
            return 0
