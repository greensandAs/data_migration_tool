# Unified step-based pipeline orchestrator with retry-from-failure support.
"""orchestrator.py — Step-based pipeline engine with retry/resume.

Executes the migration pipeline as a sequence of discrete, independently
retryable steps. On failure, records the failed step in both PIPELINE_STEP_LOG
and MIGRATION_CONFIG.LAST_FAILED_STEP. On retry (--resume), skips completed
steps and resumes from the failure point.

Usage:
  python orchestrator.py                         # run all active tables
  python orchestrator.py --table T               # run single table
  python orchestrator.py --resume                # retry failed tables from failure point
  python orchestrator.py --full                  # force full reload
  python orchestrator.py --reconcile [--table T] # soft-delete reconciliation
  python orchestrator.py --validate [--deep]     # source vs target parity
"""
from __future__ import annotations

import os
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

# Ensure project root is on sys.path when run as subprocess (python core/orchestrator.py)
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import mysql.connector
import snowflake.connector

from metadata import config_manager
from metadata import connection_manager
from metadata import source_specs
from core import file_manifest
from core import loader
from metadata import run_log
from metadata import step_tracker
from ddl_generators import generate_and_apply, target_db, RAW_SCHEMA
from extractors.mysql_full import MySQLFullExtractor
from extractors.mysql_incremental import MySQLIncrementalExtractor
from storage import get_backend

try:
    from dotenv import load_dotenv
    from pathlib import Path as _P
    load_dotenv(_P(__file__).parent.parent / ".env")
except ImportError:
    pass
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    
DEFAULT_MAX_PARALLEL = 4


def _now_local():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ── Per-table log prefixing for parallel runs ─────────────────────────────────
_LOG_LOCAL = threading.local()


class _PrefixStream:
    """stdout wrapper that prepends the calling thread's tag to each line."""
    def __init__(self, base):
        self._base = base

    def write(self, text):
        tag = getattr(_LOG_LOCAL, "tag", None)
        if not tag:
            return self._base.write(text)
        buf = getattr(_LOG_LOCAL, "buf", "") + text
        parts = buf.split("\n")
        for line in parts[:-1]:
            self._base.write(f"{tag} {line}\n")
        _LOG_LOCAL.buf = parts[-1]
        return len(text)

    def flush(self):
        tag = getattr(_LOG_LOCAL, "tag", None)
        buf = getattr(_LOG_LOCAL, "buf", "")
        if tag and buf:
            self._base.write(f"{tag} {buf}")
            _LOG_LOCAL.buf = ""
        self._base.flush()

    def __getattr__(self, name):
        return getattr(self._base, name)


# ── Connection helpers ────────────────────────────────────────────────────────

def _build_sf_cfg() -> dict:
    """Build Snowflake connection config from environment."""
    cfg = {}
    env_map = {"account": "SF_ACCOUNT", "user": "SF_USER", "password": "SF_PASSWORD",
               "role": "SF_ROLE", "warehouse": "SF_WAREHOUSE",
               "database": "SF_DATABASE", "schema": "SF_SCHEMA"}
    for key, env in env_map.items():
        val = os.getenv(env)
        if val:
            cfg[key] = val
    cfg.setdefault("database", "HISTLOAD_DB")
    cfg.setdefault("schema", "META")
    return cfg


def _build_src_cfg(profile: dict) -> dict:
    """Build source connection config from profile + env overrides."""
    import json as _json
    source_type = (profile.get("SOURCE_TYPE") or "mysql").lower()
    # EXTRA_PARAMS may come back as a JSON string from Snowflake VARIANT column
    extras = profile.get("EXTRA_PARAMS") or {}
    if isinstance(extras, str):
        try:
            extras = _json.loads(extras)
        except (ValueError, TypeError):
            extras = {}
    if source_type == "teradata":
        return {
            "host": profile.get("HOST") or os.getenv("TD_HOST", ""),
            "user": profile.get("USERNAME") or os.getenv("TD_USER", ""),
            "password": profile.get("PASSWORD") or os.getenv("TD_PASSWORD", ""),
            "logmech": profile.get("LOGMECH") or os.getenv("TD_LOGMECH", "TD2"),
        }
    if source_type == "mssql":
        return {
            "host": profile.get("HOST") or os.getenv("MSSQL_SERVER", ""),
            "port": int(profile.get("PORT") or os.getenv("MSSQL_PORT", "1433")),
            "user": profile.get("USERNAME") or os.getenv("MSSQL_USER", ""),
            "password": profile.get("PASSWORD") or os.getenv("MSSQL_PASSWORD", ""),
            "driver": extras.get("driver")
                      or os.getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server"),
        }
    if source_type == "oracle":
        return {
            "host": profile.get("HOST") or os.getenv("ORACLE_HOST", ""),
            "port": int(profile.get("PORT") or os.getenv("ORACLE_PORT", "1521")),
            "user": profile.get("USERNAME") or os.getenv("ORACLE_USER", ""),
            "password": profile.get("PASSWORD") or os.getenv("ORACLE_PASSWORD", ""),
            "service_name": extras.get("service_name")
                            or os.getenv("ORACLE_SERVICE_NAME", ""),
        }
    # Default: MySQL
    out = {
        "host": profile.get("HOST") or os.getenv("MYSQL_HOST", "localhost"),
        "port": int(profile.get("PORT") or os.getenv("MYSQL_PORT", "3306")),
        "user": profile.get("USERNAME") or os.getenv("MYSQL_USER", "root"),
        "password": profile.get("PASSWORD") or os.getenv("MYSQL_PASSWORD", ""),
    }
    return out


def _mysql_connect(src_cfg: dict):
    return mysql.connector.connect(
        host=src_cfg["host"], port=int(src_cfg["port"]),
        user=src_cfg["user"], password=src_cfg["password"])


def _teradata_connect(src_cfg: dict):
    import teradatasql
    return teradatasql.connect(
        host=src_cfg["host"],
        user=src_cfg["user"],
        password=src_cfg["password"],
        logmech=src_cfg.get("logmech", "TD2"))


def _mssql_connect(src_cfg: dict, database: str | None = None):
    """Connect to MSSQL. `database` is required for INFORMATION_SCHEMA lookups.

    Unlike MySQL, MSSQL's INFORMATION_SCHEMA is scoped per-database, so the
    connection must target the source database or metadata queries return
    nothing.
    """
    import pyodbc
    driver = src_cfg.get("driver", "ODBC Driver 17 for SQL Server")
    server = src_cfg["host"]
    port = src_cfg.get("port", 1433)
    db_part = f"DATABASE={database};" if database else ""
    conn_str = (
        f"DRIVER={{{driver}}};SERVER={server},{port};{db_part}"
        f"UID={src_cfg['user']};PWD={src_cfg['password']};"
        "Encrypt=yes;TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def _oracle_connect(src_cfg: dict):
    """Connect to Oracle via Easy Connect (host:port/service_name)."""
    import oracledb
    service = src_cfg.get("service_name") or ""
    if not service:
        raise ValueError(
            "Oracle connection requires a service_name "
            "(set EXTRA_PARAMS.service_name on the connection profile).")
    dsn = f"{src_cfg['host']}:{src_cfg.get('port', 1521)}/{service}"
    return oracledb.connect(
        user=src_cfg["user"], password=src_cfg["password"], dsn=dsn)


def _refetch_columns(source_conn, config: dict, source_type: str) -> list[tuple]:
    """Re-read source columns when the ddl step was skipped (LOAD_ONLY).

    Must dispatch on source_type: get_mysql_columns() calls
    cursor(dictionary=True), a mysql-connector-only kwarg that raises TypeError
    on a pyodbc or teradatasql cursor.
    """
    if source_type == "teradata":
        from ddl_generators.teradata import get_teradata_columns
        return get_teradata_columns(
            source_conn, config["SOURCE_DB"], config["SOURCE_TABLE"])
    if source_type == "mssql":
        from ddl_generators.mssql import get_mssql_columns
        return get_mssql_columns(
            source_conn, config["SOURCE_DB"],
            config.get("SOURCE_SCHEMA") or "dbo",
            config["SOURCE_TABLE"])
    if source_type == "oracle":
        from ddl_generators.oracle import get_oracle_columns
        return get_oracle_columns(
            source_conn, config["SOURCE_DB"], config["SOURCE_TABLE"])
    from ddl_generators.mysql import get_mysql_columns
    return get_mysql_columns(
        source_conn, config["SOURCE_DB"], config["SOURCE_TABLE"],
        blob_mode=config.get("BLOB_MODE", "binary"))


def _load_file_format(extraction_result, pending_manifest, source_type: str,
                      is_full: bool) -> str:
    """Decide which file format the loader should expect.

    Precedence:
      1. The ExtractionResult from this run (most accurate).
      2. FILE_MANIFEST rows from a prior extract — this is what makes LOAD_ONLY
         work, since the extract step never ran in this process.
      3. The source's declared output format, as a last resort.
    """
    if extraction_result is not None:
        return extraction_result.file_format
    if pending_manifest:
        fmts = {m.get("FILE_FORMAT") for m in pending_manifest if m.get("FILE_FORMAT")}
        if len(fmts) == 1:
            return fmts.pop()
        if len(fmts) > 1:
            print(f"   ⚠️ manifest has mixed file formats {sorted(fmts)} - "
                  f"falling back to the source default")
    return source_specs.output_format(source_type, is_full)


def _source_connect(source_type: str, src_cfg: dict, database: str | None = None):
    """Connect to source database based on source type.

    `database` is only used by MSSQL, where INFORMATION_SCHEMA is per-database.
    """
    if source_type == "teradata":
        return _teradata_connect(src_cfg)
    if source_type == "mssql":
        return _mssql_connect(src_cfg, database=database)
    if source_type == "oracle":
        return _oracle_connect(src_cfg)
    return _mysql_connect(src_cfg)


# Transient connection errors worth retrying (network drops, forced closes,
# timeouts). Matched case-insensitively against the exception's string form,
# since each driver raises its own exception class.
_RETRYABLE_CONN_HINTS = (
    "forcibly closed",          # MSSQL/ODBC 10054
    "communication link failure",
    "connection reset",
    "10054",
    "08s01",                    # ODBC transient connection state
    "timeout",
    "timed out",
    "connection refused",
    "broken pipe",
    "server has gone away",     # MySQL 2006
    "lost connection",          # MySQL 2013
    "network error",
)


def _is_retryable_conn_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(hint in msg for hint in _RETRYABLE_CONN_HINTS)


def _source_connect_with_retry(source_type: str, src_cfg: dict,
                               database: str | None = None,
                               max_attempts: int = 3,
                               base_delay: float = 2.0):
    """Connect to the source with retry + exponential backoff.

    Only transient network-level failures are retried (see
    _RETRYABLE_CONN_HINTS); auth or configuration errors fail immediately.
    """
    attempt = 1
    while True:
        try:
            return _source_connect(source_type, src_cfg, database=database)
        except Exception as e:
            if attempt >= max_attempts or not _is_retryable_conn_error(e):
                raise
            delay = base_delay * (2 ** (attempt - 1))
            print(f"   source connect attempt {attempt}/{max_attempts} failed "
                  f"({str(e)[:120]}); retrying in {delay:.1f}s")
            time.sleep(delay)
            attempt += 1


# ── Main run entry point ──────────────────────────────────────────────────────

def run(force_full: bool = False, only_table: str | None = None,
        only_tables: list[str] | None = None,
        resume: bool = False, max_parallel: int = DEFAULT_MAX_PARALLEL,
        execution_mode: str | None = None, profile: str | None = None):
    """Main pipeline execution.
    
    execution_mode: override per-table EXECUTION_MODE. One of FULL, EXTRACT_ONLY, LOAD_ONLY.
    profile: filter tables to only those belonging to this CONNECTION_PROFILE.
    only_table: run a single table (backward compat).
    only_tables: run a specific set of tables.
    """
    sf_cfg = _build_sf_cfg()
    sf_conn = loader.get_sf_conn(sf_cfg)
    cur = sf_conn.cursor()

    batch_id = uuid.uuid4().hex[:12]

    # Load active configs from Snowflake (filtered by profile if specified)
    configs = config_manager.list_active(cur, connection_profile=profile)
    if only_table:
        configs = [c for c in configs if c["SOURCE_TABLE"] == only_table]
    elif only_tables:
        tbl_set = set(t.upper() for t in only_tables)
        configs = [c for c in configs if c["SOURCE_TABLE"].upper() in tbl_set]

    if not configs:
        print("No active tables to process.")
        cur.close()
        sf_conn.close()
        return 0

    print("=" * 64)
    print(f" DMT v1 Pipeline | batch {batch_id} | {_now_local()}")
    print(f" tables: {len(configs)} | parallelism: {min(max_parallel, len(configs))}")
    print("=" * 64)

    # Resolve connection profiles (fetch all before closing initial connection)
    profiles_cache = {}
    for cfg in configs:
        pname = cfg.get("CONNECTION_PROFILE")
        if pname and pname not in profiles_cache:
            profiles_cache[pname] = connection_manager.get_profile(cur, pname)

    def _get_profile(profile_name):
        return profiles_cache.get(profile_name)

    cur.close()
    sf_conn.close()

    failed = 0
    _orig_stdout = sys.stdout
    sys.stdout = _PrefixStream(_orig_stdout)
    try:
        if max_parallel == 1 or len(configs) == 1:
            for cfg in configs:
                if _process_table(cfg, sf_cfg, _get_profile, batch_id,
                                  force_full=force_full, resume=resume,
                                  mode_override=execution_mode) == "failed":
                    failed += 1
        else:
            with ThreadPoolExecutor(max_workers=max_parallel) as ex:
                futures = {
                    ex.submit(_process_table, cfg, sf_cfg, _get_profile,
                              batch_id, force_full=force_full, resume=resume,
                              mode_override=execution_mode): cfg
                    for cfg in configs
                }
                for fut in as_completed(futures):
                    try:
                        if fut.result() == "failed":
                            failed += 1
                    except Exception as e:
                        failed += 1
                        print(f"   worker error: {e}")
    finally:
        sys.stdout = _orig_stdout

    print(f"\nRun complete: {_now_local()} (batch {batch_id}) | "
          f"failed: {failed}/{len(configs)}")
    return failed


def _resolve_bucket(storage_type: str, config: dict, cur) -> str:
    """Resolve the S3/Azure bucket name.

    Priority: DMT_S3_BUCKET env var -> DESCRIBE STAGE from STORAGE_PATH.
    """
    env_key = "DMT_S3_BUCKET" if storage_type == "s3" else "DMT_AZURE_CONTAINER"
    bucket = os.getenv(env_key, "").strip()
    if bucket:
        return bucket

    # Fallback: extract bucket from stage URL via DESCRIBE STAGE
    stage_name = config.get("STORAGE_PATH") or ""
    if not stage_name:
        raise ValueError(f"No {env_key} env var and no STORAGE_PATH in config")

    fq_stage = f"HISTLOAD_DB.META.{stage_name}" if "." not in stage_name else stage_name
    try:
        cur.execute(f"DESCRIBE STAGE {fq_stage}")
        rows = cur.fetchall()
        # DESCRIBE STAGE columns: parent_property, property, property_type, property_value, property_default
        for row in rows:
            parent_prop = (row[0] or "").upper()
            prop_name = (row[1] or "").upper() if len(row) > 1 else ""
            prop_value = row[3] if len(row) > 3 else ""
            if parent_prop == "STAGE_LOCATION" and prop_name == "URL":
                # property_value may be JSON array like '["s3://ta-dmt/"]' or plain string
                url_str = str(prop_value).strip()
                if url_str.startswith("["):
                    import json as _json
                    try:
                        urls = _json.loads(url_str)
                        url_str = urls[0] if urls else ""
                    except (ValueError, IndexError):
                        url_str = url_str.strip("[]\"' ")
                url_str = url_str.strip().rstrip("/")
                if "://" in url_str:
                    parts = url_str.split("://", 1)[1].split("/", 1)
                    return parts[0]
                return url_str
    except Exception as e:
        raise ValueError(
            f"Cannot resolve bucket from stage '{fq_stage}': {e}. "
            f"Set {env_key} env var or ensure STORAGE_PATH points to a valid stage."
        ) from e

    raise ValueError(
        f"Could not find URL property in DESCRIBE STAGE {fq_stage}. "
        f"Set {env_key} env var instead."
    )


def _process_table(config: dict, sf_cfg: dict, get_profile, batch_id: str,
                   force_full: bool = False, resume: bool = False,
                   mode_override: str | None = None) -> str:
    """Execute the pipeline for a single table, step by step."""
    _LOG_LOCAL.tag = f"[{config['SOURCE_DB']}.{config['SOURCE_TABLE']}]"

    sf_conn = loader.get_sf_conn(sf_cfg)
    cur = sf_conn.cursor()

    profile = get_profile(config["CONNECTION_PROFILE"])
    src_cfg = _build_src_cfg(profile)
    source_type = (profile.get("SOURCE_TYPE") or "mysql").lower()

    # Fail fast for source types that can be configured but have no extractor yet.
    from metadata.source_specs import extractor_ready, source_label
    if not extractor_ready(source_type):
        cur.close()
        sf_conn.close()
        raise NotImplementedError(
            f"{source_label(source_type)} connections can be configured and tested, "
            f"but no extractor is implemented yet. Supported for migration: "
            f"mysql, teradata, mssql, oracle.")

    # MSSQL needs the database at connect time (INFORMATION_SCHEMA is per-database).
    source_conn = _source_connect_with_retry(source_type, src_cfg,
                                             database=config["SOURCE_DB"])

    run_id = uuid.uuid4().hex[:16]
    config_id = config["CONFIG_ID"]
    first_run = config.get("LAST_LOADED_AT") is None
    is_full = force_full or first_run or config.get("LOAD_TYPE") == "full"
    # CLI mode_override takes precedence over per-table EXECUTION_MODE
    execution_mode = mode_override or config.get("EXECUTION_MODE", "FULL")

    # Determine which steps to execute based on execution mode + storage type
    storage_type = config.get("STORAGE_TYPE", "internal_stage")
    if execution_mode == "EXTRACT_ONLY":
        if storage_type in ("s3", "azure"):
            steps = ["ddl", "schema_drift", "extract", "upload"]
        else:
            steps = ["ddl", "schema_drift", "extract"]
    elif execution_mode == "LOAD_ONLY":
        steps = ["load", "merge", "watermark"]
    elif storage_type == "local":
        steps = ["ddl", "schema_drift", "extract"]
    else:
        steps = ["ddl", "schema_drift", "extract", "upload", "load", "merge", "watermark"]

    # Resume logic: find where we left off
    resume_from = None
    if resume and config.get("LAST_FAILED_STEP"):
        resume_from = config["LAST_FAILED_STEP"]
        last_run_id = config.get("LAST_RUN_ID")
        if last_run_id:
            run_id = last_run_id  # continue the same run
            print(f"   RESUMING from step '{resume_from}' (run {run_id})")

    # Initialize step tracking
    if not resume_from:
        step_tracker.init_steps(cur, run_id, config_id,
                                config["SOURCE_DB"], config["SOURCE_TABLE"], steps)
        sf_conn.commit()

    engine = source_specs.engine_name(source_type, is_full)
    label = "FULL" if is_full else "INCREMENTAL"
    print(f"[{label}/{engine}] {config['SOURCE_DB']}.{config['SOURCE_TABLE']} "
          f"-> {loader.raw_table(config)}")

    rec = {
        "batch_id": batch_id, "config_id": config_id,
        "connection_profile": config["CONNECTION_PROFILE"],
        "source_db": config["SOURCE_DB"], "source_table": config["SOURCE_TABLE"],
        "target_db": config.get("TARGET_DB") or target_db(config["SOURCE_DB"]),
        "target_table": config.get("TARGET_TABLE") or config["SOURCE_TABLE"].upper(),
        "load_type": label.lower(), "engine": engine,
        "rows_extracted": 0, "rows_loaded": 0,
        "watermark_from": config.get("LAST_LOADED_AT"),
        "watermark_to": None, "watermark_type": config.get("WATERMARK_TYPE"),
        "status": "failed", "error_message": None, "failed_step": None,
        "duration_sec": None, "run_start": _now_local(), "run_end": None,
    }

    t0 = time.monotonic()
    extraction_result = None
    columns = None
    current_step = None
    is_single = (len(steps) <= 3)  # LOAD_ONLY or single table = verbose

    # LOAD_ONLY skips the extract step, so there is no ExtractionResult to tell
    # the loader what was produced. Recover it from FILE_MANIFEST, which the
    # earlier EXTRACT_ONLY run populated with each file's path and format.
    pending_manifest = []
    if execution_mode == "LOAD_ONLY":
        try:
            pending_manifest = file_manifest.get_pending_files(cur, config_id)
        except Exception as me:
            print(f"   ⚠️ could not read FILE_MANIFEST: {me}")
        if pending_manifest:
            print(f"   LOAD_ONLY: {len(pending_manifest)} pending file(s) "
                  f"from a prior extract")
        else:
            print("   LOAD_ONLY: no pending files in FILE_MANIFEST - "
                  "run EXTRACT first, or the files were already loaded")

    try:
        for step_name in steps:
            # Skip steps before resume point
            if resume_from and step_name != resume_from:
                existing = step_tracker.get_steps(cur, run_id)
                step_entry = next((s for s in existing if s["STEP_NAME"] == step_name), None)
                if step_entry and step_entry["STATUS"] in ("success", "skipped"):
                    continue
            resume_from = None

            current_step = step_name
            step_t0 = time.monotonic()
            step_tracker.mark_running(cur, run_id, step_name)
            sf_conn.commit()

            if step_name == "ddl":
                if source_type == "teradata":
                    from ddl_generators.teradata import generate_and_apply as td_generate_and_apply
                    meta = td_generate_and_apply(sf_conn, source_conn, config)
                    # Propagate resolved names to config for downstream steps
                    config["TARGET_TABLE"] = meta["target_table"]
                    config["TARGET_SCHEMA"] = meta["target_schema"]
                elif source_type == "mssql":
                    from ddl_generators.mssql import generate_and_apply as mssql_generate_and_apply
                    meta = mssql_generate_and_apply(sf_conn, source_conn, config)
                elif source_type == "oracle":
                    from ddl_generators.oracle import generate_and_apply as oracle_generate_and_apply
                    meta = oracle_generate_and_apply(sf_conn, source_conn, config)
                else:
                    meta = generate_and_apply(sf_conn, source_conn, config)
                columns = meta["columns"]
                step_tracker.mark_success(cur, run_id, step_name,
                                          {"columns_count": len(columns)})

            elif step_name == "schema_drift":
                from core import schema_drift
                drift = schema_drift.detect_and_apply(cur, source_conn, config,
                                                      source_type=source_type)
                step_tracker.mark_success(cur, run_id, step_name, drift)

            elif step_name == "extract":
                conn_name = config.get("CONNECTION_PROFILE", "default")
                export_base = str(Path("./export") / source_type / conn_name / config["SOURCE_TABLE"])

                if source_type == "teradata":
                    if is_full:
                        from extractors.teradata_full import TeradataFullExtractor
                        extractor = TeradataFullExtractor()
                        extraction_result = extractor.extract_full(
                            config, src_cfg, export_base)
                    else:
                        from extractors.teradata_incremental import TeradataIncrementalExtractor
                        extractor = TeradataIncrementalExtractor()
                        extraction_result = extractor.extract_incremental(
                            config, src_cfg, export_base, source_conn=source_conn)
                elif source_type == "mssql":
                    if is_full:
                        from extractors.mssql_full import MSSQLFullExtractor
                        extractor = MSSQLFullExtractor()
                        extraction_result = extractor.extract_full(
                            config, src_cfg, export_base)
                    else:
                        from extractors.mssql_incremental import MSSQLIncrementalExtractor
                        extractor = MSSQLIncrementalExtractor()
                        extraction_result = extractor.extract_incremental(
                            config, src_cfg, export_base, source_conn=source_conn)
                elif source_type == "oracle":
                    if is_full:
                        from extractors.oracle_full import OracleFullExtractor
                        extractor = OracleFullExtractor()
                        extraction_result = extractor.extract_full(
                            config, src_cfg, export_base)
                    else:
                        from extractors.oracle_incremental import OracleIncrementalExtractor
                        extractor = OracleIncrementalExtractor()
                        extraction_result = extractor.extract_incremental(
                            config, src_cfg, export_base, source_conn=source_conn)
                else:
                    if is_full:
                        extractor = MySQLFullExtractor()
                        extraction_result = extractor.extract_full(
                            config, src_cfg, export_base)
                    else:
                        extractor = MySQLIncrementalExtractor()
                        extraction_result = extractor.extract_incremental(
                            config, src_cfg, export_base, source_conn=source_conn)

                if extraction_result.skipped:
                    step_tracker.mark_skipped(cur, run_id, step_name,
                                             extraction_result.skip_reason)
                    rec["status"] = "skipped"
                    print(f"   skipped: {extraction_result.skip_reason}")
                    break
                else:
                    rec["rows_extracted"] = extraction_result.row_count
                    if extraction_result.row_count > 0:
                        print(f"   extracted: {extraction_result.row_count:,} rows -> "
                              f"{len(extraction_result.files)} file(s)")
                    else:
                        print(f"   extracted: {len(extraction_result.files)} file(s)")
                    file_manifest.register_files(
                        cur, run_id=run_id, config_id=config_id,
                        source_db=config["SOURCE_DB"],
                        source_table=config["SOURCE_TABLE"],
                        files=[{
                            "file_path": str(f),
                            "storage_type": "local",
                            "file_format": extraction_result.file_format,
                            "part_number": i,
                        } for i, f in enumerate(extraction_result.files)]
                    )
                    step_tracker.mark_success(cur, run_id, step_name, {
                        "files": len(extraction_result.files),
                        "rows": extraction_result.row_count,
                    })

            elif step_name == "upload":
                storage_type = config.get("STORAGE_TYPE", "internal_stage")
                if storage_type == "internal_stage":
                    if extraction_result and extraction_result.files:
                        sub = "full" if is_full else "incremental"
                        loader.clear_stage_safe(cur, config, sub)
                        loader.put_files_parallel(
                            sf_cfg, extraction_result.files, config, sub)
                else:
                    # Resolve bucket: env var first, then query stage definition
                    bucket = _resolve_bucket(storage_type, config, cur)

                    # Auto-structure: dmt/<source>/<connection>/<schema>/<table>/<full|incremental>/
                    source_type = profile.get("SOURCE_TYPE", "mysql") if profile else "mysql"
                    conn_name = config.get("CONNECTION_PROFILE", "default")
                    sub = "full" if is_full else "incremental"
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    full_prefix = f"dmt/{source_type}/{conn_name}/{config['SOURCE_DB']}/{config['SOURCE_TABLE']}/{sub}"

                    backend = get_backend(storage_type, bucket=bucket, prefix=full_prefix)

                    if extraction_result and extraction_result.files:
                        uploaded_paths = []
                        for i, f in enumerate(extraction_result.files):
                            # Filename: <table>_<timestamp>_part<N>.<ext>
                            ext = "".join(f.suffixes)  # .tsv.zst or .parquet
                            remote_name = f"{config['SOURCE_TABLE']}_{timestamp}_part{i:04d}{ext}"
                            remote_uri = backend.upload(f, remote_name)
                            uploaded_paths.append(remote_uri)
                        print(f"   uploaded: {len(uploaded_paths)} file(s) -> "
                              f"{storage_type}://{bucket}/{full_prefix}/")
                        for p in uploaded_paths[:3]:
                            print(f"     {p}")
                        if len(uploaded_paths) > 3:
                            print(f"     ... and {len(uploaded_paths) - 3} more")

                        # Update manifest: mark entries as uploaded with S3 paths
                        manifest_entries = file_manifest.get_files_for_run(cur, run_id, config_id)
                        for entry, uri in zip(manifest_entries, uploaded_paths):
                            mid = entry.get("MANIFEST_ID")
                            if mid:
                                cur.execute(
                                    f"UPDATE {file_manifest._TABLE} "
                                    "SET STORAGE_TYPE = %s, FILE_PATH = %s, STATUS = 'uploaded', "
                                    "UPLOADED_AT = CURRENT_TIMESTAMP() "
                                    "WHERE MANIFEST_ID = %s",
                                    (storage_type, uri, mid))
                        sf_conn.commit()

                step_tracker.mark_success(cur, run_id, step_name,
                                          {"storage": storage_type})

                # Inline cleanup: delete local files after successful upload to S3/Azure
                if storage_type in ("s3", "azure") and extraction_result and extraction_result.files:
                    try:
                        for f in extraction_result.files:
                            if f.exists():
                                f.unlink()
                        # Remove empty parent directories
                        export_dir = Path(extraction_result.files[0]).parent
                        if export_dir.exists() and not any(export_dir.iterdir()):
                            export_dir.rmdir()
                        print(f"   cleanup: deleted {len(extraction_result.files)} local file(s)")
                    except Exception as ce:
                        print(f"   cleanup WARNING: {ce}")

            elif step_name == "load":
                sub = "full" if is_full else "incremental"
                storage_type = config.get("STORAGE_TYPE", "internal_stage")

                if storage_type in ("s3", "azure"):
                    # Load from external stage (storage integration)
                    source_type = profile.get("SOURCE_TYPE", "mysql") if profile else "mysql"
                    ext_path = loader.ext_stage_path(config, sub, source_type)
                    # Format must follow what the extractor produced, not the
                    # load mode. In LOAD_ONLY this comes from FILE_MANIFEST.
                    load_fmt = _load_file_format(extraction_result, pending_manifest,
                                                 source_type, is_full)
                    _spec = loader.resolve_format(load_fmt)
                    fmt = _spec["fmt"]
                    pattern = _spec["pattern"]
                    match_by = _spec["match_by"]

                    # List files on external stage
                    try:
                        cur.execute(f"LIST '{ext_path}/'")
                        ext_files = cur.fetchall()
                    except Exception:
                        ext_files = []

                    if not ext_files:
                        print(f"   load: no files on external stage ({ext_path}) -- skipping")
                        step_tracker.mark_skipped(cur, run_id, step_name, "no files on external stage")
                        continue

                    print(f"   load: {len(ext_files)} file(s) on external stage ({storage_type})")
                    for ef in ext_files[:5]:
                        print(f"     {ef[0]}")

                    if is_full:
                        # Full load from external stage
                        fqn = loader.raw_table(config)
                        scd_type = int(config.get("SCD_TYPE") or 1)
                        if columns is None:
                            columns = _refetch_columns(source_conn, config,
                                                       source_type)

                        if scd_type == 2:
                            # SCD2: route full load through merge (preserves history)
                            rec["rows_loaded"] = loader.copy_into_merge(
                                cur, config, batch_id,
                                file_format=load_fmt,
                                stage_override=ext_path)
                        else:
                            # SCD0/SCD1: TRUNCATE + COPY (replace all).
                            # purge=False — files are moved to processed/ below.
                            rec["rows_loaded"] = loader.copy_into_full(
                                cur, config, columns, batch_id,
                                file_format=load_fmt,
                                stage_override=ext_path,
                                purge=False)
                        print(f"   loaded: {rec['rows_loaded']:,} rows from {storage_type}")
                    # For incremental, merge step handles it (below)

                    # Move loaded files to processed/<sub>/<date>/ on S3
                    if rec["rows_loaded"] > 0 or (not is_full):
                        try:
                            from storage.s3 import S3Storage
                            raw_path = config.get("STORAGE_PATH") or ""
                            # Parse bucket from external stage URL
                            cur.execute(f"SHOW STAGES LIKE '{raw_path}' IN HISTLOAD_DB.META")
                            stage_info = cur.fetchone()
                            if stage_info:
                                stage_cols = [d[0] for d in cur.description]
                                stage_dict = dict(zip(stage_cols, stage_info))
                                bucket_url = stage_dict.get("url", "")
                                bucket_name = bucket_url.replace("s3://", "").strip("/").split("/")[0]
                            else:
                                bucket_name = "ta-dmt"  # fallback

                            s3 = S3Storage(bucket=bucket_name)
                            # Get the keys of files we just loaded
                            source_type_str = profile.get("SOURCE_TYPE", "mysql") if profile else "mysql"
                            conn_name_str = config.get("CONNECTION_PROFILE", "default")
                            prefix = f"dmt/{source_type_str}/{conn_name_str}/{config['SOURCE_DB']}/{config['SOURCE_TABLE']}/{sub}"
                            file_keys = s3.list_files(prefix)
                            if file_keys:
                                date_str = datetime.now().strftime("%Y%m%d")
                                moved = s3.move_to_processed(file_keys, sub, date_str)
                                print(f"   moved {len(moved)} file(s) to processed/{sub}/{date_str}/")
                        except Exception as move_err:
                            print(f"   WARNING: move to processed failed: {move_err}")

                else:
                    # Load from internal stage
                    stage_files = loader.list_stage_files(cur, config, sub)
                    if not stage_files:
                        print(f"   load: no files on stage ({sub}) -- skipping")
                        step_tracker.mark_skipped(cur, run_id, step_name, "no files on stage")
                        continue
                    else:
                        print(f"   load: {len(stage_files)} file(s) on stage:")
                        for sf in stage_files:
                            fname = sf.get("name", "?")
                            fsize = sf.get("size", 0)
                            print(f"     {fname} ({fsize:,} bytes)")

                    if is_full:
                        scd_type = int(config.get("SCD_TYPE") or 1)
                        if columns is None:
                            columns = _refetch_columns(source_conn, config,
                                                       source_type)
                        # Follow the extractor's actual output format; in
                        # LOAD_ONLY this comes from FILE_MANIFEST.
                        load_fmt = _load_file_format(extraction_result,
                                                     pending_manifest,
                                                     source_type, is_full)
                        if scd_type == 2:
                            # SCD2: route full load through merge (preserves history)
                            rec["rows_loaded"] = loader.copy_into_merge(
                                cur, config, batch_id, file_format=load_fmt)
                        else:
                            # SCD0/SCD1: TRUNCATE + COPY (replace all)
                            rec["rows_loaded"] = loader.copy_into_full(
                                cur, config, columns, batch_id,
                                file_format=load_fmt)
                        rec["rows_extracted"] = rec["rows_loaded"]

                step_tracker.mark_success(cur, run_id, step_name,
                                          {"rows": rec["rows_loaded"]})

                # Inline cleanup: delete local files after successful load from internal stage
                if storage_type == "internal_stage" and extraction_result and extraction_result.files:
                    try:
                        deleted = 0
                        for f in extraction_result.files:
                            if f.exists():
                                f.unlink()
                                deleted += 1
                        # Remove empty parent directories
                        if extraction_result.files:
                            export_dir = Path(extraction_result.files[0]).parent
                            if export_dir.exists() and not any(export_dir.iterdir()):
                                export_dir.rmdir()
                        if deleted:
                            print(f"   cleanup: deleted {deleted} local file(s)")
                    except Exception as ce:
                        print(f"   cleanup WARNING: {ce}")

            elif step_name == "merge":
                # Data to merge comes either from this run's extract, or — in
                # LOAD_ONLY — from files a prior extract left in FILE_MANIFEST.
                fresh_extract = (extraction_result is not None
                                 and not extraction_result.skipped)
                has_data = fresh_extract or bool(pending_manifest)

                if not is_full and has_data:
                    # For S3/Azure, pass external stage path to merge
                    merge_stage = None
                    storage_type = config.get("STORAGE_TYPE", "internal_stage")
                    if storage_type in ("s3", "azure"):
                        source_type = profile.get("SOURCE_TYPE", "mysql") if profile else "mysql"
                        merge_stage = loader.ext_stage_path(config, "incremental", source_type)
                    rec["rows_loaded"] = loader.copy_into_merge(
                        cur, config, batch_id,
                        file_format=_load_file_format(extraction_result,
                                                      pending_manifest,
                                                      source_type, is_full),
                        stage_override=merge_stage)
                    print(f"   merged: {rec['rows_loaded']:,} rows")

                    # Retire the manifest rows we just consumed so a repeat
                    # LOAD_ONLY does not re-merge the same files.
                    if pending_manifest and rec["rows_loaded"] > 0:
                        for m in pending_manifest:
                            try:
                                file_manifest.mark_loaded(cur, m["MANIFEST_ID"])
                            except Exception:
                                pass
                elif not is_full:
                    print("   merge: nothing to merge (no extract this run and "
                          "no pending files)")
                    step_tracker.mark_skipped(cur, run_id, step_name,
                                              "no new data")
                    continue

                # Archive: move S3/Azure files to processed/ after successful merge
                if not is_full and storage_type in ("s3", "azure") and rec["rows_loaded"] > 0:
                    try:
                        from storage.s3 import S3Storage
                        raw_path = config.get("STORAGE_PATH") or ""
                        cur.execute(f"SHOW STAGES LIKE '{raw_path}' IN HISTLOAD_DB.META")
                        stage_info = cur.fetchone()
                        if stage_info:
                            stage_cols = [d[0] for d in cur.description]
                            stage_dict = dict(zip(stage_cols, stage_info))
                            bucket_url = stage_dict.get("url", "")
                            bucket_name = bucket_url.replace("s3://", "").strip("/").split("/")[0]
                        else:
                            bucket_name = _resolve_bucket(storage_type, config, cur)

                        s3 = S3Storage(bucket=bucket_name)
                        source_type_str = profile.get("SOURCE_TYPE", "mysql") if profile else "mysql"
                        conn_name_str = config.get("CONNECTION_PROFILE", "default")
                        prefix = f"dmt/{source_type_str}/{conn_name_str}/{config['SOURCE_DB']}/{config['SOURCE_TABLE']}/incremental"
                        file_keys = s3.list_files(prefix)
                        if file_keys:
                            date_str = datetime.now().strftime("%Y%m%d")
                            moved = s3.move_to_processed(file_keys, "incremental", date_str)
                            print(f"   archive: moved {len(moved)} file(s) to processed/incremental/{date_str}/")
                    except Exception as archive_err:
                        print(f"   archive WARNING: {archive_err}")

                step_tracker.mark_success(cur, run_id, step_name,
                                          {"rows": rec["rows_loaded"]})

            elif step_name == "watermark":
                wm_col = config.get("WATERMARK_COL")
                wm_type = config.get("WATERMARK_TYPE")
                wm = None
                if wm_col:
                    wm = loader.current_max_watermark(cur, config, wm_col)
                elif extraction_result and extraction_result.watermark_to:
                    wm = extraction_result.watermark_to

                if wm:
                    rec["watermark_to"] = wm
                    print(f"   watermark: {rec['watermark_from']} -> {wm}")
                    if wm_type == "id":
                        config_manager.update_watermark(
                            cur, config_id, status="success",
                            last_loaded_key=wm,
                            last_loaded_at=_now_local(),
                            last_run_id=run_id)
                    else:
                        config_manager.update_watermark(
                            cur, config_id, status="success",
                            last_loaded_at=wm,
                            last_run_id=run_id)
                else:
                    config_manager.update_watermark(
                        cur, config_id, status="success",
                        last_loaded_at=_now_local(),
                        last_run_id=run_id)
                rec["status"] = "success"
                step_tracker.mark_success(cur, run_id, step_name,
                                          {"watermark_to": wm})

                # Cleanup old manifest entries (keep last 5 runs per config)
                try:
                    file_manifest.cleanup_old_manifests(cur, config_id, keep_runs=5)
                except Exception:
                    pass

            sf_conn.commit()

        if rec["status"] != "failed":
            if rec["status"] != "skipped":
                rec["status"] = "success"
            total_sec = time.monotonic() - t0
            if storage_type == "local":
                print(f"   done ({rec['status']}) - "
                      f"{rec['rows_extracted']:,} extracted to local (no load) - "
                      f"{total_sec:.1f}s")
                print(f"   NOTE: storage=local - files extracted only, no upload/load performed")
            else:
                print(f"   done ({rec['status']}) - "
                      f"{rec['rows_extracted']:,} extracted, {rec['rows_loaded']:,} loaded, "
                      f"{total_sec:.1f}s")

    except Exception as e:
        sf_conn.rollback()
        rec["status"] = "failed"
        rec["failed_step"] = current_step
        rec["error_message"] = str(e)[:4000]
        step_tracker.mark_failed(cur, run_id, current_step, str(e))
        config_manager.update_watermark(
            cur, config_id, status="failed",
            last_run_id=run_id, last_failed_step=current_step)
        sf_conn.commit()
        print(f"   FAILED at step '{current_step}': {e}")

    finally:
        rec["run_end"] = _now_local()
        rec["duration_sec"] = round(time.monotonic() - t0, 2)
        try:
            run_log.write_run_log(cur, rec)
            sf_conn.commit()
        except Exception as le:
            print(f"   (run_log write failed: {le})")

        # Evaluate alert rules after each run
        try:
            from core.alerts import evaluate_and_fire
            evaluate_and_fire(sf_conn, run_context={
                "source_table": rec.get("source_table"),
                "status": rec.get("status"),
                "failed_step": rec.get("failed_step"),
                "error_message": rec.get("error_message"),
                "duration_sec": rec.get("duration_sec"),
                "rows_extracted": rec.get("rows_extracted"),
                "rows_loaded": rec.get("rows_loaded"),
                "batch_id": rec.get("batch_id"),
                "connection_profile": rec.get("connection_profile"),
            })
        except Exception as ae:
            print(f"   (alert evaluation failed: {ae})")

        try:
            source_conn.close()
        finally:
            cur.close()
            sf_conn.close()
        _LOG_LOCAL.tag = None

    return rec["status"]


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = sys.argv[1:]
    force_full = "--full" in args
    resume_mode = "--resume" in args
    extract_only = "--extract-only" in args
    load_only = "--load-only" in args
    only_table = None
    only_tables = []
    only_profile = None
    i = 0
    while i < len(args):
        if args[i] == "--table" and i + 1 < len(args):
            only_tables.append(args[i + 1])
            i += 2
        elif args[i] == "--profile" and i + 1 < len(args):
            only_profile = args[i + 1]
            i += 2
        else:
            i += 1

    # Single table backward compat
    if len(only_tables) == 1:
        only_table = only_tables[0]
    elif len(only_tables) > 1:
        only_table = None  # handled below

    # Determine execution mode from CLI flags
    exec_mode = None
    if extract_only:
        exec_mode = "EXTRACT_ONLY"
    elif load_only:
        exec_mode = "LOAD_ONLY"

    failed = run(force_full=force_full, only_table=only_table,
                 only_tables=only_tables if len(only_tables) > 1 else None,
                 resume=resume_mode, execution_mode=exec_mode,
                 profile=only_profile)
    raise SystemExit(1 if failed else 0)
