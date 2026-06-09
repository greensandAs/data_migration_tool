"""orchestrator.py — MySQL -> Snowflake historical-load driver (1:1, single layer).

Per active table:
  1. Generate/ensure <DB>.RAW.<table> DDL (1:1 + audit) from MySQL info_schema.
  2. Decide engine: FULL (first run or load_type=full) -> mysqlsh,
     else INCREMENTAL -> connectorx.
  3. Extract -> PUT to stage -> load into RAW (TRUNCATE+COPY | COPY+MERGE).
  4. Read MAX(watermark) from RAW (source of truth) -> cache to config.
  5. Write an audit row to HISTLOAD_DB.META.RUN_LOG.

Usage:
  python orchestrator.py [config.json]            # full/incremental per table
  python orchestrator.py --full [--table T]       # force full reload
  python orchestrator.py --reconcile [--table T]  # soft-delete missing keys
  python orchestrator.py --validate [--deep] [--table T]
"""
from __future__ import annotations

import json
import os
import sys
import time
import uuid
from datetime import datetime

import mysql.connector

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import ddl_generator
import extractor_full
import extractor_incremental
import loader
import reconciler
import run_log
import schema_drift
import validator
import watermark

CONFIG_DEFAULT = "histload_config.json"


def _now_local():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _build_src_cfg(src: dict) -> dict:
    """Merge env vars over JSON source config (env wins)."""
    out = dict(src)
    env_map = {"host": "MYSQL_HOST", "port": "MYSQL_PORT",
               "user": "MYSQL_USER", "password": "MYSQL_PASSWORD"}
    for key, env in env_map.items():
        val = os.getenv(env)
        if val is not None:
            out[key] = int(val) if key == "port" else val
    return out


def _build_sf_cfg(sf: dict) -> dict:
    out = dict(sf)
    env_map = {"account": "SF_ACCOUNT", "user": "SF_USER", "password": "SF_PASSWORD",
               "role": "SF_ROLE", "warehouse": "SF_WAREHOUSE",
               "database": "SF_DATABASE", "schema": "SF_SCHEMA"}
    for key, env in env_map.items():
        val = os.getenv(env)
        if val is not None:
            out[key] = val
    out.setdefault("database", "HISTLOAD_DB")
    out.setdefault("schema", "META")
    return out


def _connect(cfg):
    src_cfg = _build_src_cfg(cfg.get("source", {}))
    sf_cfg = _build_sf_cfg(cfg.get("snowflake", {}))
    mysql_conn = mysql.connector.connect(
        host=src_cfg["host"], port=int(src_cfg["port"]),
        user=src_cfg["user"], password=src_cfg["password"],
    )
    sf_conn = loader.get_sf_conn(sf_cfg)
    return src_cfg, sf_conn, mysql_conn


def run(config_path: str = CONFIG_DEFAULT, force_full: bool = False,
        only_table: str | None = None):
    with open(config_path) as f:
        cfg = json.load(f)
    export_dir = cfg.get("export_dir", "./export")
    batch_id = uuid.uuid4().hex[:12]

    print("=" * 64)
    print(f" MySQL -> Snowflake historical load | batch {batch_id} | {_now_local()} local")
    print("=" * 64)

    src_cfg, sf_conn, mysql_conn = _connect(cfg)
    failed = 0
    try:
        for tbl in cfg["tables"]:
            if not tbl.get("active", True):
                continue
            if only_table and tbl["source_table"] != only_table:
                continue
            status = _process_table(tbl, src_cfg, sf_conn, mysql_conn,
                                    export_dir, batch_id, config_path,
                                    force_full=force_full)
            if status == "failed":
                failed += 1
    finally:
        mysql_conn.close()
        sf_conn.close()

    print(f"\nRun complete: {_now_local()} local (batch {batch_id}) | "
          f"failed tables: {failed}")
    return failed


def _process_table(tbl, src_cfg, sf_conn, mysql_conn, export_dir, batch_id,
                   config_path, force_full: bool = False):
    first_run = tbl.get("last_loaded_at") is None
    is_full = force_full or first_run or tbl.get("load_type") == "full"
    engine = "mysqlsh" if is_full else "connectorx"
    label = "FULL" if is_full else "INCREMENTAL"
    print(f"\n[{label}/{engine}] {tbl['source_db']}.{tbl['source_table']} "
          f"-> {loader.target_db(tbl['source_db'])}.RAW.{tbl['target_table']}")

    # Watermark mode + the cursor field it reads/writes (id -> last_loaded_key,
    # time -> last_loaded_at). Used for RUN_LOG WATERMARK_FROM and persistence.
    wm_type = (extractor_incremental._watermark_type(tbl, src_cfg)
               if tbl.get("watermark_col") else "time")
    wm_from_cursor = (tbl.get("last_loaded_key") if wm_type == "id"
                      else tbl.get("last_loaded_at"))

    rec = {
        "batch_id": batch_id, "source_db": tbl["source_db"],
        "source_table": tbl["source_table"],
        "target_db": loader.target_db(tbl["source_db"]),
        "target_table": tbl["target_table"],
        "load_type": label.lower(), "engine": engine,
        "rows_extracted": 0, "rows_loaded": 0,
        "watermark_from": wm_from_cursor, "watermark_to": None,
        "watermark_type": (wm_type if tbl.get("watermark_col") else None),
        "status": "failed", "error_message": None, "failed_step": None,
        "duration_sec": None, "run_start": _now_local(), "run_end": None,
    }
    cur = sf_conn.cursor()
    t0 = time.monotonic()
    step = "start"
    try:
        step = "ddl"
        meta = ddl_generator.generate_and_apply(sf_conn, mysql_conn, tbl)
        columns = meta["columns"]

        step = "schema_drift"
        schema_drift.detect_and_apply(cur, mysql_conn, tbl)

        if is_full:
            step = "extract_full"
            files, _ = extractor_full.extract_full_mysqlsh(tbl, src_cfg, export_dir)
            step = "put"
            for fp in files:
                loader.put_file(cur, fp, tbl, "full")
            step = "copy_full"
            rec["rows_loaded"] = loader.copy_into_full(cur, tbl, columns, batch_id)
            rec["rows_extracted"] = rec["rows_loaded"]
            # Snapshot marker; overridden below by MAX(watermark_col) if present.
            rec["watermark_to"] = _now_local()
        else:
            step = "extract_incremental"
            files, rows, wm_to = extractor_incremental.extract_incremental_connectorx(
                tbl, src_cfg, export_dir)
            rec["rows_extracted"] = rows
            rec["watermark_to"] = wm_to
            if files and rows > 0:
                step = "put"
                loader.clear_stage_safe(cur, tbl, "incremental")
                for fp in files:
                    loader.put_file(cur, fp, tbl, "incremental")
                step = "copy_merge"
                rec["rows_loaded"] = loader.copy_into_merge(cur, tbl, batch_id)
            else:
                rec["status"] = "skipped"

        if rec["status"] != "skipped":
            step = "watermark"
            wm = loader.current_max_watermark(cur, tbl)
            if wm:
                rec["watermark_to"] = wm
            # Persist the cursor in the right field: id mode -> last_loaded_key
            # (numeric PK), with last_loaded_at as a tracking timestamp; time mode
            # -> last_loaded_at (the timestamp cursor).
            if wm_type == "id":
                watermark.update_table_state(
                    config_path, tbl["source_db"], tbl["source_table"],
                    status="success", last_loaded_key=wm, last_loaded_at=_now_local())
            else:
                watermark.update_table_state(
                    config_path, tbl["source_db"], tbl["source_table"],
                    status="success", last_loaded_at=wm)
            rec["status"] = "success"
        else:
            print("   skipped — no new rows")
            watermark.update_table_state(
                config_path, tbl["source_db"], tbl["source_table"], status="skipped")

        sf_conn.commit()
        print(f"   done ({rec['status']})")

    except Exception as e:  # noqa: BLE001 — log + continue to next table
        sf_conn.rollback()
        rec["status"] = "failed"
        rec["failed_step"] = step
        rec["error_message"] = str(e)[:4000]
        watermark.update_table_state(
            config_path, tbl["source_db"], tbl["source_table"], status="failed")
        print(f"   FAILED at step '{step}': {e}")
    finally:
        rec["run_end"] = _now_local()
        rec["duration_sec"] = round(time.monotonic() - t0, 2)
        try:
            run_log.write_run_log(cur, rec)
            sf_conn.commit()
        except Exception as le:  # noqa: BLE001
            print(f"   (run_log write failed: {le})")
        cur.close()

    return rec["status"]


def run_reconcile(config_path: str = CONFIG_DEFAULT, only_table: str | None = None):
    with open(config_path) as f:
        cfg = json.load(f)
    batch_id = uuid.uuid4().hex[:12]

    print("=" * 64)
    print(f" Delete reconciliation | batch {batch_id} | {_now_local()} local")
    print("=" * 64)

    _, sf_conn, mysql_conn = _connect(cfg)
    failed = 0
    try:
        for tbl in cfg["tables"]:
            if not tbl.get("active", True) or not tbl.get("reconcile", False):
                continue
            if only_table and tbl["source_table"] != only_table:
                continue
            cur = sf_conn.cursor()
            t0 = time.monotonic()
            rec = {
                "batch_id": batch_id, "source_db": tbl["source_db"],
                "source_table": tbl["source_table"],
                "target_db": loader.target_db(tbl["source_db"]),
                "target_table": tbl["target_table"],
                "load_type": "reconcile", "engine": "reconciler",
                "rows_extracted": None, "rows_loaded": 0,
                "watermark_from": None, "watermark_to": None,
                "status": "failed", "error_message": None, "failed_step": None,
                "duration_sec": None, "run_start": _now_local(), "run_end": None,
            }
            print(f"\n[RECONCILE] {tbl['source_db']}.{tbl['source_table']}")
            try:
                result = reconciler.reconcile_table(cur, mysql_conn, tbl)
                if result["skipped"]:
                    print(f"   skipped — {result['skipped']}")
                    rec["status"] = "skipped"
                else:
                    rec["rows_loaded"] = result["deleted"]
                    print(f"   soft-deleted {result['deleted']} row(s)")
                    rec["status"] = "success"
                sf_conn.commit()
            except Exception as e:  # noqa: BLE001
                sf_conn.rollback()
                failed += 1
                rec["status"] = "failed"
                rec["failed_step"] = "reconcile"
                rec["error_message"] = str(e)[:4000]
                print(f"   FAILED: {e}")
            finally:
                rec["run_end"] = _now_local()
                rec["duration_sec"] = round(time.monotonic() - t0, 2)
                try:
                    run_log.write_run_log(cur, rec)
                    sf_conn.commit()
                except Exception as le:  # noqa: BLE001
                    print(f"   (run_log write failed: {le})")
                cur.close()
    finally:
        mysql_conn.close()
        sf_conn.close()

    print(f"\nReconcile complete: {_now_local()} local (batch {batch_id}) | "
          f"failed tables: {failed}")
    return failed


def run_validate(config_path: str = CONFIG_DEFAULT, only_table: str | None = None,
                 deep: bool = False):
    with open(config_path) as f:
        cfg = json.load(f)
    batch_id = uuid.uuid4().hex[:12]

    print("=" * 64)
    print(f" Source↔RAW validation{' [DEEP]' if deep else ''} | "
          f"batch {batch_id} | {_now_local()} local")
    print("=" * 64)

    _, sf_conn, mysql_conn = _connect(cfg)
    mismatches = 0
    try:
        for tbl in cfg["tables"]:
            if not tbl.get("active", True):
                continue
            if only_table and tbl["source_table"] != only_table:
                continue
            cur = sf_conn.cursor()
            start = _now_local()
            t0 = time.monotonic()
            try:
                r = validator.validate_table(cur, mysql_conn, tbl, deep=deep)
                ok = r["ok"]
                checks = []
                if not r["count_ok"]:
                    checks.append(f"count delta {r['delta']:+d}")
                if r["has_wm"] and not r["wm_ok"]:
                    checks.append(f"watermark src={r['source_wm']} raw={r['raw_wm']}")
                if r["deep"] and not r["hash_ok"]:
                    checks.append("row-hash differs")
                flag = "OK" if ok else "MISMATCH (" + "; ".join(checks) + ")"
                wm_txt = f" wm={r['raw_wm']}" if r["has_wm"] else ""
                hash_txt = (" hash=OK" if (r["deep"] and r["hash_ok"]) else
                            " hash=DIFF" if r["deep"] else "")
                print(f"  {tbl['source_db']}.{tbl['source_table']}: "
                      f"source={r['source']} raw={r['raw_live']}{wm_txt}{hash_txt} -> {flag}")
                if not ok:
                    mismatches += 1
                run_log.write_run_log(cur, {
                    "batch_id": batch_id, "source_db": tbl["source_db"],
                    "source_table": tbl["source_table"],
                    "target_db": loader.target_db(tbl["source_db"]),
                    "target_table": tbl["target_table"],
                    "load_type": "validate", "engine": "validator",
                    "rows_extracted": r["source"], "rows_loaded": r["raw_live"],
                    "watermark_from": r["source_wm"], "watermark_to": r["raw_wm"],
                    "watermark_type": (extractor_incremental._watermark_type(tbl, src_cfg)
                                       if tbl.get("watermark_col") else None),
                    "status": "success" if ok else "mismatch",
                    "error_message": None if ok else "; ".join(checks),
                    "failed_step": None if ok else "parity",
                    "duration_sec": round(time.monotonic() - t0, 2),
                    "run_start": start, "run_end": _now_local(),
                })
                sf_conn.commit()
            except Exception as e:  # noqa: BLE001
                mismatches += 1
                print(f"  {tbl['source_table']}: VALIDATION ERROR: {e}")
            finally:
                cur.close()
    finally:
        mysql_conn.close()
        sf_conn.close()

    print(f"\nValidation complete: {_now_local()} local (batch {batch_id}) | "
          f"mismatches: {mismatches}")
    return mismatches


if __name__ == "__main__":
    args = sys.argv[1:]
    reconcile_mode = "--reconcile" in args
    validate_mode = "--validate" in args
    force_full = "--full" in args
    deep = "--deep" in args
    only_table = None
    positional = []
    i = 0
    while i < len(args):
        a = args[i]
        if a == "--table":
            i += 1
            only_table = args[i] if i < len(args) else None
        elif not a.startswith("--"):
            positional.append(a)
        i += 1
    path = positional[0] if positional else CONFIG_DEFAULT

    if validate_mode:
        failed = run_validate(path, only_table=only_table, deep=deep)
    elif reconcile_mode:
        failed = run_reconcile(path, only_table=only_table)
    else:
        failed = run(path, force_full=force_full, only_table=only_table)
    raise SystemExit(1 if failed else 0)
