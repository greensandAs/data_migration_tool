"""config.py — Configuration management (local JSON + Snowflake sync)."""
from __future__ import annotations

import json
import os
from pathlib import Path

from core.connections import sf_execute, sf_query

CONFIG_FILE = Path(__file__).parent.parent / "migration_config.json"


def load_config() -> dict:
    """Load config from local JSON file."""
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE) as f:
            return json.load(f)
    return {"export_dir": "./export", "tables": [], "defaults": {}}


def save_config(cfg: dict):
    """Save config to local JSON AND auto-sync to Snowflake CONFIG_TABLE."""
    with open(CONFIG_FILE, "w") as f:
        json.dump(cfg, f, indent=2)
    _sync_to_snowflake(cfg)


def _sync_to_snowflake(cfg: dict):
    """Push all table configs to Snowflake CONFIG_TABLE (DELETE + INSERT)."""
    tables = cfg.get("tables", [])
    try:
        sf_execute("DELETE FROM DATA_MIGRATION.CONTROL.CONFIG_TABLE")
        for t in tables:
            sf_execute(
                "INSERT INTO DATA_MIGRATION.CONTROL.CONFIG_TABLE "
                "(MSSQL_DATABASE_NAME, MSSQL_SCHEMA_NAME, MSSQL_TABLE_NAME, "
                "SF_DATABASE_NAME, SF_SCHEMA_NAME, SF_TABLE_NAME, "
                "WAREHOUSE_NAME, SCD_TYPE, LOAD_TYPE, CDC_COLUMNS, PRIMARY_KEY, "
                "DELIMITER, FILTER_CONDITION, TRIM, ENCRYPTION_COLUMNS, "
                "S3_PATH, CUSTOM_SQL, EXECUTION_MODE, CDC_TYPE, ENABLED) "
                "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    t.get("source_db"), t.get("source_schema", "dbo"), t.get("source_table"),
                    t.get("target_db", "ANALYTICS"), t.get("target_schema", "PUBLIC"), t.get("target_table"),
                    t.get("warehouse_name", "COMPUTE_WH"), t.get("scd_type", 0),
                    (t.get("load_type") or "full").upper(), t.get("cdc_columns"),
                    t.get("primary_key"), t.get("delimiter", "|"),
                    t.get("filter_condition"), t.get("trim", "N"),
                    t.get("encryption_columns"), t.get("cloud_path"),
                    t.get("custom_sql"), (t.get("execution_mode") or "FULL").upper(),
                    (t.get("cdc_type") or "TIMESTAMP").upper(),
                    "Y" if t.get("active") else "N",
                ),
            )
    except Exception:
        pass


def pull_from_snowflake() -> dict:
    """Pull config from Snowflake CONFIG_TABLE → returns config dict."""
    df = sf_query("SELECT * FROM DATA_MIGRATION.CONTROL.CONFIG_TABLE ORDER BY JOB_ID")
    if df.empty:
        return {"export_dir": "./export", "tables": [], "defaults": {}}

    tables = [{
        "source_db": r["MSSQL_DATABASE_NAME"],
        "source_schema": r["MSSQL_SCHEMA_NAME"],
        "source_table": r["MSSQL_TABLE_NAME"],
        "target_db": r["SF_DATABASE_NAME"],
        "target_schema": r["SF_SCHEMA_NAME"],
        "target_table": r["SF_TABLE_NAME"],
        "warehouse_name": r.get("WAREHOUSE_NAME", "COMPUTE_WH"),
        "scd_type": int(r.get("SCD_TYPE", 0) or 0),
        "load_type": (r.get("LOAD_TYPE") or "full").lower(),
        "cdc_columns": r.get("CDC_COLUMNS"),
        "primary_key": r.get("PRIMARY_KEY"),
        "delimiter": r.get("DELIMITER", "|"),
        "filter_condition": r.get("FILTER_CONDITION"),
        "trim": r.get("TRIM", "N"),
        "encryption_columns": r.get("ENCRYPTION_COLUMNS"),
        "cloud_path": r.get("S3_PATH") or os.getenv("CLOUD_PATH", ""),
        "custom_sql": r.get("CUSTOM_SQL"),
        "execution_mode": (r.get("EXECUTION_MODE") or "FULL").upper(),
        "cdc_type": (r.get("CDC_TYPE") or "TIMESTAMP").upper(),
        "active": r.get("ENABLED") == "Y",
        "last_run_status": None,
    } for _, r in df.iterrows()]

    return {"export_dir": "./export", "tables": tables, "defaults": {}}
