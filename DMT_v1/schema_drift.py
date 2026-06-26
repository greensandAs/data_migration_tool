# Additive schema-drift detection and resolution for Snowflake RAW tables.
# Co-authored with CoCo
"""schema_drift.py — Additive schema-drift handling (RAW layer only).

Before each load, compares MySQL columns against the existing Snowflake table:
  - New columns in MySQL -> ALTER TABLE ADD COLUMN
  - Dropped columns in MySQL -> warn only (preserve Snowflake data)
"""
from __future__ import annotations

from ddl_generators import RAW_SCHEMA, get_mysql_columns, map_mysql_type, target_db


def _snowflake_business_cols(cur, db: str, table: str) -> set:
    """Get non-audit column names from the Snowflake table."""
    cur.execute(
        f"SELECT COLUMN_NAME FROM {db}.INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION",
        (RAW_SCHEMA, table),
    )
    return {r[0].upper() for r in cur.fetchall() if not r[0].startswith("_")}


def detect_and_apply(cur, source_conn, config: dict, source_type: str = "mysql") -> dict:
    """Reconcile additive schema drift for one table. Returns {added, dropped}."""
    db = config.get("TARGET_DB") or target_db(config["SOURCE_DB"])
    tgt_table = config.get("TARGET_TABLE") or config["SOURCE_TABLE"].upper()

    sf_cols = _snowflake_business_cols(cur, db, tgt_table)
    if not sf_cols:
        return {"added": [], "dropped": []}

    # Get source columns based on source type
    if source_type == "teradata":
        from ddl_generators.teradata import get_teradata_columns
        source_cols = get_teradata_columns(source_conn, config["SOURCE_DB"], config["SOURCE_TABLE"])
    else:
        source_cols = get_mysql_columns(source_conn, config["SOURCE_DB"], config["SOURCE_TABLE"])

    source_by_upper = {name.upper(): (name, sf_type) for name, sf_type in source_cols}

    new_cols = [source_by_upper[u] for u in source_by_upper if u not in sf_cols]
    dropped = [c for c in sf_cols if c not in source_by_upper]

    added = []
    for name, sf_type in new_cols:
        cur.execute(
            f'ALTER TABLE {db}.{RAW_SCHEMA}.{tgt_table} '
            f'ADD COLUMN IF NOT EXISTS "{name}" {sf_type}'
        )
        added.append(name)
        print(f'   schema drift: added "{name}" {sf_type}')

    if dropped:
        print(f"   schema drift WARNING: columns removed in source but kept in "
              f"Snowflake: {sorted(dropped)}")

    return {"added": added, "dropped": sorted(dropped)}
