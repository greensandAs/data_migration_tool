"""schema_drift.py — additive schema-drift handling (RAW only).

Before each load, compares MySQL columns against the existing <DB>.RAW.<table>:
  * New MySQL columns  -> ALTER TABLE ADD COLUMN (typed via the MySQL->Snowflake
                          map), so the load no longer fails.
  * Dropped MySQL cols -> warn only (data in Snowflake is preserved).
"""
from __future__ import annotations

import ddl_generator
import loader


def _snowflake_business_cols(cur, db: str, table: str) -> set:
    """Uppercased non-audit (non '_'-prefixed) columns of a Snowflake table."""
    cur.execute(
        f"SELECT COLUMN_NAME FROM {db}.INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION",
        (ddl_generator.RAW_SCHEMA, table),
    )
    return {r[0].upper() for r in cur.fetchall() if not r[0].startswith("_")}


def detect_and_apply(cur, mysql_conn, tbl: dict) -> dict:
    """Reconcile additive schema drift for one table. Returns {added, dropped}."""
    db = loader.target_db(tbl["source_db"])
    target = tbl["target_table"]

    sf_cols = _snowflake_business_cols(cur, db, target)
    if not sf_cols:
        # Table not created yet (first run) — ddl_generator will build it.
        return {"added": [], "dropped": []}

    mysql_cols = ddl_generator.get_mysql_columns(
        mysql_conn, tbl["source_db"], tbl["source_table"])
    mysql_by_upper = {name.upper(): (name, sf_type) for name, sf_type in mysql_cols}

    new_cols = [mysql_by_upper[u] for u in mysql_by_upper if u not in sf_cols]
    dropped = [c for c in sf_cols if c not in mysql_by_upper]

    added = []
    for name, sf_type in new_cols:
        cur.execute(
            f'ALTER TABLE {db}.{ddl_generator.RAW_SCHEMA}.{target} '
            f'ADD COLUMN IF NOT EXISTS "{name}" {sf_type}'
        )
        added.append(name)
        print(f'   schema drift: added "{name}" {sf_type} to '
              f"{db}.{ddl_generator.RAW_SCHEMA}.{target}")

    if dropped:
        print(f"   schema drift WARNING: columns dropped in MySQL but kept in "
              f"Snowflake: {sorted(dropped)}")

    return {"added": added, "dropped": sorted(dropped)}
