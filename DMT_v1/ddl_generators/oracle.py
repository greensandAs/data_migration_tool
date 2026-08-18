# Oracle-to-Snowflake type mapping and DDL generation.
# Co-authored with CoCo
"""ddl_generators.oracle — Generate Snowflake DDL from Oracle metadata.

Reads ALL_TAB_COLUMNS for column metadata, maps Oracle types to Snowflake
equivalents, and creates target tables: <TARGET_DB>.RAW.<table> with
business + audit columns.

Handles ~30 Oracle data types including:
  - NUMBER (with/without precision), FLOAT, BINARY_FLOAT, BINARY_DOUBLE
  - VARCHAR2, NVARCHAR2, CHAR, NCHAR, CLOB, NCLOB, LONG
  - DATE, TIMESTAMP (with/without timezone), INTERVAL
  - RAW, BLOB, LONG RAW, BFILE
  - XMLTYPE, SDO_GEOMETRY, ROWID, UROWID
"""
from __future__ import annotations

from ddl_generators import RAW_SCHEMA, AUDIT_COLS, SCD2_COLS, target_db


# ── Oracle → Snowflake Type Mapping ──────────────────────────────────────────

def map_oracle_type(data_type: str, data_length: int | None,
                    data_precision: int | None, data_scale: int | None) -> str:
    """Map a single Oracle column type to a Snowflake data type.

    Oracle's NUMBER type is complex:
      - NUMBER (no prec/scale) → can hold any numeric value → FLOAT
      - NUMBER(p) → integer → NUMBER(p, 0)
      - NUMBER(p, s) → fixed decimal → NUMBER(p, s)
      - NUMBER(*, 0) → integer → NUMBER(38, 0)
    """
    dt = (data_type or "").upper().strip()

    # ── Numeric types ─────────────────────────────────────────────────────────
    if dt == "NUMBER":
        if data_precision is None and data_scale is None:
            # NUMBER without precision: can hold anything from int to float
            return "FLOAT"
        p = int(data_precision) if data_precision is not None else 38
        s = int(data_scale) if data_scale is not None else 0
        if p > 38:
            return "VARCHAR(16777216)"
        if s <= 0:
            # Integer — absorb negative scale into precision
            effective_p = min(p + abs(s), 38) if s < 0 else p
            return f"NUMBER({effective_p},0)"
        return f"NUMBER({p},{s})"

    if dt == "FLOAT":
        return "FLOAT"

    if dt in ("BINARY_FLOAT",):
        return "FLOAT"

    if dt in ("BINARY_DOUBLE",):
        return "FLOAT"

    # ── Character types ───────────────────────────────────────────────────────
    if dt in ("VARCHAR2", "NVARCHAR2"):
        n = int(data_length) if data_length and int(data_length) > 0 else 16777216
        return f"VARCHAR({min(n * 4, 16777216)})"  # *4 for byte vs char semantics

    if dt in ("CHAR", "NCHAR"):
        n = int(data_length) if data_length and int(data_length) > 0 else 1
        return f"VARCHAR({min(n * 4, 16777216)})"

    if dt in ("CLOB", "NCLOB", "LONG"):
        return "VARCHAR(16777216)"

    # ── Date/Time types ───────────────────────────────────────────────────────
    if dt == "DATE":
        # Oracle DATE includes time component (YYYY-MM-DD HH:MI:SS)
        return "TIMESTAMP_NTZ"

    if "TIMESTAMP" in dt:
        if "TIME ZONE" in dt or "TZ" in dt:
            return "TIMESTAMP_TZ"
        if "LOCAL" in dt:
            return "TIMESTAMP_LTZ"
        return "TIMESTAMP_NTZ"

    if "INTERVAL" in dt:
        # INTERVAL YEAR TO MONTH / INTERVAL DAY TO SECOND
        return "VARCHAR(100)"

    # ── Binary/LOB types ──────────────────────────────────────────────────────
    if dt in ("RAW",):
        n = int(data_length) if data_length else 2000
        return f"BINARY({min(n, 8388608)})"

    if dt in ("BLOB", "LONG RAW", "BFILE"):
        return "BINARY"

    # ── Special types ─────────────────────────────────────────────────────────
    if dt == "XMLTYPE":
        return "VARIANT"

    if dt in ("ROWID", "UROWID"):
        return "VARCHAR(128)"

    if dt == "SDO_GEOMETRY":
        return "VARIANT"

    if dt == "BOOLEAN":
        return "BOOLEAN"

    # ── Fallback ──────────────────────────────────────────────────────────────
    return "VARCHAR(16777216)"


def get_oracle_columns(oracle_conn, schema: str,
                       table: str) -> list[tuple[str, str]]:
    """Return ordered list of (NAME, snowflake_type) for an Oracle table.

    Uses ALL_TAB_COLUMNS for metadata. Column names are uppercased to match
    Snowflake conventions.
    """
    cur = oracle_conn.cursor()
    cur.execute(
        """
        SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH,
               DATA_PRECISION, DATA_SCALE
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = :1 AND TABLE_NAME = :2
        ORDER BY COLUMN_ID
        """,
        (schema.upper(), table.upper()),
    )
    cols = []
    for row in cur.fetchall():
        col_name, data_type, data_length, data_precision, data_scale = row
        sf_type = map_oracle_type(data_type, data_length, data_precision, data_scale)
        cols.append((col_name.upper(), sf_type))
    cur.close()

    if not cols:
        raise ValueError(
            f"No columns found for {schema}.{table} — "
            "check the schema/table name and grants on ALL_TAB_COLUMNS.")
    return cols


def build_table_ddl(db: str, target_table: str, cols: list[tuple],
                    scd_type: int = 1) -> str:
    """Generate CREATE TABLE IF NOT EXISTS DDL for Snowflake."""
    col_defs = [f'    "{name}" {sf_type}' for name, sf_type in cols]
    col_defs += [f'    "{name}" {sf_type}' for name, sf_type in AUDIT_COLS]
    if scd_type == 2:
        col_defs += [f'    "{name}" {sf_type}' for name, sf_type in SCD2_COLS]
    body = ",\n".join(col_defs)
    return (
        f"CREATE TABLE IF NOT EXISTS {db}.{RAW_SCHEMA}.{target_table} (\n"
        f"{body}\n);"
    )


def generate_and_apply(sf_conn, oracle_conn, config: dict) -> dict:
    """Generate + execute DDL for one Oracle table. Returns column metadata."""
    source_schema = config["SOURCE_DB"]  # In Oracle, SOURCE_DB = schema/owner
    source_table = config["SOURCE_TABLE"]
    tgt_db = config.get("TARGET_DB") or target_db(source_schema)
    tgt_table = config.get("TARGET_TABLE") or source_table.upper()
    scd_type = int(config.get("SCD_TYPE") or 1)

    cols = get_oracle_columns(oracle_conn, source_schema, source_table)

    cur = sf_conn.cursor()
    try:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {tgt_db}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {tgt_db}.{RAW_SCHEMA}")
        cur.execute(build_table_ddl(tgt_db, tgt_table, cols, scd_type=scd_type))
        # For SCD2: add columns if migrating existing table
        if scd_type == 2:
            for col_name, col_def in SCD2_COLS:
                try:
                    cur.execute(
                        f'ALTER TABLE {tgt_db}.{RAW_SCHEMA}.{tgt_table} '
                        f'ADD COLUMN IF NOT EXISTS "{col_name}" {col_def}')
                except Exception:
                    pass
        print(f"   DDL ready: {tgt_db}.{RAW_SCHEMA}.{tgt_table} "
              f"({len(cols)} cols + audit"
              f"{' + SCD2' if scd_type == 2 else ''})")
    finally:
        cur.close()

    return {"columns": cols, "target_db": tgt_db, "target_table": tgt_table}
