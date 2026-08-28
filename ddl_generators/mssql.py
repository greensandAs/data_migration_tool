# MSSQL-to-Snowflake type mapping and DDL generation.
"""ddl_generators.mssql — Generate Snowflake DDL from MSSQL metadata.

Reads INFORMATION_SCHEMA.COLUMNS via pyodbc, maps MSSQL types to Snowflake
equivalents, and creates target tables: <TARGET_DB>.RAW.<table> with
business + audit columns.
"""
from __future__ import annotations

import re

from ddl_generators import RAW_SCHEMA, AUDIT_COLS, SCD2_COLS, target_db


# ── MSSQL → Snowflake Type Mapping ──────────────────────────────────────────

MSSQL_TO_SF_DIRECT = {
    "int": "NUMBER(10,0)",
    "bigint": "NUMBER(19,0)",
    "smallint": "NUMBER(5,0)",
    "tinyint": "NUMBER(3,0)",
    "bit": "BOOLEAN",
    "float": "FLOAT",
    "real": "FLOAT",
    "money": "NUMBER(19,4)",
    "smallmoney": "NUMBER(10,4)",
    "date": "DATE",
    "time": "TIME",
    "datetime": "TIMESTAMP_NTZ",
    "datetime2": "TIMESTAMP_NTZ",
    "smalldatetime": "TIMESTAMP_NTZ",
    "datetimeoffset": "TIMESTAMP_TZ",
    "uniqueidentifier": "VARCHAR(36)",
    "text": "VARCHAR(16777216)",
    "ntext": "VARCHAR(16777216)",
    "image": "BINARY",
    "xml": "VARIANT",
    "sql_variant": "VARCHAR(16777216)",
    "hierarchyid": "VARCHAR(4000)",
    "geography": "VARCHAR(16777216)",
    "geometry": "VARCHAR(16777216)",
    "timestamp": "BINARY(8)",
    "rowversion": "BINARY(8)",
}


def map_mssql_type(data_type: str, max_length, precision, scale) -> str:
    """Map a single MSSQL column type to a Snowflake data type."""
    dt = (data_type or "").lower().strip()

    if dt in MSSQL_TO_SF_DIRECT:
        return MSSQL_TO_SF_DIRECT[dt]

    # varchar / nvarchar / char / nchar with length
    if dt in ("varchar", "nvarchar", "char", "nchar"):
        if max_length and int(max_length) > 0:
            # nvarchar max_length is in bytes (2x chars) — cap at Snowflake max
            n = min(int(max_length), 16777216)
            return f"VARCHAR({n})"
        return "VARCHAR(16777216)"

    # varbinary / binary
    if dt in ("varbinary", "binary"):
        if max_length and int(max_length) > 0:
            return f"BINARY({int(max_length)})"
        return "BINARY"

    # decimal / numeric with precision and scale
    if dt in ("decimal", "numeric"):
        p = int(precision) if precision else 38
        s = int(scale) if scale else 0
        if p > 38:
            return "VARCHAR(16777216)"
        return f"NUMBER({p},{s})"

    return "VARCHAR(16777216)"


def get_mssql_columns(mssql_conn, source_db: str, source_schema: str,
                      source_table: str) -> list[tuple]:
    """Return ordered list of (NAME, snowflake_type) for an MSSQL table.

    Uses INFORMATION_SCHEMA.COLUMNS via pyodbc.
    """
    cur = mssql_conn.cursor()
    cur.execute(
        "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, "
        "NUMERIC_PRECISION, NUMERIC_SCALE "
        "FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
        "ORDER BY ORDINAL_POSITION",
        (source_schema, source_table),
    )
    cols = []
    for row in cur.fetchall():
        col_name, data_type, max_length, precision, scale = row
        sf_type = map_mssql_type(data_type, max_length, precision, scale)
        cols.append((col_name.upper(), sf_type))
    cur.close()

    if not cols:
        raise ValueError(
            f"No columns found for [{source_schema}].[{source_table}] "
            f"in database {source_db} — check name/grants.")
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


def generate_and_apply(sf_conn, mssql_conn, config: dict) -> dict:
    """Generate + execute DDL for one MSSQL table. Returns column metadata."""
    source_db = config["SOURCE_DB"]
    source_schema = config.get("SOURCE_SCHEMA") or "dbo"
    source_table = config["SOURCE_TABLE"]
    tgt_db = config.get("TARGET_DB") or target_db(source_db)
    tgt_table = config.get("TARGET_TABLE") or source_table.upper()
    scd_type = int(config.get("SCD_TYPE") or 1)

    cols = get_mssql_columns(mssql_conn, source_db, source_schema, source_table)

    cur = sf_conn.cursor()
    try:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {tgt_db}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {tgt_db}.{RAW_SCHEMA}")
        cur.execute(build_table_ddl(tgt_db, tgt_table, cols, scd_type=scd_type))
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
