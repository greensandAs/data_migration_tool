"""ddl_generator.py — Generate Snowflake DDL from MySQL metadata (1:1 + audit).

Reads MySQL information_schema.columns for each configured table, maps MySQL
types to Snowflake types, and creates a single target table:
    <MYSQL_SCHEMA>.RAW.<table>   business columns (1:1) + audit columns

Column order is preserved (ORDINAL_POSITION) so the TSV/CSV full-load path loads
correctly (CSV has no MATCH_BY_COLUMN_NAME).
"""
from __future__ import annotations

import mysql.connector

RAW_SCHEMA = "RAW"  # single layer; schema is literally named RAW in each DB.

# Audit columns appended to every target table (kept separate from business cols).
AUDIT_COLS = [
    ("_LOAD_TS", "TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()"),
    ("_SRC_FILE", "VARCHAR"),
    ("_BATCH_ID", "VARCHAR"),
    ("_IS_DELETED", "BOOLEAN DEFAULT FALSE"),
    ("_DELETED_AT", "TIMESTAMP_NTZ"),
]


def target_db(source_db: str) -> str:
    """Snowflake database name for a MySQL schema (1:1, uppercased)."""
    return source_db.strip().upper()


def map_mysql_type(data_type: str, col_type: str, num_prec, num_scale,
                   char_len) -> str:
    """Map a MySQL column type to a Snowflake type."""
    dt = (data_type or "").lower()
    ct = (col_type or "").lower()

    if dt in ("tinyint",) and "tinyint(1)" in ct:
        return "BOOLEAN"
    if dt in ("tinyint", "smallint", "mediumint", "int", "integer", "bigint",
              "year"):
        return "NUMBER(38,0)"
    if dt in ("decimal", "numeric"):
        p = int(num_prec) if num_prec is not None else 38
        s = int(num_scale) if num_scale is not None else 0
        # Snowflake NUMBER maxes at precision 38; MySQL DECIMAL goes up to 65.
        # Store overflow values as text (exact, collision-free). Plain VARCHAR =
        # VARCHAR(16777216) in Snowflake (max length, no storage penalty).
        if p > 38:
            return "VARCHAR"
        return f"NUMBER({p},{s})"
    if dt in ("float", "double", "real"):
        return "FLOAT"
    if dt in ("datetime", "timestamp"):
        return "TIMESTAMP_NTZ"
    if dt == "date":
        return "DATE"
    if dt == "time":
        return "TIME"
    if dt == "json":
        return "VARIANT"
    if dt in ("blob", "tinyblob", "mediumblob", "longblob", "binary",
              "varbinary"):
        return "BINARY"
    if dt in ("char", "varchar"):
        n = int(char_len) if char_len else 16777216
        return f"VARCHAR({n})"
    return "VARCHAR(16777216)"


def get_mysql_columns(mysql_conn, source_db: str, source_table: str):
    """Return ordered list of (NAME, snowflake_type) for a MySQL table.

    Names are UPPERCASED so Snowflake stores conventional uppercase identifiers.
    """
    cur = mysql_conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE,
               NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_MAXIMUM_LENGTH
        FROM information_schema.columns
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
        """,
        (source_db, source_table),
    )
    cols = []
    for r in cur.fetchall():
        sf_type = map_mysql_type(
            r["DATA_TYPE"], r["COLUMN_TYPE"], r["NUMERIC_PRECISION"],
            r["NUMERIC_SCALE"], r["CHARACTER_MAXIMUM_LENGTH"],
        )
        cols.append((r["COLUMN_NAME"].upper(), sf_type))
    cur.close()
    if not cols:
        raise ValueError(
            f"No columns found for {source_db}.{source_table} — check name/grants.")
    return cols


def build_table_ddl(db: str, target_table: str, cols) -> str:
    col_defs = [f'    "{name}" {sf_type}' for name, sf_type in cols]
    col_defs += [f'    "{name}" {sf_type}' for name, sf_type in AUDIT_COLS]
    body = ",\n".join(col_defs)
    return (
        f"CREATE TABLE IF NOT EXISTS {db}.{RAW_SCHEMA}.{target_table} (\n"
        f"{body}\n);"
    )


def generate_and_apply(sf_conn, mysql_conn, tbl: dict) -> dict:
    """Generate + execute DDL for one table. Returns the ordered column list."""
    cols = get_mysql_columns(mysql_conn, tbl["source_db"], tbl["source_table"])
    db = target_db(tbl["source_db"])

    cur = sf_conn.cursor()
    try:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {db}.{RAW_SCHEMA}")
        cur.execute(build_table_ddl(db, tbl["target_table"], cols))
        print(f"   DDL ready: {db}.{RAW_SCHEMA}.{tbl['target_table']} "
              f"({len(cols)} cols + audit)")
    finally:
        cur.close()

    return {"columns": cols}


if __name__ == "__main__":
    import json
    import os
    import snowflake.connector

    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    with open("histload_config.json") as f:
        cfg = json.load(f)

    myc = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
    )
    sfc = snowflake.connector.connect(
        account=os.getenv("SF_ACCOUNT"), user=os.getenv("SF_USER"),
        password=os.getenv("SF_PASSWORD"), role=os.getenv("SF_ROLE"),
        warehouse=os.getenv("SF_WAREHOUSE"),
        database=os.getenv("SF_DATABASE", "HISTLOAD_DB"),
        schema=os.getenv("SF_SCHEMA", "META"),
    )
    try:
        for t in cfg["tables"]:
            if t.get("active", True):
                generate_and_apply(sfc, myc, t)
    finally:
        myc.close()
        sfc.close()
