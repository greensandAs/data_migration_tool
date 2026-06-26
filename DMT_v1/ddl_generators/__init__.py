# MySQL-to-Snowflake type mapping and DDL generation.
"""ddl_generators.mysql — Generate Snowflake DDL from MySQL metadata.

Reads MySQL information_schema.columns, maps types to Snowflake equivalents,
and creates target tables: <TARGET_DB>.RAW.<table> with business + audit columns.
"""
from __future__ import annotations

RAW_SCHEMA = "RAW"

# Audit columns appended to every target table.
AUDIT_COLS = [
    ("_LOAD_TS", "TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()"),
    ("_SRC_FILE", "VARCHAR"),
    ("_BATCH_ID", "VARCHAR"),
    ("_IS_DELETED", "BOOLEAN DEFAULT FALSE"),
    ("_DELETED_AT", "TIMESTAMP_NTZ"),
]

# Additional audit columns for SCD Type 2 tables.
SCD2_COLS = [
    ("_VALID_FROM", "TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()"),
    ("_VALID_TO", "TIMESTAMP_NTZ"),
    ("_IS_CURRENT", "BOOLEAN DEFAULT TRUE"),
]


def target_db(source_db: str) -> str:
    """Snowflake database name for a MySQL schema (1:1, uppercased)."""
    return source_db.strip().upper()


def map_mysql_type(data_type: str, col_type: str, num_prec, num_scale,
                   char_len, blob_mode: str = "binary") -> str:
    """Map a MySQL column type to a Snowflake type.

    blob_mode controls how BLOB columns are handled:
      - "binary": map to BINARY (default, for true binary data)
      - "text":   map to VARCHAR (for BLOBs that actually store text/JSON)
      - "skip":   map to VARCHAR with a placeholder comment (exclude from extraction)
    """
    dt = (data_type or "").lower()
    ct = (col_type or "").lower()

    # Boolean: tinyint(1) is MySQL's boolean convention
    if dt in ("tinyint",) and "tinyint(1)" in ct:
        return "BOOLEAN"
    # Integers: all stored as NUMBER(38,0) in Snowflake
    if dt in ("tinyint", "smallint", "mediumint", "int", "integer", "bigint", "year"):
        return "NUMBER(38,0)"
    # Fixed-point decimals
    if dt in ("decimal", "numeric"):
        p = int(num_prec) if num_prec is not None else 38
        s = int(num_scale) if num_scale is not None else 0
        if p > 38:
            return "VARCHAR"
        return f"NUMBER({p},{s})"
    # Floating-point
    if dt in ("float", "double", "real"):
        return "FLOAT"
    # BIT type
    if dt == "bit":
        return "NUMBER(38,0)"
    # Date/Time types
    if dt == "datetime":
        return "TIMESTAMP_NTZ"
    if dt == "timestamp":
        return "TIMESTAMP_TZ"
    if dt == "date":
        return "DATE"
    if dt == "time":
        return "TIME"
    # JSON -> VARIANT (Snowflake's native semi-structured type)
    if dt == "json":
        return "VARIANT"
    # ENUM and SET -> VARCHAR (Snowflake has no enum type)
    if dt in ("enum", "set"):
        return "VARCHAR(512)"
    # Binary/Blob types — behavior depends on blob_mode
    if dt in ("blob", "tinyblob", "mediumblob", "longblob", "binary", "varbinary"):
        if blob_mode == "text":
            return "VARCHAR(16777216)"
        elif blob_mode == "skip":
            return "VARCHAR"
        else:
            return "BINARY"
    # Text types -> VARCHAR (Snowflake VARCHAR max 16 MB)
    if dt in ("text", "tinytext", "mediumtext", "longtext"):
        return "VARCHAR(16777216)"
    # Character types with explicit length
    if dt in ("char", "varchar"):
        n = int(char_len) if char_len else 16777216
        return f"VARCHAR({n})"
    # Fallback
    return "VARCHAR(16777216)"


def get_mysql_columns(mysql_conn, source_db: str, source_table: str,
                      blob_mode: str = "binary") -> list[tuple]:
    """Return ordered list of (NAME, snowflake_type) for a MySQL table.

    blob_mode: "binary" | "text" | "skip" — controls BLOB column mapping.
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
            blob_mode=blob_mode,
        )
        cols.append((r["COLUMN_NAME"].upper(), sf_type))
    cur.close()
    if not cols:
        raise ValueError(
            f"No columns found for {source_db}.{source_table} — check name/grants.")
    return cols


def build_table_ddl(db: str, target_table: str, cols: list[tuple],
                    scd_type: int = 1) -> str:
    """Generate CREATE TABLE IF NOT EXISTS DDL.

    Args:
        scd_type: 0=append, 1=upsert, 2=history. SCD2 adds _VALID_FROM/_VALID_TO/_IS_CURRENT.
    """
    col_defs = [f'    "{name}" {sf_type}' for name, sf_type in cols]
    col_defs += [f'    "{name}" {sf_type}' for name, sf_type in AUDIT_COLS]
    if scd_type == 2:
        col_defs += [f'    "{name}" {sf_type}' for name, sf_type in SCD2_COLS]
    body = ",\n".join(col_defs)
    return (
        f"CREATE TABLE IF NOT EXISTS {db}.{RAW_SCHEMA}.{target_table} (\n"
        f"{body}\n);"
    )


def generate_and_apply(sf_conn, mysql_conn, config: dict) -> dict:
    """Generate + execute DDL for one table. Returns column metadata."""
    source_db = config["SOURCE_DB"]
    source_table = config["SOURCE_TABLE"]
    tgt_db = config.get("TARGET_DB") or target_db(source_db)
    tgt_table = config.get("TARGET_TABLE") or source_table.upper()
    blob_mode = config.get("BLOB_MODE", "binary")
    scd_type = int(config.get("SCD_TYPE") or 1)

    cols = get_mysql_columns(mysql_conn, source_db, source_table, blob_mode=blob_mode)

    cur = sf_conn.cursor()
    try:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {tgt_db}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {tgt_db}.{RAW_SCHEMA}")
        cur.execute(build_table_ddl(tgt_db, tgt_table, cols, scd_type=scd_type))
        # For SCD2 tables on existing tables, add columns if missing
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
