# Teradata-to-Snowflake type mapping and DDL generation via DBC.ColumnsV.
"""ddl_generators.teradata — Generate Snowflake DDL from Teradata metadata.

Reads DBC.ColumnsV for column metadata, maps Teradata types to Snowflake equivalents,
and creates target tables: <TARGET_DB>.RAW.<table> with business + audit columns.
"""
from __future__ import annotations

import re

from ddl_generators import RAW_SCHEMA, AUDIT_COLS, SCD2_COLS

# Teradata system databases to exclude from auto-discover (Appendix 1 from migration guide)
TD_SYSTEM_DATABASES = {
    "DBC", "CRASHDUMPS", "DBCMNGR", "EXTERNAL_AP", "EXTUSER",
    "LOCKLOGSHREDDER", "QCD", "SQLJ", "SYS_CALENDAR", "SYSADMIN",
    "SYSBAR", "SYSJDBC", "SYSLIB", "SYSSPATIAL", "SYSTEMFE",
    "SYSUDTLIB", "SYSUIF", "TD_SERVER_DB", "TD_SYSFNLIB",
    "TD_SYSGPL", "TD_SYSXML", "TDPUSER", "TDQCD", "TDSTATS",
    "TDWM", "DEFAULT",
}


def map_teradata_type(td_type: str) -> str:
    """Map a Teradata column type string to a Snowflake type.

    td_type comes from the DBC.ColumnsV CASE expression, e.g.:
      'INTEGER', 'VARCHAR(200)', 'DECIMAL(18,2)', 'TIMESTAMP(6)',
      'CHAR(50)', 'FLOAT', 'CLOB(2000)', 'PERIOD(DATE)', etc.
    """
    t = (td_type or "").strip().upper()

    # Integer types
    if t in ("BYTEINT", "SMALLINT", "INTEGER", "BIGINT"):
        return "NUMBER(38,0)"

    # Decimal / Numeric with precision
    m = re.match(r"(DECIMAL|NUMERIC|NUMBER)\(([^)]+)\)", t)
    if m:
        params = m.group(2)
        if "*" in params:
            return "NUMBER(38,18)"
        parts = [p.strip() for p in params.split(",")]
        p = int(parts[0]) if parts[0] else 38
        s = int(parts[1]) if len(parts) > 1 and parts[1] else 0
        if p > 38:
            return "VARCHAR"
        return f"NUMBER({p},{s})"
    if t in ("DECIMAL", "NUMERIC", "NUMBER"):
        return "NUMBER(38,0)"

    # Float / Real / Double
    if t in ("FLOAT", "REAL", "DOUBLE PRECISION"):
        return "FLOAT"

    # Date
    if t in ("DATE", "INTDATE"):
        return "DATE"

    # Time (with optional precision)
    if t.startswith("TIME(") or t == "TIME":
        if "WITH TIME ZONE" in t:
            return "TIME"
        return "TIME"

    # Timestamp
    if t.startswith("TIMESTAMP"):
        if "WITH TIME ZONE" in t:
            return "TIMESTAMP_TZ"
        return "TIMESTAMP_NTZ"

    # Interval types → VARCHAR (Snowflake has no INTERVAL)
    if t.startswith("INTERVAL"):
        return "VARCHAR(128)"

    # Period types → VARCHAR
    if t.startswith("PERIOD"):
        return "VARCHAR(128)"

    # Character types
    m = re.match(r"(CHAR|CHARACTER)\((\d+)\)", t)
    if m:
        n = int(m.group(2))
        return f"VARCHAR({n})"
    if t in ("CHAR", "CHARACTER"):
        return "VARCHAR(255)"

    m = re.match(r"VARCHAR\((\d+)\)", t)
    if m:
        n = int(m.group(1))
        if n > 16777216:
            n = 16777216
        return f"VARCHAR({n})"
    if t == "VARCHAR":
        return "VARCHAR(16777216)"

    # CLOB → VARCHAR (max 16MB in Snowflake)
    if t.startswith("CLOB"):
        return "VARCHAR(16777216)"

    # BLOB / BYTE / VARBYTE → BINARY
    if t.startswith("BLOB") or t.startswith("BYTE") or t.startswith("VARBYTE"):
        return "BINARY"

    # JSON type
    if t.startswith("JSON"):
        return "VARIANT"

    # XML type
    if t == "XML":
        return "VARCHAR(16777216)"

    # TD_ANYTYPE / TD_VALIST / UDT
    if t.startswith("TD_") or t.startswith("SYSUDTLIB."):
        return "VARCHAR(16777216)"

    # Fallback
    return "VARCHAR(16777216)"


# SQL to query DBC.ColumnsV for column metadata
_COLUMN_INFO_SQL = """
SELECT
    TRIM(c.ColumnName) AS COL_NAME,
    CASE c.ColumnType
        WHEN 'BF' THEN 'BYTE(' || TRIM(c.ColumnLength (FORMAT 'Z(9)9')) || ')'
        WHEN 'BV' THEN 'VARBYTE(' || TRIM(c.ColumnLength (FORMAT 'Z(9)9')) || ')'
        WHEN 'CF' THEN 'CHAR(' || TRIM(CASE WHEN c.ColumnLength > 64000 THEN 64000 ELSE c.ColumnLength END (FORMAT 'Z(9)9')) || ')'
        WHEN 'CV' THEN 'VARCHAR(' || TRIM(CASE WHEN c.ColumnLength > 64000 THEN 64000 ELSE c.ColumnLength END (FORMAT 'Z(9)9')) || ')'
        WHEN 'D ' THEN 'DECIMAL(' || TRIM(c.DecimalTotalDigits (FORMAT '-(9)9')) || ',' || TRIM(c.DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
        WHEN 'DA' THEN 'DATE'
        WHEN 'F ' THEN 'FLOAT'
        WHEN 'I1' THEN 'BYTEINT'
        WHEN 'I2' THEN 'SMALLINT'
        WHEN 'I8' THEN 'BIGINT'
        WHEN 'I ' THEN 'INTEGER'
        WHEN 'AT' THEN 'TIME(' || TRIM(c.DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
        WHEN 'TS' THEN 'TIMESTAMP(' || TRIM(c.DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
        WHEN 'TZ' THEN 'TIME(' || TRIM(c.DecimalFractionalDigits (FORMAT '-(9)9')) || ') WITH TIME ZONE'
        WHEN 'SZ' THEN 'TIMESTAMP(' || TRIM(c.DecimalFractionalDigits (FORMAT '-(9)9')) || ') WITH TIME ZONE'
        WHEN 'BO' THEN 'BLOB(' || TRIM(c.ColumnLength (FORMAT 'Z(9)9')) || ')'
        WHEN 'CO' THEN 'CLOB(' || TRIM(c.ColumnLength (FORMAT 'Z(9)9')) || ')'
        WHEN 'PD' THEN 'PERIOD(DATE)'
        WHEN 'PS' THEN 'PERIOD(TIMESTAMP(' || TRIM(c.DecimalFractionalDigits (FORMAT '-(9)9')) || '))'
        WHEN 'PT' THEN 'PERIOD(TIME(' || TRIM(c.DecimalFractionalDigits (FORMAT '-(9)9')) || '))'
        WHEN 'PM' THEN 'PERIOD(TIMESTAMP(' || TRIM(c.DecimalFractionalDigits (FORMAT '-(9)9')) || ') WITH TIME ZONE)'
        WHEN 'PZ' THEN 'PERIOD(TIME(' || TRIM(c.DecimalFractionalDigits (FORMAT '-(9)9')) || ') WITH TIME ZONE)'
        WHEN 'N'  THEN 'NUMBER(' || CASE WHEN c.DecimalTotalDigits = -128 THEN '*' ELSE TRIM(c.DecimalTotalDigits (FORMAT '-(9)9')) END
                    || CASE WHEN c.DecimalFractionalDigits IN (0, -128) THEN '' ELSE ',' || TRIM(c.DecimalFractionalDigits (FORMAT '-(9)9')) END || ')'
        WHEN 'JN' THEN 'JSON(' || TRIM(c.ColumnLength (FORMAT 'Z(9)9')) || ')'
        WHEN 'XM' THEN 'XML'
        WHEN 'YR' THEN 'INTERVAL YEAR'
        WHEN 'YM' THEN 'INTERVAL YEAR TO MONTH'
        WHEN 'MO' THEN 'INTERVAL MONTH'
        WHEN 'DY' THEN 'INTERVAL DAY'
        WHEN 'DH' THEN 'INTERVAL DAY TO HOUR'
        WHEN 'DM' THEN 'INTERVAL DAY TO MINUTE'
        WHEN 'DS' THEN 'INTERVAL DAY TO SECOND'
        WHEN 'HR' THEN 'INTERVAL HOUR'
        WHEN 'HM' THEN 'INTERVAL HOUR TO MINUTE'
        WHEN 'HS' THEN 'INTERVAL HOUR TO SECOND'
        WHEN 'MI' THEN 'INTERVAL MINUTE'
        WHEN 'MS' THEN 'INTERVAL MINUTE TO SECOND'
        WHEN 'SC' THEN 'INTERVAL SECOND'
        ELSE 'VARCHAR(16777216)'
    END AS TD_DATA_TYPE
FROM DBC.ColumnsV c
WHERE UPPER(c.DatabaseName) = UPPER('{database}')
  AND UPPER(c.TableName) = UPPER('{table}')
ORDER BY c.ColumnId
"""


def get_teradata_columns(td_conn, source_db: str, source_table: str) -> list[tuple]:
    """Return ordered list of (NAME, snowflake_type) for a Teradata table.

    Args:
        td_conn: Active teradatasql connection
        source_db: Teradata database name
        source_table: Teradata table name

    Returns:
        List of (COLUMN_NAME, SNOWFLAKE_TYPE) tuples
    """
    sql = _COLUMN_INFO_SQL.format(database=source_db, table=source_table)
    cur = td_conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()

    cols = []
    for row in rows:
        col_name = str(row[0]).strip().upper()
        td_type = str(row[1]).strip() if row[1] else "VARCHAR(16777216)"
        sf_type = map_teradata_type(td_type)
        cols.append((col_name, sf_type))

    if not cols:
        raise ValueError(
            f"No columns found for {source_db}.{source_table} in DBC.ColumnsV — "
            "check database/table name and grants.")
    return cols


def build_table_ddl(db: str, target_table: str, cols: list[tuple],
                    scd_type: int = 1) -> str:
    """Generate CREATE TABLE IF NOT EXISTS DDL for Teradata-sourced table."""
    col_defs = [f'    "{name}" {sf_type}' for name, sf_type in cols]
    col_defs += [f'    "{name}" {sf_type}' for name, sf_type in AUDIT_COLS]
    if scd_type == 2:
        col_defs += [f'    "{name}" {sf_type}' for name, sf_type in SCD2_COLS]
    body = ",\n".join(col_defs)
    return (
        f"CREATE TABLE IF NOT EXISTS {db}.{RAW_SCHEMA}.{target_table} (\n"
        f"{body}\n);"
    )


def _resolve_td_schema(config: dict) -> str:
    """Resolve target schema for Teradata tables.

    Priority:
      1. Explicit TARGET_SCHEMA from config
      2. SOURCE_DB (if TARGET_DB differs from SOURCE_DB → consolidation mode)
      3. RAW (default, same as MySQL)
    """
    if config.get("TARGET_SCHEMA"):
        return config["TARGET_SCHEMA"].strip().upper()
    tgt_db = (config.get("TARGET_DB") or "").strip().upper()
    src_db = config["SOURCE_DB"].strip().upper()
    if tgt_db and tgt_db != src_db:
        # Consolidation: TD database becomes the schema
        return src_db
    return RAW_SCHEMA


def _resolve_td_table_name(config: dict) -> str:
    """Resolve target table name for Teradata — appends _RAW suffix."""
    base = config.get("TARGET_TABLE") or config["SOURCE_TABLE"].upper()
    if not base.endswith("_RAW"):
        return f"{base}_RAW"
    return base


def generate_and_apply(sf_conn, td_conn, config: dict) -> dict:
    """Generate + execute DDL for one Teradata table. Returns column metadata."""
    source_db = config["SOURCE_DB"]
    source_table = config["SOURCE_TABLE"]
    tgt_db = config.get("TARGET_DB") or source_db.strip().upper()
    tgt_schema = _resolve_td_schema(config)
    tgt_table = _resolve_td_table_name(config)
    scd_type = int(config.get("SCD_TYPE") or 1)

    cols = get_teradata_columns(td_conn, source_db, source_table)

    cur = sf_conn.cursor()
    try:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {tgt_db}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {tgt_db}.{tgt_schema}")
        ddl = build_table_ddl(tgt_db, tgt_table, cols, scd_type=scd_type)
        # Replace hardcoded RAW_SCHEMA in DDL with resolved schema
        ddl = ddl.replace(f"{tgt_db}.{RAW_SCHEMA}.", f"{tgt_db}.{tgt_schema}.")
        cur.execute(ddl)
        if scd_type == 2:
            for col_name, col_def in SCD2_COLS:
                try:
                    cur.execute(
                        f'ALTER TABLE {tgt_db}.{tgt_schema}.{tgt_table} '
                        f'ADD COLUMN IF NOT EXISTS "{col_name}" {col_def}')
                except Exception:
                    pass
        print(f"   DDL ready: {tgt_db}.{tgt_schema}.{tgt_table} "
              f"({len(cols)} cols + audit"
              f"{' + SCD2' if scd_type == 2 else ''})")
    finally:
        cur.close()

    return {"columns": cols, "target_db": tgt_db, "target_table": tgt_table,
            "target_schema": tgt_schema}


def list_tables(td_conn, database: str) -> list[str]:
    """List user tables in a Teradata database, excluding system databases.

    Args:
        td_conn: Active teradatasql connection
        database: Teradata database name to list tables from

    Returns:
        Sorted list of table names
    """
    if database.upper() in TD_SYSTEM_DATABASES:
        return []

    cur = td_conn.cursor()
    cur.execute(
        "SELECT TRIM(TableName) FROM DBC.TablesV "
        "WHERE UPPER(DatabaseName) = UPPER(?) AND TableKind = 'T' "
        "ORDER BY TableName", (database,))
    tables = [row[0] for row in cur.fetchall()]
    cur.close()
    return tables


def list_databases(td_conn) -> list[str]:
    """List non-system Teradata databases available for migration.

    Filters out all known Teradata system databases per the migration guide.

    Returns:
        Sorted list of user database names
    """
    cur = td_conn.cursor()
    cur.execute("SELECT TRIM(DatabaseName) FROM DBC.DatabasesV ORDER BY DatabaseName")
    all_dbs = [row[0] for row in cur.fetchall()]
    cur.close()
    return [db for db in all_dbs if db.upper() not in TD_SYSTEM_DATABASES]
