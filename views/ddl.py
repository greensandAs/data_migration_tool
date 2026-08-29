# DDL Viewer — source DDL, converted Snowflake DDL, and AI validation of type mapping.
"""views/ddl.py — DDL Conversion Viewer with AI Validation.

Shows side-by-side comparison of source DDL (MySQL/MSSQL/Oracle/Teradata) and the
converted Snowflake DDL. Uses Cortex AI to validate data type conversions.
"""
from __future__ import annotations

import os
import time

import streamlit as st

from metadata import config_manager
from metadata import connection_manager


_RETRYABLE_HINTS = (
    "forcibly closed", "communication link failure", "connection reset",
    "10054", "08s01", "timeout", "timed out", "connection refused",
    "broken pipe", "server has gone away", "lost connection", "network error",
)


def _with_retry(fn, max_attempts: int = 3, base_delay: float = 2.0):
    """Call fn() with retry on transient connection errors."""
    attempt = 1
    while True:
        try:
            return fn()
        except Exception as e:
            msg = str(e).lower()
            retryable = any(h in msg for h in _RETRYABLE_HINTS)
            if attempt >= max_attempts or not retryable:
                raise
            delay = base_delay * (2 ** (attempt - 1))
            time.sleep(delay)
            attempt += 1


def _get_source_ddl_mysql(profile: dict, source_db: str, source_table: str) -> str:
    """Extract CREATE TABLE DDL from MySQL."""
    import mysql.connector
    password = profile.get("PASSWORD") or os.getenv(profile.get("AUTH_SECRET") or "", "") or ""
    conn = mysql.connector.connect(
        host=str(profile["HOST"]), port=int(profile["PORT"]),
        user=str(profile["USERNAME"]), password=str(password))
    cur = conn.cursor()
    cur.execute(f"SHOW CREATE TABLE `{source_db}`.`{source_table}`")
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[1] if row else f"-- Could not retrieve DDL for {source_db}.{source_table}"


def _get_source_ddl_mssql(profile: dict, source_db: str, source_table: str,
                           source_schema: str = "dbo") -> str:
    """Extract CREATE TABLE DDL from MSSQL via INFORMATION_SCHEMA."""
    import pyodbc
    password = profile.get("PASSWORD") or os.getenv(profile.get("AUTH_SECRET") or "", "") or ""
    driver = profile.get("DRIVER") or "ODBC Driver 17 for SQL Server"
    host = str(profile["HOST"])
    port = int(profile.get("PORT") or 1433)
    conn_str = (
        f"DRIVER={{{driver}}};SERVER={host},{port};DATABASE={source_db};"
        f"UID={profile['USERNAME']};PWD={password};"
        "Encrypt=yes;TrustServerCertificate=yes;"
        "LoginTimeout=10;Connection Timeout=10;"
    )
    conn = pyodbc.connect(conn_str, timeout=10)
    cur = conn.cursor()
    cur.execute("""
        SELECT c.COLUMN_NAME, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH,
               c.NUMERIC_PRECISION, c.NUMERIC_SCALE, c.IS_NULLABLE,
               c.COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION
    """, (source_schema, source_table))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        return f"-- No columns found for [{source_schema}].[{source_table}] in {source_db}"

    lines = [f"CREATE TABLE [{source_schema}].[{source_table}] ("]
    for i, r in enumerate(rows):
        col_name, dtype, max_len, prec, scale, nullable, default = r
        type_str = dtype.upper()
        if dtype in ("varchar", "nvarchar", "char", "nchar"):
            l = "MAX" if (max_len and int(max_len) == -1) else str(max_len or "")
            type_str = f"{dtype.upper()}({l})"
        elif dtype in ("decimal", "numeric"):
            type_str = f"{dtype.upper()}({prec},{scale})"
        null_str = "NULL" if nullable == "YES" else "NOT NULL"
        comma = "," if i < len(rows) - 1 else ""
        lines.append(f"    [{col_name}] {type_str} {null_str}{comma}")
    lines.append(");")
    return "\n".join(lines)


def _get_source_ddl_oracle(profile: dict, source_db: str, source_table: str) -> str:
    """Extract CREATE TABLE DDL from Oracle via ALL_TAB_COLUMNS."""
    import oracledb
    password = profile.get("PASSWORD") or os.getenv(profile.get("AUTH_SECRET") or "", "") or ""
    extra = profile.get("EXTRA_PARAMS") or {}
    if isinstance(extra, str):
        import json
        try:
            extra = json.loads(extra)
        except Exception:
            extra = {}
    service = extra.get("service_name") or profile.get("SERVICE_NAME") or ""
    dsn = f"{profile['HOST']}:{profile.get('PORT', 1521)}/{service}"
    conn = oracledb.connect(user=str(profile["USERNAME"]), password=str(password), dsn=dsn)
    cur = conn.cursor()
    cur.execute("""
        SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH,
               DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = :1 AND TABLE_NAME = :2
        ORDER BY COLUMN_ID
    """, (source_db.upper(), source_table.upper()))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        return f"-- No columns found for {source_db}.{source_table}"

    lines = [f"CREATE TABLE {source_db}.{source_table} ("]
    for i, r in enumerate(rows):
        col_name, dtype, data_len, prec, scale, nullable, default = r
        type_str = dtype
        if dtype in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "RAW"):
            type_str = f"{dtype}({data_len})"
        elif dtype == "NUMBER":
            if prec is not None:
                type_str = f"NUMBER({prec},{scale or 0})"
            else:
                type_str = "NUMBER"
        elif "TIMESTAMP" in dtype:
            type_str = dtype
        null_str = "NOT NULL" if nullable == "N" else ""
        comma = "," if i < len(rows) - 1 else ""
        parts = [f"    {col_name}", type_str]
        if null_str:
            parts.append(null_str)
        lines.append(f"{'  '.join(parts)}{comma}")
    lines.append(");")
    return "\n".join(lines)


def _format_source_ddl(ddl: str) -> str:
    """Format a raw DDL string for readability (add line breaks at structural points)."""
    import re
    # If already multi-line and readable, return as-is
    if ddl.count("\n") > 3:
        return ddl
    # Add line breaks at key SQL structural points
    ddl = re.sub(r"\(\s*", "(\n    ", ddl, count=1)  # after opening paren
    ddl = re.sub(r",\s*(?=[A-Za-z_\"])", ",\n    ", ddl)  # before each column
    ddl = re.sub(r"\)\s*(PRIMARY INDEX|UNIQUE PRIMARY|NO PRIMARY|PARTITION BY|;)", r"\n) \1", ddl)
    ddl = re.sub(r"\)\s*$", "\n);", ddl)
    return ddl


def _get_source_ddl_teradata(profile: dict, source_db: str, source_table: str) -> str:
    """Extract CREATE TABLE DDL from Teradata via SHOW TABLE."""
    import teradatasql
    password = profile.get("PASSWORD") or os.getenv(profile.get("AUTH_SECRET") or "", "") or ""
    conn = teradatasql.connect(
        host=str(profile["HOST"]),
        user=str(profile["USERNAME"]),
        password=str(password),
        logmech=profile.get("LOGMECH", "TD2"))
    cur = conn.cursor()
    try:
        cur.execute(f"SHOW TABLE {source_db}.{source_table}")
        rows = cur.fetchall()
        ddl = "\n".join(str(r[0]) for r in rows)
    except Exception:
        # Fallback: reconstruct from DBC.ColumnsV
        from ddl_generators.teradata import get_teradata_columns
        cols = get_teradata_columns(conn, source_db, source_table)
        ddl = f"-- Reconstructed from DBC.ColumnsV\nCREATE TABLE {source_db}.{source_table} (\n"
        ddl += ",\n".join(f"    {name}  /* → {sf_type} */" for name, sf_type in cols)
        ddl += "\n);"
    cur.close()
    conn.close()
    return _format_source_ddl(ddl)


def _get_snowflake_ddl(sf_cur, config: dict, source_type: str,
                       mapping: list[dict] | None = None) -> tuple[str, list[tuple]]:
    """Generate the Snowflake DDL without executing it. Returns (ddl_string, columns).

    If the table exists in Snowflake, returns GET_DDL output.
    Otherwise, generates a preview DDL from the type mapping.
    """
    from ddl_generators import RAW_SCHEMA, AUDIT_COLS, SCD2_COLS

    if source_type == "teradata":
        from ddl_generators.teradata import _resolve_td_table_name
        tgt_db = config.get("TARGET_DB") or config["SOURCE_DB"].strip().upper()
        tgt_schema = config.get("TARGET_SCHEMA") or RAW_SCHEMA
        tgt_table = _resolve_td_table_name(config)
    else:
        from ddl_generators import target_db
        tgt_db = config.get("TARGET_DB") or target_db(config["SOURCE_DB"])
        tgt_table = config.get("TARGET_TABLE") or config["SOURCE_TABLE"].upper()
        tgt_schema = config.get("TARGET_SCHEMA") or RAW_SCHEMA

    # Try to read existing table DDL from Snowflake
    try:
        sf_cur.execute(f"SELECT GET_DDL('TABLE', '{tgt_db}.{tgt_schema}.{tgt_table}')")
        row = sf_cur.fetchone()
        if row:
            return row[0], []
    except Exception:
        pass

    # Table doesn't exist yet — generate preview from mapping
    if mapping:
        scd_type = int(config.get("SCD_TYPE") or 1)
        col_defs = [f'    "{m["Column"].upper()}" {m["Snowflake Type"]}' for m in mapping]
        col_defs += [f'    "{name}" {sf_type}' for name, sf_type in AUDIT_COLS]
        if scd_type == 2:
            col_defs += [f'    "{name}" {sf_type}' for name, sf_type in SCD2_COLS]
        body = ",\n".join(col_defs)
        preview = (
            f"-- PREVIEW (table not yet created)\n"
            f"CREATE TABLE IF NOT EXISTS {tgt_db}.{tgt_schema}.{tgt_table} (\n"
            f"{body}\n);")
        return preview, []

    return (f"-- Table not yet created: {tgt_db}.{tgt_schema}.{tgt_table}\n"
            f"-- Run pipeline DDL step first."), []


def _get_type_mapping(sf_cur, config: dict, source_type: str, profile: dict) -> list[dict]:
    """Get column-by-column type mapping comparison."""
    password = profile.get("PASSWORD") or os.getenv(profile.get("AUTH_SECRET") or "", "") or ""

    if source_type == "teradata":
        import teradatasql
        from ddl_generators.teradata import _COLUMN_INFO_SQL, map_teradata_type
        conn = teradatasql.connect(
            host=str(profile["HOST"]),
            user=str(profile["USERNAME"]),
            password=str(password),
            logmech=profile.get("LOGMECH", "TD2"))
        cur = conn.cursor()
        sql = _COLUMN_INFO_SQL.format(database=config["SOURCE_DB"], table=config["SOURCE_TABLE"])
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        mapping = []
        for row in rows:
            col_name = str(row[0]).strip()
            td_type = str(row[1]).strip()
            sf_type = map_teradata_type(td_type)
            mapping.append({"Column": col_name, "Source Type": td_type, "Snowflake Type": sf_type})
        return mapping

    elif source_type == "mssql":
        import pyodbc
        from ddl_generators.mssql import map_mssql_type
        host = profile.get("HOST", "")
        port = profile.get("PORT", 1433)
        schema = profile.get("SCHEMA", "dbo")
        driver = profile.get("DRIVER") or "ODBC Driver 17 for SQL Server"
        conn_str = (
            f"DRIVER={{{driver}}};SERVER={host},{port};"
            f"DATABASE={config['SOURCE_DB']};"
            f"UID={profile.get('USERNAME', '')};PWD={password};"
            "Encrypt=yes;TrustServerCertificate=yes;"
            "LoginTimeout=10;Connection Timeout=10;"
        )
        conn = pyodbc.connect(conn_str, timeout=10)
        cur = conn.cursor()
        cur.execute(
            "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, "
            "NUMERIC_PRECISION, NUMERIC_SCALE "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
            "ORDER BY ORDINAL_POSITION",
            (schema, config["SOURCE_TABLE"]))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        mapping = []
        for r in rows:
            col_name, dt, max_len, prec, scale = r
            sf_type = map_mssql_type(dt, max_len, prec, scale)
            src_display = dt.upper()
            if dt in ("varchar", "nvarchar", "char", "nchar"):
                src_display = f"{dt.upper()}({max_len if max_len and max_len > 0 else 'MAX'})"
            elif dt in ("decimal", "numeric"):
                src_display = f"{dt.upper()}({prec},{scale})"
            mapping.append({"Column": col_name, "Source Type": src_display, "Snowflake Type": sf_type})
        return mapping

    elif source_type == "oracle":
        import oracledb
        from ddl_generators.oracle import map_oracle_type
        extra = profile.get("EXTRA_PARAMS") or {}
        if isinstance(extra, str):
            import json
            try:
                extra = json.loads(extra)
            except Exception:
                extra = {}
        service = extra.get("service_name") or profile.get("SERVICE_NAME") or profile.get("SERVICE") or ""
        dsn = f"{profile['HOST']}:{profile.get('PORT', 1521)}/{service}"
        conn = oracledb.connect(user=str(profile["USERNAME"]), password=str(password), dsn=dsn)
        cur = conn.cursor()
        cur.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = :1 AND TABLE_NAME = :2
            ORDER BY COLUMN_ID
        """, (config["SOURCE_DB"].upper(), config["SOURCE_TABLE"].upper()))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        mapping = []
        for r in rows:
            col_name, dt, data_len, prec, scale = r
            sf_type = map_oracle_type(dt, data_len, prec, scale)
            src_display = dt
            if dt in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "RAW"):
                src_display = f"{dt}({data_len})"
            elif dt == "NUMBER":
                src_display = f"NUMBER({prec},{scale or 0})" if prec is not None else "NUMBER"
            mapping.append({"Column": col_name, "Source Type": src_display, "Snowflake Type": sf_type})
        return mapping

    else:  # mysql (default)
        import mysql.connector
        from ddl_generators import map_mysql_type
        conn = mysql.connector.connect(
            host=str(profile["HOST"]), port=int(profile["PORT"]),
            user=str(profile["USERNAME"]), password=str(password))
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, "
            "NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_MAXIMUM_LENGTH "
            "FROM information_schema.columns "
            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION",
            (config["SOURCE_DB"], config["SOURCE_TABLE"]))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        mapping = []
        for r in rows:
            sf_type = map_mysql_type(
                r["DATA_TYPE"], r["COLUMN_TYPE"],
                r["NUMERIC_PRECISION"], r["NUMERIC_SCALE"],
                r["CHARACTER_MAXIMUM_LENGTH"])
            mapping.append({
                "Column": r["COLUMN_NAME"],
                "Source Type": r["COLUMN_TYPE"],
                "Snowflake Type": sf_type,
            })
        return mapping


def _ai_validate_mapping(sf_cur, mapping: list[dict], source_type: str) -> str:
    """Validate type mapping using Org AI Gateway or rule-based fallback.

    LLM priority:
      1. Per-feature model (LLM_MODEL_DDL) from DMT_SETTINGS
      2. Global model (LLM_MODEL) from DMT_SETTINGS
      3. User's UI selection
      4. Rule-based validation (if gateway not configured)
    """
    from utils.shared import get_setting, cortex_complete

    api_base = get_setting(sf_cur, "LLM_API_BASE") or os.getenv("LLM_API_BASE", "")
    api_key = get_setting(sf_cur, "LLM_API_KEY") or os.getenv("LLM_API_KEY", "")

    # Only use AI if gateway is configured
    if not api_base or not api_key:
        return _rule_based_validate(mapping, source_type)

    # Build concise prompt to minimize tokens
    mapping_text = "\n".join(
        f"{m['Column']}:{m['Source Type']}→{m['Snowflake Type']}"
        for m in mapping
    )
    prompt = (
        f"{source_type.upper()} to Snowflake type mapping. Flag ONLY issues "
        f"(precision loss, truncation, timezone loss). Be brief.\n\n"
        f"{mapping_text}\n\n"
        "Reply: PASS or list issues only. Max 5 lines."
    )

    result = cortex_complete(prompt, feature="ddl", conn=sf_cur.connection)
    if result.startswith("("):
        # Error from gateway — fallback to rule-based
        return (f"**AI unavailable:** {result}\n\n---\n\n" +
                _rule_based_validate(mapping, source_type))
    return f"**AI Review:**\n\n{result}"


def _classify_columns(mapping: list[dict], source_type: str) -> list[dict]:
    """Classify each column mapping as safe/warning/issue. Returns enriched mapping list."""
    import re
    result = []
    for m in mapping:
        src = m["Source Type"].upper()
        sf = m["Snowflake Type"].upper()
        col = m["Column"]
        status = "safe"
        note = ""

        # Precision loss
        if "DECIMAL" in src or "NUMERIC" in src or "NUMBER" in src:
            prec_match = re.search(r"\((\d+)", src)
            if prec_match and int(prec_match.group(1)) > 38:
                status, note = "issue", "Precision > 38 → VARCHAR (precision loss)"

        # CLOB/TEXT truncation
        if "CLOB" in src or "LONGTEXT" in src or "MEDIUMTEXT" in src or "NTEXT" in src:
            if "16777216" in sf:
                status, note = "warning", "Source may exceed 16MB VARCHAR limit"

        # BLOB/BINARY
        if ("BLOB" in src or "IMAGE" in src or "LONG RAW" in src) and "BINARY" in sf:
            status, note = "warning", "Consider VARCHAR if data is text/JSON"

        # Timezone checks
        if source_type == "teradata" and "WITH TIME ZONE" in src and "TIMESTAMP_TZ" not in sf:
            status, note = "issue", "Timezone lost in mapping"
        if source_type == "mssql":
            if "DATETIMEOFFSET" in src and "TIMESTAMP_TZ" not in sf:
                status, note = "issue", "Timezone lost in mapping"
            if src in ("SQL_VARIANT",) and "VARCHAR" in sf:
                status, note = "warning", "Runtime type info lost"
            if src in ("XML",) and "VARIANT" in sf:
                status, note = "warning", "XQuery not available in Snowflake"
            if src in ("GEOGRAPHY", "GEOMETRY") and "VARCHAR" in sf:
                status, note = "warning", "Use Snowflake GEOGRAPHY type for spatial queries"
            if src in ("HIERARCHYID",):
                status, note = "warning", "Hierarchy path stored as string"
        if source_type == "oracle":
            if "WITH TIME ZONE" in src and "TIMESTAMP_TZ" not in sf:
                status, note = "issue", "Timezone lost in mapping"
            if src == "NUMBER" and sf == "FLOAT":
                status, note = "warning", "NUMBER (no precision) → FLOAT; consider NUMBER(38,0)"
            if "XMLTYPE" in src and "VARIANT" in sf:
                status, note = "warning", "Oracle XML functions not available"
            if "SDO_GEOMETRY" in src:
                status, note = "warning", "Use Snowflake GEOGRAPHY for spatial queries"
            if "BFILE" in src:
                status, note = "warning", "BFILE is a file pointer — content won't migrate"
            if src in ("ROWID", "UROWID"):
                status, note = "warning", "Physical row address, not meaningful in Snowflake"

        # PERIOD / INTERVAL
        if "PERIOD" in src and "VARCHAR" in sf:
            status, note = "warning", "No PERIOD type in Snowflake — stored as string"
        if "INTERVAL" in src and "VARCHAR" in sf:
            status, note = "warning", "No INTERVAL type in Snowflake — stored as string"

        result.append({**m, "Status": status, "Note": note})
    return result


def _detect_schema_drift(sf_cur, config: dict, source_type: str,
                         mapping: list[dict]) -> list[dict]:
    """Compare source columns with existing Snowflake table. Returns drift list."""
    from ddl_generators import RAW_SCHEMA

    if source_type == "teradata":
        from ddl_generators.teradata import _resolve_td_table_name
        tgt_db = config.get("TARGET_DB") or config["SOURCE_DB"].strip().upper()
        tgt_table = _resolve_td_table_name(config)
    else:
        from ddl_generators import target_db
        tgt_db = config.get("TARGET_DB") or target_db(config["SOURCE_DB"])
        tgt_table = config.get("TARGET_TABLE") or config["SOURCE_TABLE"].upper()
    tgt_schema = config.get("TARGET_SCHEMA") or RAW_SCHEMA

    try:
        sf_cur.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM {tgt_db}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{tgt_schema}' AND TABLE_NAME = '{tgt_table}'
            ORDER BY ORDINAL_POSITION
        """)
        sf_cols = {row[0].upper(): row[1] for row in sf_cur.fetchall()}
    except Exception:
        return []

    if not sf_cols:
        return []

    from ddl_generators import AUDIT_COLS
    audit_names = {name.upper() for name, _ in AUDIT_COLS}

    drifts = []
    src_col_names = {m["Column"].upper() for m in mapping}

    # New columns in source not in Snowflake
    for m in mapping:
        col_upper = m["Column"].upper()
        if col_upper not in sf_cols:
            drifts.append({"Column": m["Column"], "Drift": "NEW",
                           "Source Type": m["Source Type"],
                           "Snowflake Type": m["Snowflake Type"],
                           "Detail": "Exists in source but not in Snowflake target"})
        elif m["Snowflake Type"].upper() != sf_cols[col_upper].upper():
            drifts.append({"Column": m["Column"], "Drift": "TYPE_CHANGED",
                           "Source Type": m["Source Type"],
                           "Snowflake Type": m["Snowflake Type"],
                           "Current SF Type": sf_cols[col_upper],
                           "Detail": f"Mapped to {m['Snowflake Type']} but Snowflake has {sf_cols[col_upper]}"})

    # Columns in Snowflake not in source (excluding audit cols)
    for sf_col in sf_cols:
        if sf_col not in src_col_names and sf_col not in audit_names:
            drifts.append({"Column": sf_col, "Drift": "DROPPED",
                           "Source Type": "—",
                           "Snowflake Type": sf_cols[sf_col],
                           "Detail": "Exists in Snowflake but not in source (may have been dropped)"})
    return drifts


def _rule_based_validate(mapping: list[dict], source_type: str) -> str:
    """Rule-based validation — delegates to _classify_columns and formats as markdown."""
    classified = _classify_columns(mapping, source_type)
    issues = [c for c in classified if c["Status"] == "issue"]
    warnings = [c for c in classified if c["Status"] == "warning"]
    total = len(classified)

    if not issues and not warnings:
        result = f"**PASS** — All {total} column mappings are correct. No issues detected."
    elif not issues:
        result = f"**PASS (with warnings)** — {total} columns checked, {len(warnings)} warning(s):\n\n"
        result += "\n".join(f'- **{w["Column"]}**: `{w["Source Type"]}` → `{w["Snowflake Type"]}` — {w["Note"]}' for w in warnings)
    else:
        result = f"**ISSUES FOUND** — {len(issues)} issue(s), {len(warnings)} warning(s):\n\n"
        if issues:
            result += "**Issues (may affect data):**\n"
            result += "\n".join(f'- **{i["Column"]}**: `{i["Source Type"]}` → `{i["Snowflake Type"]}` — {i["Note"]}' for i in issues) + "\n\n"
        if warnings:
            result += "**Warnings (informational):**\n"
            result += "\n".join(f'- **{w["Column"]}**: `{w["Source Type"]}` → `{w["Snowflake Type"]}` — {w["Note"]}' for w in warnings)

    result += "\n\n---\n*Validated using rule-based checks*"
    return result


def _fallback_mapping_from_sf(sf_cur, config: dict, source_type: str) -> list[dict]:
    """Fallback: read column info from the Snowflake target table when source is unreachable."""
    from ddl_generators import RAW_SCHEMA, AUDIT_COLS
    if source_type == "teradata":
        from ddl_generators.teradata import _resolve_td_table_name
        tgt_db = config.get("TARGET_DB") or config["SOURCE_DB"].strip().upper()
        tgt_table = _resolve_td_table_name(config)
    else:
        from ddl_generators import target_db
        tgt_db = config.get("TARGET_DB") or target_db(config["SOURCE_DB"])
        tgt_table = config.get("TARGET_TABLE") or config["SOURCE_TABLE"].upper()
    tgt_schema = config.get("TARGET_SCHEMA") or RAW_SCHEMA

    audit_names = {n.upper() for n, _ in AUDIT_COLS}
    try:
        sf_cur.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM {tgt_db}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{tgt_schema}' AND TABLE_NAME = '{tgt_table}'
            ORDER BY ORDINAL_POSITION
        """)
        rows = sf_cur.fetchall()
        return [{"Column": r[0], "Source Type": "(from Snowflake)", "Snowflake Type": r[1]}
                for r in rows if r[0].upper() not in audit_names]
    except Exception:
        return []


def render(conn):
    """Main render function for the DDL page."""
    cur = conn.cursor()

    st.markdown('<div class="section-header">DDL Conversion Viewer</div>',
                unsafe_allow_html=True)
    st.caption("Compare source DDL with converted Snowflake DDL. "
               "AI validates data type mappings for correctness.")

    # Get selected profile
    _sidebar_profile = st.session_state.get("selected_profile", "All Connections")
    if _sidebar_profile == "All Connections":
        from utils.shared import empty_state
        empty_state("🏗️", "Select a Source Connection",
                    "Choose a connection from the sidebar to view DDL conversions.")
        cur.close()
        return

    sel_profile = connection_manager.get_profile(cur, _sidebar_profile)
    if not sel_profile:
        st.error(f"Profile '{_sidebar_profile}' not found.")
        cur.close()
        return

    source_type = (sel_profile.get("SOURCE_TYPE") or "mysql").lower()

    # Get tables for this profile
    tables = config_manager.list_all(cur, connection_profile=_sidebar_profile)
    if not tables:
        from utils.shared import empty_state
        empty_state("📋", "No Tables Configured",
                    "Add tables in the <b>Config</b> page first.")
        cur.close()
        return

    # Table selector
    table_names = [f"{t['SOURCE_DB']}.{t['SOURCE_TABLE']}" for t in tables]
    selected = st.selectbox("Select Table", table_names, key="ddl_table_select")

    if not selected:
        cur.close()
        return

    # Find the config for selected table
    idx = table_names.index(selected)
    config = tables[idx]

    # Build type mapping first (needed for Snowflake DDL preview if table doesn't exist)
    with st.spinner("Building type mapping..."):
        try:
            mapping = _with_retry(
                lambda: _get_type_mapping(cur, config, source_type, sel_profile))
        except Exception as e:
            # Fallback: reconstruct mapping from Snowflake target table
            mapping = _fallback_mapping_from_sf(cur, config, source_type)
            if mapping:
                st.warning(f"Source unreachable ({type(e).__name__}). "
                           "Showing columns from Snowflake target table instead.")
            else:
                st.error(f"Could not build type mapping: {e}")

    # Layout: Source DDL | Snowflake DDL
    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown(f"#### Source DDL ({source_type.upper()})")
        with st.spinner("Fetching source DDL (retrying if needed)..."):
            try:
                if source_type == "teradata":
                    src_ddl = _with_retry(lambda: _get_source_ddl_teradata(
                        sel_profile, config["SOURCE_DB"], config["SOURCE_TABLE"]))
                elif source_type == "mssql":
                    src_ddl = _with_retry(lambda: _get_source_ddl_mssql(
                        sel_profile, config["SOURCE_DB"], config["SOURCE_TABLE"],
                        source_schema=config.get("SOURCE_SCHEMA") or "dbo"))
                elif source_type == "oracle":
                    src_ddl = _with_retry(lambda: _get_source_ddl_oracle(
                        sel_profile, config["SOURCE_DB"], config["SOURCE_TABLE"]))
                else:
                    src_ddl = _with_retry(lambda: _get_source_ddl_mysql(
                        sel_profile, config["SOURCE_DB"], config["SOURCE_TABLE"]))
            except Exception as e:
                err_msg = str(e)
                if "ODBC" in err_msg or "driver" in err_msg.lower():
                    src_ddl = (f"-- Source DDL unavailable: ODBC driver not installed in this environment.\n"
                               f"-- Source DDL is fetched when running from a machine with {source_type.upper()} drivers.\n"
                               f"-- The Snowflake DDL (right panel) shows the target table structure.")
                else:
                    src_ddl = f"-- Error fetching source DDL (after 3 attempts): {e}"
        st.code(src_ddl, language="sql")

    with col2:
        st.markdown("#### Snowflake DDL")
        with st.spinner("Fetching Snowflake DDL..."):
            sf_ddl, _ = _get_snowflake_ddl(cur, config, source_type, mapping=mapping or None)
        st.code(sf_ddl, language="sql")

    # Type Mapping Table with color coding
    st.markdown("---")
    st.markdown("#### Column Type Mapping")

    if mapping:
        import pandas as pd

        classified = _classify_columns(mapping, source_type)
        n_safe = sum(1 for c in classified if c["Status"] == "safe")
        n_warn = sum(1 for c in classified if c["Status"] == "warning")
        n_issue = sum(1 for c in classified if c["Status"] == "issue")
        total = len(classified)

        # Stats summary bar
        safe_pct = round(n_safe / total * 100) if total else 0
        warn_pct = round(n_warn / total * 100) if total else 0
        issue_pct = round(n_issue / total * 100) if total else 0
        st.markdown(f"""<div style="display:flex;gap:16px;align-items:center;margin-bottom:12px;">
            <span style="color:#34D058;font-weight:600;">{n_safe} safe</span>
            <span style="color:#F0A742;font-weight:600;">{n_warn} warning(s)</span>
            <span style="color:#F85149;font-weight:600;">{n_issue} issue(s)</span>
            <span style="color:#7E96B0;font-size:.8rem;">of {total} columns</span>
        </div>
        <div style="display:flex;height:6px;border-radius:3px;overflow:hidden;background:#263245;">
            <div style="width:{safe_pct}%;background:#34D058;"></div>
            <div style="width:{warn_pct}%;background:#F0A742;"></div>
            <div style="width:{issue_pct}%;background:#F85149;"></div>
        </div>""", unsafe_allow_html=True)

        # Color-coded dataframe
        display_df = pd.DataFrame(classified)
        STATUS_ICONS = {"safe": "🟢", "warning": "🟡", "issue": "🔴"}
        display_df["Status"] = display_df["Status"].map(lambda s: f"{STATUS_ICONS.get(s, '')} {s}")
        show_cols = ["Column", "Source Type", "Snowflake Type", "Status", "Note"]
        st.dataframe(display_df[show_cols], use_container_width=True, hide_index=True,
                     column_config={
                         "Column": st.column_config.TextColumn(width="medium"),
                         "Note": st.column_config.TextColumn(width="large"),
                     })

        # ── Schema Drift Detection ────────────────────────────────────────────
        st.markdown("---")
        st.markdown("#### Schema Drift")
        drifts = _detect_schema_drift(cur, config, source_type, mapping)
        if drifts:
            drift_df = pd.DataFrame(drifts)
            st.warning(f"{len(drifts)} column(s) differ between source and Snowflake target")
            st.dataframe(drift_df, use_container_width=True, hide_index=True)
        else:
            st.success("No schema drift detected — source and target columns are in sync.")

        # ── Validation ────────────────────────────────────────────────────────
        st.markdown("---")
        st.markdown("#### Validate Mapping")

        # Always run rule-based validation automatically
        validation_key = f"_ddl_validation_{config.get('CONFIG_ID', selected)}"
        if validation_key not in st.session_state:
            st.session_state[validation_key] = _rule_based_validate(mapping, source_type)

        st.markdown(st.session_state[validation_key])

        # AI validation button
        ai_on = st.session_state.get("_ai_on", False)
        ai_key = f"_ddl_ai_result_{config.get('CONFIG_ID', selected)}"
        if ai_on:
            from utils.shared import get_setting
            _ai_configured = bool(get_setting(cur, "LLM_API_BASE") and get_setting(cur, "LLM_API_KEY"))
            if _ai_configured:
                if st.button("🤖 AI Validate (uses quota)", key="ddl_ai_validate",
                             type="primary", use_container_width=False):
                    with st.spinner("Calling AI Gateway..."):
                        ai_result = _ai_validate_mapping(cur, mapping, source_type)
                    st.session_state[ai_key] = ai_result
                if st.session_state.get(ai_key):
                    st.markdown(st.session_state[ai_key])
            else:
                st.info("Configure `LLM_API_BASE` + `LLM_API_KEY` in DMT_SETTINGS to enable AI validation.")
        else:
            st.button("🤖 AI Validate", key="ddl_ai_validate_disabled",
                      disabled=True, use_container_width=False)
            st.caption("Enable **AI Assist** toggle in the sidebar to activate.")

        # ── Apply DDL (only if issues found) ──────────────────────────────────
        has_issues = n_issue > 0 or len(drifts) > 0
        if has_issues:
            st.markdown("---")
            st.markdown("#### Apply Corrected DDL")
            st.caption("Issues or drift detected. Review and apply the DDL to create/update the target table.")
            if st.button("🔧 Apply DDL to Snowflake", key="ddl_apply",
                         type="primary", use_container_width=False):
                with st.spinner("Applying DDL..."):
                    try:
                        from ddl_generators import RAW_SCHEMA, AUDIT_COLS, SCD2_COLS, target_db as _tgt_db
                        if source_type == "teradata":
                            from ddl_generators.teradata import _resolve_td_table_name
                            tgt_db = config.get("TARGET_DB") or config["SOURCE_DB"].strip().upper()
                            tgt_table = _resolve_td_table_name(config)
                        else:
                            tgt_db = config.get("TARGET_DB") or _tgt_db(config["SOURCE_DB"])
                            tgt_table = config.get("TARGET_TABLE") or config["SOURCE_TABLE"].upper()
                        tgt_schema = config.get("TARGET_SCHEMA") or RAW_SCHEMA
                        scd_type = int(config.get("SCD_TYPE") or 1)

                        cur.execute(f"CREATE DATABASE IF NOT EXISTS {tgt_db}")
                        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {tgt_db}.{tgt_schema}")

                        col_defs = [f'    "{m["Column"].upper()}" {m["Snowflake Type"]}' for m in mapping]
                        col_defs += [f'    "{n}" {t}' for n, t in AUDIT_COLS]
                        if scd_type == 2:
                            col_defs += [f'    "{n}" {t}' for n, t in SCD2_COLS]
                        ddl = (f"CREATE TABLE IF NOT EXISTS {tgt_db}.{tgt_schema}.{tgt_table} (\n"
                               + ",\n".join(col_defs) + "\n);")
                        cur.execute(ddl)

                        # Add any new columns from drift
                        for d in drifts:
                            if d["Drift"] == "NEW":
                                try:
                                    cur.execute(
                                        f'ALTER TABLE {tgt_db}.{tgt_schema}.{tgt_table} '
                                        f'ADD COLUMN IF NOT EXISTS "{d["Column"].upper()}" {d["Snowflake Type"]}')
                                except Exception:
                                    pass
                        conn.commit()
                        st.success(f"DDL applied: `{tgt_db}.{tgt_schema}.{tgt_table}`")
                    except Exception as e:
                        st.error(f"Failed to apply DDL: {e}")

        # ── Export DDL ────────────────────────────────────────────────────────
        st.markdown("---")
        st.download_button("📥 Download Snowflake DDL", sf_ddl,
                           file_name=f"{config['SOURCE_TABLE'].lower()}_ddl.sql",
                           mime="text/sql", key="ddl_download")

    cur.close()
