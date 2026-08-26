# DDL Viewer — source DDL, converted Snowflake DDL, and AI validation of type mapping.
"""views/ddl.py — DDL Conversion Viewer with AI Validation.

Shows side-by-side comparison of source DDL (MySQL/Teradata) and the converted
Snowflake DDL. Uses Cortex AI to validate data type conversions.
"""
from __future__ import annotations

import os

import streamlit as st

from metadata import config_manager
from metadata import connection_manager


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


def _get_snowflake_ddl(sf_cur, config: dict, source_type: str) -> tuple[str, list[tuple]]:
    """Generate the Snowflake DDL without executing it. Returns (ddl_string, columns)."""
    from ddl_generators import RAW_SCHEMA, AUDIT_COLS, SCD2_COLS

    if source_type == "teradata":
        from ddl_generators.teradata import (
            get_teradata_columns, _resolve_td_schema, _resolve_td_table_name, build_table_ddl)
        # We need a TD connection to get columns — return placeholder if not available
        tgt_db = config.get("TARGET_DB") or config["SOURCE_DB"].strip().upper()
        tgt_schema = config.get("TARGET_SCHEMA") or RAW_SCHEMA
        tgt_table = _resolve_td_table_name(config)
        # Try to read existing table DDL from Snowflake
        try:
            sf_cur.execute(f"SELECT GET_DDL('TABLE', '{tgt_db}.{tgt_schema}.{tgt_table}')")
            row = sf_cur.fetchone()
            if row:
                return row[0], []
        except Exception:
            pass
        return f"-- Table not yet created: {tgt_db}.{tgt_schema}.{tgt_table}\n-- Run pipeline DDL step first.", []
    else:
        from ddl_generators import build_table_ddl, target_db
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
        return f"-- Table not yet created: {tgt_db}.{tgt_schema}.{tgt_table}\n-- Run pipeline DDL step first.", []


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
    else:
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


def _rule_based_validate(mapping: list[dict], source_type: str) -> str:
    """Rule-based type mapping validation (no AI required)."""
    issues = []
    warnings = []

    for m in mapping:
        src = m["Source Type"].upper()
        sf = m["Snowflake Type"].upper()
        col = m["Column"]

        # Precision loss checks
        if "DECIMAL" in src or "NUMERIC" in src or "NUMBER" in src:
            # Check if precision > 38 was truncated
            import re
            prec_match = re.search(r"\((\d+)", src)
            if prec_match and int(prec_match.group(1)) > 38:
                issues.append(f"**{col}**: `{src}` has precision > 38 → mapped to VARCHAR (precision loss)")

        # CLOB/TEXT truncation
        if "CLOB" in src or "LONGTEXT" in src or "MEDIUMTEXT" in src:
            if "16777216" in sf:
                warnings.append(f"**{col}**: `{src}` → VARCHAR(16MB). Source may exceed 16MB limit.")

        # BLOB/BINARY handling
        if "BLOB" in src and "BINARY" in sf:
            warnings.append(f"**{col}**: `{src}` → BINARY. Consider VARCHAR if data is text/JSON (set BLOB_MODE=text).")

        # Timestamp timezone awareness
        if source_type == "mysql":
            if "TIMESTAMP" in src and "TIMESTAMP_TZ" in sf:
                pass  # correct: MySQL TIMESTAMP is UTC-aware
            if "DATETIME" in src and "TIMESTAMP_NTZ" in sf:
                pass  # correct: MySQL DATETIME has no timezone
        if source_type == "teradata":
            if "WITH TIME ZONE" in src and "TIMESTAMP_TZ" not in sf:
                issues.append(f"**{col}**: `{src}` has timezone but mapped to `{sf}` (timezone lost)")

        # PERIOD types
        if "PERIOD" in src and "VARCHAR" in sf:
            warnings.append(f"**{col}**: `{src}` → VARCHAR. Snowflake has no PERIOD type — stored as string.")

        # INTERVAL types
        if "INTERVAL" in src and "VARCHAR" in sf:
            warnings.append(f"**{col}**: `{src}` → VARCHAR. Snowflake has no INTERVAL type — stored as string.")

        # FLOAT precision
        if src in ("FLOAT", "REAL", "DOUBLE PRECISION", "DOUBLE") and sf == "FLOAT":
            pass  # correct mapping

    # Build result
    total = len(mapping)
    issue_count = len(issues)
    warn_count = len(warnings)

    if issue_count == 0 and warn_count == 0:
        result = f"**PASS** — All {total} column mappings are correct. No issues detected."
    elif issue_count == 0:
        result = f"**PASS (with warnings)** — {total} columns checked, {warn_count} warning(s):\n\n"
        result += "\n".join(f"- {w}" for w in warnings)
    else:
        result = f"**ISSUES FOUND** — {issue_count} issue(s), {warn_count} warning(s):\n\n"
        if issues:
            result += "**Issues (may affect data):**\n" + "\n".join(f"- {i}" for i in issues) + "\n\n"
        if warnings:
            result += "**Warnings (informational):**\n" + "\n".join(f"- {w}" for w in warnings)

    result += "\n\n---\n*Validated using rule-based checks (Cortex AI not available on trial accounts)*"
    return result


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

    # Layout: Source DDL | Snowflake DDL
    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Source DDL")
        with st.spinner("Fetching source DDL..."):
            try:
                if source_type == "teradata":
                    src_ddl = _get_source_ddl_teradata(
                        sel_profile, config["SOURCE_DB"], config["SOURCE_TABLE"])
                else:
                    src_ddl = _get_source_ddl_mysql(
                        sel_profile, config["SOURCE_DB"], config["SOURCE_TABLE"])
            except Exception as e:
                src_ddl = f"-- Error fetching source DDL: {e}"
        st.code(src_ddl, language="sql")

    with col2:
        st.markdown("#### Snowflake DDL")
        with st.spinner("Fetching Snowflake DDL..."):
            sf_ddl, _ = _get_snowflake_ddl(cur, config, source_type)
        st.code(sf_ddl, language="sql")

    # Type Mapping Table
    st.markdown("---")
    st.markdown("#### Column Type Mapping")

    with st.spinner("Building type mapping..."):
        try:
            mapping = _get_type_mapping(cur, config, source_type, sel_profile)
        except Exception as e:
            st.error(f"Could not build type mapping: {e}")
            mapping = []

    if mapping:
        import pandas as pd
        df = pd.DataFrame(mapping)
        st.dataframe(df, use_container_width=True, hide_index=True)

        # Validation
        st.markdown("---")
        st.markdown("#### Validate Mapping")

        # AI button enabled only when sidebar AI Assist toggle is ON
        ai_on = st.session_state.get("_ai_on", False)
        if ai_on:
            from utils.shared import get_setting
            _ai_configured = bool(get_setting(cur, "LLM_API_BASE") and get_setting(cur, "LLM_API_KEY"))
            if _ai_configured:
                if st.button("🤖 AI Validate (uses quota)", key="ddl_ai_validate",
                             type="primary", use_container_width=False):
                    with st.spinner("Calling AI Gateway..."):
                        result = _ai_validate_mapping(cur, mapping, source_type)
                    st.markdown(result)
            else:
                st.info("Configure `LLM_API_BASE` + `LLM_API_KEY` in DMT_SETTINGS to enable AI validation.")
        else:
            st.button("🤖 AI Validate", key="ddl_ai_validate_disabled",
                      disabled=True, use_container_width=False)
            st.caption("Enable **AI Assist** toggle in the sidebar to activate.")

    cur.close()
