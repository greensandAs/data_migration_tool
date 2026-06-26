# Migration config editor — schema groups, mini-cards, AI recommend, auto-discover.
"""pages/config.py — Manage per-table migration configuration.

CRUD interface for HISTLOAD_DB.META.MIGRATION_CONFIG. Styled with Tiger Analytics
theme. Supports auto-discovery, schema grouping, bulk actions, per-table editor
with Cortex AI recommendations.
"""
from __future__ import annotations

import json
import os
from collections import OrderedDict

import streamlit as st

import config_manager
import connection_manager

# Brand tokens (duplicated from app.py for standalone usability)
TA_NAVY = "#0F1B2D"
TXT_PRIMARY = "#F0F4F8"
TXT_SECONDARY = "#A8B8CC"
TXT_LABEL = "#7E96B0"
ST_SUCCESS = "#34D058"
ST_FAILED = "#F85149"
ST_SKIPPED = "#F0A742"
ST_PENDING = "#58A6FF"
BORDER = "#263245"


class theme:
    """Lightweight theme helper (replaces ui_theme module)."""
    ST_SUCCESS = ST_SUCCESS
    ST_FAILED = ST_FAILED
    ST_SKIPPED = ST_SKIPPED
    ST_PENDING = ST_PENDING
    TXT_LABEL = TXT_LABEL
    TXT_PRIMARY = TXT_PRIMARY

    @staticmethod
    def section_header(text: str):
        st.markdown(f'<div class="section-header">{text}</div>', unsafe_allow_html=True)

    @staticmethod
    def status_icon(s):
        return {"success": "✅", "failed": "❌", "skipped": "⏭️", None: "⏳"}.get(s, "⏳")

    @staticmethod
    def mini_card(label, value, border_color, value_color):
        return (f'<div class="cfg-mini-card" style="border-left:3px solid {border_color}">'
                f'<div class="mc-val" style="color:{value_color}">{value}</div>'
                f'<div class="mc-lbl">{label}</div></div>')

    @staticmethod
    def mini_cards_row(cards: list[str]):
        st.markdown(f'<div class="cfg-summary">{"".join(cards)}</div>',
                    unsafe_allow_html=True)


def _discover_mysql_schemas(profile: dict) -> list[str]:
    import mysql.connector
    password = profile.get("PASSWORD") or os.getenv(profile.get("AUTH_SECRET") or "", "") or ""
    myconn = mysql.connector.connect(
        host=str(profile["HOST"]), port=int(profile["PORT"]),
        user=str(profile["USERNAME"]), password=str(password))
    cur = myconn.cursor()
    cur.execute("SELECT SCHEMA_NAME FROM information_schema.schemata "
                "WHERE SCHEMA_NAME NOT IN ('information_schema','performance_schema','mysql','sys') "
                "ORDER BY SCHEMA_NAME")
    schemas = [r[0] for r in cur.fetchall()]
    cur.close()
    myconn.close()
    return schemas


def _discover_tables(profile: dict, schema: str) -> list[dict]:
    source_type = (profile.get("SOURCE_TYPE") or "mysql").lower()
    password = profile.get("PASSWORD") or os.getenv(profile.get("AUTH_SECRET") or "", "") or ""

    if source_type == "teradata":
        return _discover_tables_teradata(profile, schema, password)
    return _discover_tables_mysql(profile, schema, password)


def _discover_tables_teradata(profile: dict, schema: str, password: str) -> list[dict]:
    """Auto-discover tables from Teradata via DBC.TablesV / DBC.ColumnsV."""
    import teradatasql
    from ddl_generators.teradata import list_tables, TD_SYSTEM_DATABASES

    td_conn = teradatasql.connect(
        host=str(profile["HOST"]),
        user=str(profile["USERNAME"]),
        password=str(password),
        logmech=profile.get("LOGMECH", "TD2"))
    cur = td_conn.cursor()

    tables = list_tables(td_conn, schema)

    wm_candidates = ("updated_at", "modified_at", "last_modified", "updated_on",
                     "created_at", "created_on", "update_ts", "modify_ts")
    entries = []
    for table in tables:
        # Get columns and primary index info
        cur.execute(
            "SELECT TRIM(ColumnName), ColumnType FROM DBC.ColumnsV "
            "WHERE UPPER(DatabaseName) = UPPER(?) AND UPPER(TableName) = UPPER(?) "
            "ORDER BY ColumnId", (schema, table))
        col_rows = cur.fetchall()
        col_names = [str(r[0]).strip().upper() for r in col_rows]
        col_types = {str(r[0]).strip().lower(): str(r[1]).strip() for r in col_rows}

        # Get primary index columns
        cur.execute(
            "SELECT TRIM(ColumnName) FROM DBC.IndicesV "
            "WHERE UPPER(DatabaseName) = UPPER(?) AND UPPER(TableName) = UPPER(?) "
            "AND IndexType = 'P' ORDER BY ColumnPosition", (schema, table))
        pk_cols = [str(r[0]).strip().upper() for r in cur.fetchall()]

        # Detect watermark column
        wm = None
        for cand in wm_candidates:
            if cand in col_types:
                ct = col_types[cand]
                if ct in ("TS", "SZ", "DA", "AT"):
                    wm = cand.upper()
                    break

        reviews = []
        if not pk_cols:
            reviews.append("no primary index — full load only")
        if len(pk_cols) > 1:
            reviews.append(f"composite PI ({', '.join(pk_cols)})")
        if not wm:
            reviews.append("no watermark column detected")

        entries.append({
            "SOURCE_TABLE": table,
            "PRIMARY_KEY": pk_cols[0] if pk_cols else None,
            "MERGE_KEYS": pk_cols if len(pk_cols) > 1 else None,
            "WATERMARK_COL": wm,
            "LOAD_TYPE": "incremental" if wm else "full",
            "PARTITION_COL": pk_cols[0] if len(pk_cols) == 1 else None,
            "_review": "; ".join(reviews) if reviews else None,
        })

    cur.close()
    td_conn.close()
    return entries


def _discover_tables_mysql(profile: dict, schema: str, password: str) -> list[dict]:
    """Auto-discover tables from MySQL via information_schema."""
    import mysql.connector
    myconn = mysql.connector.connect(
        host=str(profile["HOST"]), port=int(profile["PORT"]),
        user=str(profile["USERNAME"]), password=str(password))
    cur = myconn.cursor(dictionary=True)

    cur.execute(
        "SELECT TABLE_NAME FROM information_schema.tables "
        "WHERE TABLE_SCHEMA=%s AND TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME",
        (schema,))
    tables = [r["TABLE_NAME"] for r in cur.fetchall()]

    wm_candidates = ("updated_at", "modified_at", "last_modified", "updated_on",
                     "created_at", "created_on")
    entries = []
    for table in tables:
        cur.execute(
            "SELECT COLUMN_NAME FROM information_schema.key_column_usage "
            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND CONSTRAINT_NAME='PRIMARY' "
            "ORDER BY ORDINAL_POSITION", (schema, table))
        pk_cols = [r["COLUMN_NAME"] for r in cur.fetchall()]

        cur.execute(
            "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.columns "
            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s", (schema, table))
        col_map = {r["COLUMN_NAME"].lower(): r["DATA_TYPE"].lower() for r in cur.fetchall()}

        wm = None
        for cand in wm_candidates:
            if cand in col_map and col_map[cand] in ("datetime", "timestamp", "date"):
                wm = cand
                break

        reviews = []
        if not pk_cols:
            reviews.append("no primary key — full load only")
        if len(pk_cols) > 1:
            reviews.append(f"composite key ({', '.join(c.upper() for c in pk_cols)})")
        if not wm:
            reviews.append("no watermark column detected")

        entries.append({
            "SOURCE_TABLE": table,
            "PRIMARY_KEY": pk_cols[0].upper() if pk_cols else None,
            "MERGE_KEYS": [c.upper() for c in pk_cols] if len(pk_cols) > 1 else None,
            "WATERMARK_COL": wm.upper() if wm else None,
            "LOAD_TYPE": "incremental" if wm else "full",
            "PARTITION_COL": pk_cols[0].upper() if len(pk_cols) == 1 else None,
            "_review": "; ".join(reviews) if reviews else None,
        })

    cur.close()
    myconn.close()
    return entries


def _ai_recommend(source_db: str, source_table: str, profile: dict):
    """Ask Cortex for config recommendations."""
    import mysql.connector
    password = os.getenv(profile.get("AUTH_SECRET") or "", "")
    myconn = mysql.connector.connect(
        host=profile["HOST"], port=profile["PORT"],
        user=profile["USERNAME"], password=password)
    cur = myconn.cursor()
    cur.execute(
        "SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_KEY, EXTRA "
        "FROM information_schema.columns "
        "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION",
        (source_db, source_table))
    cols = [{"name": r[0], "type": r[1], "key": r[2], "extra": r[3]} for r in cur.fetchall()]
    cur.execute(
        "SELECT COLUMN_NAME FROM information_schema.key_column_usage "
        "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND CONSTRAINT_NAME='PRIMARY' "
        "ORDER BY ORDINAL_POSITION", (source_db, source_table))
    pk = [r[0] for r in cur.fetchall()]
    cur.close()
    myconn.close()

    cols_txt = "\n".join(f"- {c['name']} {c['type']} key={c['key']} extra={c['extra']}" for c in cols)
    prompt = (
        "You are a data migration assistant for a MySQL->Snowflake replication tool.\n"
        f"Source table: {source_db}.{source_table}\n"
        f"Primary key: {pk or 'NONE'}\n"
        f"Columns:\n{cols_txt}\n\n"
        "Recommend load settings. Rules: use 'incremental' only if there is a "
        "reliable cursor — a timestamp column that updates on change (watermark_type "
        "'time') OR a monotonic AUTO_INCREMENT integer PK (watermark_type 'id', "
        "INSERTS ONLY). Otherwise 'full'. merge_keys = full uniqueness grain.\n"
        "Respond with ONLY a JSON object: load_type, watermark_col, watermark_type, "
        "merge_keys, partition_col, rationale (one sentence).")

    from shared import cortex_complete
    raw = cortex_complete(prompt)
    try:
        txt = raw.strip()
        if "```" in txt:
            txt = txt.split("```")[1].lstrip("json").strip()
        start, end = txt.find("{"), txt.rfind("}")
        if start != -1 and end != -1:
            return {"json": json.loads(txt[start:end + 1]), "raw": raw}
    except Exception:
        pass
    return {"json": None, "raw": raw}


def _render_add_table_dialog(cur, conn, profile_name: str, default_schema: str | None):
    """Render the Add Table form as a dialog (modal) like the original app."""

    @st.dialog("➕ Add Table Manually")
    def _dialog():
        st.caption(f"Add a table to `{profile_name}` configuration")
        c1, c2 = st.columns(2)
        source_db = c1.text_input("Source schema (MySQL db)",
                                  value=default_schema or "", key="dlg_add_db")
        source_table = c2.text_input("Source table", key="dlg_add_tbl")
        target_table = st.text_input("Target table (Snowflake)",
                                     help="Defaults to UPPER(source table) if blank",
                                     key="dlg_add_tgt")
        c3, c4 = st.columns(2)
        primary_key = c3.text_input("Primary key", key="dlg_add_pk")
        watermark_col = c4.text_input("Watermark column (optional)", key="dlg_add_wm")
        merge_keys_raw = st.text_input(
            "Merge keys (composite, comma-separated — blank = primary key)",
            key="dlg_add_mk",
            help="Full uniqueness grain for MERGE/dedupe, e.g. EMP_NO, FROM_DATE")
        c5, c6 = st.columns(2)
        load_type = c5.selectbox("Load type", ["full", "incremental"], key="dlg_add_lt")
        wm_type = c6.selectbox("Watermark type", ["auto", "time", "id"], key="dlg_add_wt",
                               help="auto = detect · time = timestamp · id = monotonic PK")
        c7, c8 = st.columns(2)
        reconcile = c7.checkbox("Reconcile deletes", key="dlg_add_rec")
        active = c8.checkbox("Active", value=True, key="dlg_add_act")

        # SCD Type + Filter + Storage
        c9, c10 = st.columns([1, 3])
        scd_labels_map = {0: "0 — Append", 1: "1 — Upsert", 2: "2 — History"}
        scd_type = c9.selectbox("SCD Type", [0, 1, 2], index=1,
                                format_func=lambda x: scd_labels_map[x],
                                key="dlg_add_scd",
                                help="0=Append, 1=Upsert (MERGE), 2=History (versioned)")
        filter_condition = c10.text_input("Filter Condition", key="dlg_add_filter",
                                          placeholder="e.g. region = 'US'",
                                          help="Static WHERE clause applied every run")
        c11, c12 = st.columns(2)
        storage_type = c11.selectbox("Storage", ["internal_stage", "s3", "azure"],
                                     key="dlg_add_storage")
        partition_num = c12.number_input("Partitions", 1, 32, 8, key="dlg_add_parts")

        # External stage picker (shown only for s3/azure)
        storage_path = ""
        if storage_type in ("s3", "azure"):
            try:
                _stg_cur = conn.cursor()
                _stg_cur.execute("SHOW STAGES IN HISTLOAD_DB.META")
                stages_raw = _stg_cur.fetchall()
                stage_cols = [d[0] for d in _stg_cur.description]
                _stg_cur.close()
                stages = [dict(zip(stage_cols, r)) for r in stages_raw]
                filtered = []
                for s in stages:
                    url = (s.get("url") or s.get("URL") or "").lower()
                    name = s.get("name") or s.get("NAME") or ""
                    if storage_type == "s3" and url.startswith("s3://"):
                        filtered.append(name)
                    elif storage_type == "azure" and url.startswith("azure://"):
                        filtered.append(name)
            except Exception:
                filtered = []
            if filtered:
                storage_path = st.selectbox("External Stage", filtered,
                                            key="dlg_add_stage_path")
            else:
                st.warning(f"No {storage_type.upper()} external stages found in HISTLOAD_DB.META.")

        bc1, bc2 = st.columns(2)
        if bc1.button("➕ Add Table", type="primary", use_container_width=True, key="dlg_submit"):
            if not (source_db.strip() and source_table.strip()):
                st.error("Source schema and source table are required.")
            else:
                tgt = (target_table.strip() or source_table.strip()).upper()
                pk_u = primary_key.strip().upper() or None
                wm_u = watermark_col.strip().upper() or None
                mk_u = [c.strip().upper() for c in merge_keys_raw.split(",") if c.strip()] or None
                wm_type_val = None
                if wm_type == "time":
                    wm_type_val = "time"
                elif wm_type == "id":
                    wm_type_val = "id"
                elif wm_u:
                    wm_type_val = "time"

                _save_cur = conn.cursor()
                config_manager.upsert(_save_cur, {
                    "CONNECTION_PROFILE": profile_name,
                    "SOURCE_DB": source_db.strip(),
                    "SOURCE_TABLE": source_table.strip(),
                    "TARGET_DB": source_db.strip().upper(),
                    "TARGET_TABLE": tgt,
                    "LOAD_TYPE": load_type,
                    "PRIMARY_KEY": pk_u,
                    "MERGE_KEYS": mk_u,
                    "WATERMARK_COL": wm_u,
                    "WATERMARK_TYPE": wm_type_val,
                    "PARTITION_COL": pk_u,
                    "PARTITION_NUM": partition_num,
                    "ROWS_PER_FILE": 1000000,
                    "STORAGE_TYPE": storage_type,
                    "STORAGE_PATH": storage_path.strip() or None,
                    "EXECUTION_MODE": "FULL",
                    "RECONCILE": reconcile,
                    "ACTIVE": active,
                    "SCD_TYPE": scd_type,
                    "FILTER_CONDITION": filter_condition.strip() or None,
                })
                st.success(f"Added `{source_db.strip()}.{source_table.strip()}`")
                st.session_state["_show_add_dialog"] = False
                st.rerun()
        if bc2.button("Cancel", use_container_width=True, key="dlg_cancel"):
            st.session_state["_show_add_dialog"] = False
            st.rerun()

    _dialog()


def render(conn):
    """Main render function for the Config page."""
    cur = conn.cursor()

    # Get selected profile from sidebar
    _sidebar_profile = st.session_state.get("selected_profile", "All Connections")
    profile_filter = None if _sidebar_profile == "All Connections" else _sidebar_profile

    # Resolve the profile dict for the selected connection
    profiles = connection_manager.list_profiles(cur)
    if not profiles:
        st.warning("No connection profiles. Create one via **🔌 Manage Connections** in the sidebar.")
        cur.close()
        return

    if profile_filter:
        sel_profile = next((p for p in profiles if p["PROFILE_NAME"] == profile_filter), None)
        if not sel_profile:
            st.error(f"Profile `{profile_filter}` not found.")
            cur.close()
            return
        sel_profile_name = profile_filter
    else:
        # No specific profile selected — show empty state
        from shared import empty_state
        empty_state("🔌", "Select a Source Connection",
                    "Choose a connection from the <b>Source Connection</b> dropdown "
                    "in the sidebar to view and manage its table configuration.")
        cur.close()
        return

    # ── Build configuration (auto-discover) ───────────────────────────────────
    theme.section_header(f"Build Configuration — {sel_profile_name}")

    # Single row: Schema dropdown | Generate Config | Add Table — vertically aligned
    col_schema, col_gen, col_add = st.columns([3, 2, 2])

    sel_schema = None
    if sel_profile["SOURCE_TYPE"] == "mysql":
        try:
            schemas = _discover_mysql_schemas(sel_profile)
            sel_schema = col_schema.selectbox(
                "MySQL Schema", schemas, key="cfg_schema")
        except Exception as e:
            col_schema.error(f"Cannot connect: {e}")
    elif sel_profile["SOURCE_TYPE"] == "teradata":
        try:
            import teradatasql
            from ddl_generators.teradata import list_databases
            td_password = sel_profile.get("PASSWORD") or os.getenv(sel_profile.get("AUTH_SECRET") or "", "") or ""
            td_conn = teradatasql.connect(
                host=str(sel_profile["HOST"]),
                user=str(sel_profile["USERNAME"]),
                password=str(td_password),
                logmech=sel_profile.get("LOGMECH", "TD2"))
            td_dbs = list_databases(td_conn)
            td_conn.close()
            sel_schema = col_schema.selectbox(
                "Teradata Database", td_dbs, key="cfg_td_schema")
        except Exception as e:
            col_schema.error(f"Cannot connect: {e}")
            sel_schema = col_schema.text_input("Teradata Database (manual)", key="cfg_td_schema_manual")
    else:
        sel_schema = col_schema.text_input("Source Schema", key="cfg_other_schema")

    # Add vertical spacer so buttons align with the selectbox
    col_gen.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
    discover_clicked = col_gen.button("⚙️ Generate Config", type="primary",
                                      use_container_width=True)

    col_add.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
    add_clicked = col_add.button("➕ Add Table", use_container_width=True,
                                 key="cfg_add_btn")

    # ── Add Table dialog (modal, like the original app) ───────────────────────
    if add_clicked:
        st.session_state["_show_add_dialog"] = True

    if st.session_state.get("_show_add_dialog"):
        _render_add_table_dialog(cur, conn, sel_profile_name, sel_schema)

    if discover_clicked and sel_schema:
        with st.spinner(f"Scanning `{sel_schema}`..."):
            entries = _discover_tables(sel_profile, sel_schema)
        st.session_state["_discovered"] = entries
        st.session_state["_discovered_schema"] = sel_schema

    if "_discovered" in st.session_state:
        entries = st.session_state["_discovered"]
        schema_name = st.session_state["_discovered_schema"]
        st.success(f"Found {len(entries)} table(s) in `{schema_name}`")

        import pandas as pd
        df = pd.DataFrame(entries)

        # Add selection column
        df.insert(0, "Import", True)
        display_cols = ["Import", "SOURCE_TABLE", "PRIMARY_KEY", "LOAD_TYPE",
                        "WATERMARK_COL", "_review"]
        display_cols = [c for c in display_cols if c in df.columns]

        edited_df = st.data_editor(
            df[display_cols],
            use_container_width=True,
            hide_index=True,
            column_config={
                "Import": st.column_config.CheckboxColumn(
                    "Import", help="Select tables to import", default=True),
                "_review": st.column_config.TextColumn(
                    "Notes", help="Auto-detected issues (review after import)", width="large"),
                "SOURCE_TABLE": st.column_config.TextColumn("Table", width="medium"),
                "LOAD_TYPE": st.column_config.TextColumn("Load", width="small"),
            },
            key="cfg_table_selector",
        )

        # Storage config
        sc1, sc2 = st.columns(2)
        storage_type = sc1.selectbox(
            "Storage Type", ["internal_stage", "local", "s3", "azure"], key="cfg_st")
        storage_path = sc2.text_input(
            "Storage Path (S3/Azure)", placeholder="s3://bucket/prefix/", key="cfg_sp")

        # Count selected
        selected_mask = edited_df["Import"].tolist()
        selected_entries = [e for e, sel in zip(entries, selected_mask) if sel]
        n_selected = len(selected_entries)

        import_clicked = st.button(
            f"📥 Import Selected ({n_selected}/{len(entries)})", type="primary",
            use_container_width=True, disabled=(n_selected == 0))

        if import_clicked and selected_entries:
            imported = 0
            for entry in selected_entries:
                config_manager.upsert(cur, {
                    "CONNECTION_PROFILE": sel_profile_name,
                    "SOURCE_DB": schema_name,
                    "SOURCE_TABLE": entry["SOURCE_TABLE"],
                    "TARGET_DB": schema_name.upper(),
                    "TARGET_TABLE": entry["SOURCE_TABLE"].upper(),
                    "LOAD_TYPE": entry["LOAD_TYPE"],
                    "WATERMARK_COL": entry.get("WATERMARK_COL"),
                    "WATERMARK_TYPE": "time" if entry.get("WATERMARK_COL") else None,
                    "PRIMARY_KEY": entry.get("PRIMARY_KEY"),
                    "MERGE_KEYS": entry.get("MERGE_KEYS"),
                    "PARTITION_COL": entry.get("PARTITION_COL"),
                    "PARTITION_NUM": 8,
                    "ROWS_PER_FILE": 1000000,
                    "STORAGE_TYPE": storage_type,
                    "STORAGE_PATH": storage_path or None,
                    "EXECUTION_MODE": "FULL",
                    "ACTIVE": False,
                    "NOTES": entry.get("_review"),  # Store review notes
                })
                imported += 1
            st.success(f"✅ Imported {imported} table(s).")
            del st.session_state["_discovered"]
            st.rerun()

    # ── Table Configuration ───────────────────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    theme.section_header(f"Table Configuration — {sel_profile_name}" if profile_filter else "Table Configuration — All")
    tables = config_manager.list_all(cur, connection_profile=profile_filter)

    if not tables:
        from shared import empty_state
        empty_state("📋", "No Tables Configured",
                    "Select a MySQL schema above and click <b>⚙️ Generate Config</b> "
                    "to auto-discover tables, or use <b>➕ Add Table</b> to add one manually.")
        cur.close()
        return

    # ── Summary hero cards ────────────────────────────────────────────────────
    n_active = sum(1 for t in tables if t.get("ACTIVE"))
    n_inactive = sum(1 for t in tables if not t.get("ACTIVE"))
    n_review = sum(1 for t in tables if t.get("NOTES"))

    theme.mini_cards_row([
        theme.mini_card("Active", n_active, theme.ST_SUCCESS, theme.ST_SUCCESS),
        theme.mini_card("Inactive", n_inactive, theme.TXT_LABEL, theme.TXT_LABEL),
        theme.mini_card("Needs Review", n_review, "#F0A742", "#F0A742"),
        theme.mini_card("Total", len(tables), theme.ST_PENDING, theme.ST_PENDING),
    ])

    # ── Filter bar ────────────────────────────────────────────────────────────
    fc1, fc2, fc3 = st.columns([3, 2, 2])
    search_q = fc1.text_input("🔍 Search", placeholder="Filter by name…",
                              label_visibility="collapsed", key="cfg_search")
    cfg_schema_f = fc2.selectbox(
        "Schema", ["All schemas"] + sorted(set(t.get("SOURCE_DB", "") for t in tables)),
        label_visibility="collapsed", key="cfg_schema_f")
    cfg_status_f = fc3.selectbox(
        "Status", ["All", "Active", "Inactive", "Failed"],
        label_visibility="collapsed", key="cfg_status_f")

    # ── Apply filters + paginate ──────────────────────────────────────────────
    filtered = []
    for t in tables:
        if search_q:
            haystack = f"{t.get('SOURCE_TABLE','')} {t.get('TARGET_TABLE','')}".lower()
            if search_q.strip().lower() not in haystack:
                continue
        if cfg_schema_f != "All schemas" and t.get("SOURCE_DB") != cfg_schema_f:
            continue
        if cfg_status_f == "Active" and not t.get("ACTIVE"):
            continue
        if cfg_status_f == "Inactive" and t.get("ACTIVE"):
            continue
        if cfg_status_f == "Failed" and (t.get("LAST_RUN_STATUS") or "").lower() != "failed":
            continue
        filtered.append(t)

    PAGE = 10
    total_f = len(filtered)
    npages = max(1, (total_f + PAGE - 1) // PAGE)
    if npages > 1:
        pp1, pp2 = st.columns([1, 4])
        cfg_pg = pp1.number_input("Page", 1, npages, 1, key="cfg_page")
        pp2.caption(f"Showing {(cfg_pg-1)*PAGE+1}–{min(cfg_pg*PAGE, total_f)} of {total_f}")
        filtered = filtered[(cfg_pg - 1) * PAGE: cfg_pg * PAGE]
    else:
        st.caption(f"{total_f} table(s)")

    # ── Group by schema ───────────────────────────────────────────────────────
    schema_groups = OrderedDict()
    for t in filtered:
        s = t.get("SOURCE_DB", "unknown")
        schema_groups.setdefault(s, []).append(t)

    # Track which table is expanded for editing
    if "_editing_table" not in st.session_state:
        st.session_state["_editing_table"] = None

    for schema_name, group in schema_groups.items():
        n_grp_active = sum(1 for t in group if t.get("ACTIVE"))
        # Schema header: name + active count on left, bulk actions on right
        sg1, sg2, sg3 = st.columns([5, 1, 1])
        sg1.markdown(f"**📂 {schema_name}** · {len(group)} table(s) · {n_grp_active} active")
        if sg2.button("✅ Activate All", key=f"ba_act_{schema_name}",
                      use_container_width=True):
            for t in group:
                config_manager.activate(cur, t["CONFIG_ID"])
            st.rerun()
        if sg3.button("⚫ Deactivate All", key=f"ba_deact_{schema_name}",
                      use_container_width=True):
            for t in group:
                config_manager.deactivate(cur, t["CONFIG_ID"])
            st.rerun()

        # Per-table rows (collapsed by default, expand on Edit)
        for tbl in group:
            cfg_id = tbl["CONFIG_ID"]
            is_active = tbl.get("ACTIVE", False)
            status = (tbl.get("LAST_RUN_STATUS") or "pending").lower()
            icon = theme.status_icon(status if status != "pending" else None)
            load_type = tbl.get("LOAD_TYPE", "full")
            is_editing = (st.session_state["_editing_table"] == cfg_id)

            if not is_editing:
                # ── Collapsed row (compact, with inline Edit button) ──────
                active_dot = f'<span style="color:{ST_SUCCESS}">●</span>' if is_active else '<span style="color:#555">●</span>'
                pk_display = tbl.get("PRIMARY_KEY") or "—"
                row_cols = st.columns([0.3, 3, 1.2, 1.5, 1.2, 0.5])
                row_cols[0].markdown(active_dot, unsafe_allow_html=True)
                row_cols[1].markdown(f"**{tbl['SOURCE_TABLE']}**")
                row_cols[2].caption(load_type)
                row_cols[3].caption(pk_display)
                row_cols[4].caption(f"{icon} {status}")
                if row_cols[5].button("✏️", key=f"edit_{cfg_id}"):
                    st.session_state["_editing_table"] = cfg_id
                    st.rerun()
            else:
                # ── Expanded card (full edit form) ────────────────────────
                with st.container(border=True):
                    # Header row with close button
                    h1, h2, h3 = st.columns([4, 2, 1])
                    h1.markdown(f"**{tbl['SOURCE_TABLE']}** → "
                                f"`{tbl.get('TARGET_DB','?')}.RAW.{tbl.get('TARGET_TABLE','?')}`")
                    h2.caption(f"{icon} {status} · {load_type}")
                    if h3.button("✖ Close", key=f"close_{cfg_id}",
                                 use_container_width=True):
                        st.session_state["_editing_table"] = None
                        st.rerun()

                    # Review note (if present) with reviewed toggle
                    review_note = tbl.get("NOTES") or ""
                    if review_note:
                        rn1, rn2 = st.columns([5, 1])
                        rn1.caption(f"⚠️ {review_note}")
                        if rn2.button("✓ Reviewed", key=f"rev_{cfg_id}",
                                      use_container_width=True):
                            # Clear the note = mark as reviewed
                            cur.execute(
                                f"UPDATE HISTLOAD_DB.META.MIGRATION_CONFIG "
                                f"SET NOTES = NULL, UPDATED_AT = CURRENT_TIMESTAMP() "
                                f"WHERE CONFIG_ID = %s", (cfg_id,))
                            st.rerun()

                    # Config fields
                    c1, c2, c3, c4 = st.columns(4)
                    new_load = c1.selectbox(
                        "Load type", ["full", "incremental"],
                        index=["full", "incremental"].index(load_type),
                        key=f"lt_{cfg_id}")
                    new_wm = c2.text_input("Watermark col",
                                           value=tbl.get("WATERMARK_COL") or "",
                                           key=f"wm_{cfg_id}",
                                           placeholder="e.g. UPDATED_AT")
                    new_pk = c3.text_input("Primary key",
                                           value=tbl.get("PRIMARY_KEY") or "",
                                           key=f"pk_{cfg_id}",
                                           placeholder="e.g. EMP_NO")
                    # Merge keys — parse from Snowflake ARRAY to readable string
                    existing_mk = tbl.get("MERGE_KEYS")
                    mk_str = ""
                    if existing_mk:
                        if isinstance(existing_mk, list):
                            mk_str = ", ".join(str(v) for v in existing_mk)
                        elif isinstance(existing_mk, str):
                            try:
                                parsed = json.loads(existing_mk)
                                if isinstance(parsed, list):
                                    mk_str = ", ".join(str(v) for v in parsed)
                                else:
                                    mk_str = str(existing_mk)
                            except (json.JSONDecodeError, TypeError):
                                # Fallback: strip brackets and quotes
                                mk_str = existing_mk.strip("[]").replace('"', '').replace("'", "")
                    new_mk = c4.text_input("Merge keys",
                                           value=mk_str,
                                           key=f"mk_{cfg_id}",
                                           placeholder="e.g. EMP_NO, FROM_DATE")

                    # Second row: Partitions + Storage Type + External Stage
                    c5, c6, c6b = st.columns([1, 2, 3])
                    new_parts = c5.number_input("Partitions", 1, 32,
                                                int(tbl.get("PARTITION_NUM", 8)),
                                                key=f"pn_{cfg_id}")
                    new_storage = c6.selectbox(
                        "Storage", ["internal_stage", "s3", "azure"],
                        index=["internal_stage", "s3", "azure"].index(
                            tbl.get("STORAGE_TYPE", "internal_stage"))
                        if tbl.get("STORAGE_TYPE", "internal_stage") in ["internal_stage", "s3", "azure"]
                        else 0,
                        key=f"st_{cfg_id}")

                    # Show external stage dropdown if s3/azure selected
                    new_storage_path = ""
                    if new_storage in ("s3", "azure"):
                        # Fetch available external stages from Snowflake (filtered by type)
                        cache_key = f"_ext_stages_{new_storage}"
                        if cache_key not in st.session_state:
                            try:
                                cur.execute("SHOW STAGES IN HISTLOAD_DB.META")
                                stages_raw = cur.fetchall()
                                stage_cols = [d[0] for d in cur.description]
                                stages = [dict(zip(stage_cols, r)) for r in stages_raw]
                                # Filter: S3 stages have url starting with s3://
                                # Azure stages have url starting with azure://
                                filtered = []
                                for s in stages:
                                    url = (s.get("url") or "").lower()
                                    if new_storage == "s3" and url.startswith("s3://"):
                                        filtered.append(s["name"])
                                    elif new_storage == "azure" and url.startswith("azure://"):
                                        filtered.append(s["name"])
                                st.session_state[cache_key] = filtered
                            except Exception:
                                st.session_state[cache_key] = []

                        ext_stages = st.session_state.get(cache_key, [])
                        if ext_stages:
                            current_path = tbl.get("STORAGE_PATH") or ""
                            stage_options = ext_stages
                            default_idx = (stage_options.index(current_path)
                                          if current_path in stage_options else 0)
                            new_storage_path = c6b.selectbox(
                                "External Stage",
                                stage_options,
                                index=default_idx,
                                key=f"sp_{cfg_id}")
                        else:
                            c6b.warning(f"No {new_storage.upper()} external stages found.")
                    else:
                        c6b.caption("Using internal stage")

                    # SCD Type + Filter Condition
                    c_scd, c_filter = st.columns([1, 5])
                    scd_options = [0, 1, 2]
                    scd_labels = {0: "0 — Append", 1: "1 — Upsert", 2: "2 — History"}
                    current_scd = int(tbl.get("SCD_TYPE") or 1)
                    new_scd = c_scd.selectbox(
                        "SCD Type", scd_options,
                        index=scd_options.index(current_scd) if current_scd in scd_options else 1,
                        format_func=lambda x: scd_labels[x],
                        key=f"scd_{cfg_id}",
                        help="0=Append (no dedup), 1=Upsert (MERGE), 2=History (versioned rows)")
                    new_filter = c_filter.text_input(
                        "Filter Condition",
                        value=tbl.get("FILTER_CONDITION") or "",
                        key=f"fc_{cfg_id}",
                        placeholder="e.g. region = 'US' AND status = 'active'",
                        help="Static WHERE clause applied on every extraction run")

                    # Third row: Reconcile + Active (aligned as checkboxes)
                    c7, c8, c9 = st.columns([1, 1, 4])
                    new_reconcile = c7.checkbox("Reconcile",
                                               value=tbl.get("RECONCILE", False),
                                               key=f"rec_{cfg_id}")
                    new_active = c8.checkbox("Active", value=is_active,
                                            key=f"act_{cfg_id}")

                    # Action buttons
                    b1, b2, b3 = st.columns([2, 1, 5])
                    if b1.button("💾 Save", key=f"save_{cfg_id}",
                                 type="primary", use_container_width=True):
                        config_manager.upsert(cur, {
                            "CONFIG_ID": cfg_id,
                            "LOAD_TYPE": new_load,
                            "WATERMARK_COL": new_wm.strip().upper() or None,
                            "WATERMARK_TYPE": "time" if new_wm.strip() else None,
                            "PRIMARY_KEY": new_pk.strip().upper() or None,
                            "MERGE_KEYS": [k.strip().upper() for k in new_mk.split(",") if k.strip()] or None,
                            "ACTIVE": new_active,
                            "RECONCILE": new_reconcile,
                            "PARTITION_NUM": int(new_parts),
                            "STORAGE_TYPE": new_storage,
                            "STORAGE_PATH": new_storage_path.strip() or None,
                            "SCD_TYPE": new_scd,
                            "FILTER_CONDITION": new_filter.strip() or None,
                        })
                        st.session_state["_editing_table"] = None
                        st.rerun()
                    if b2.button("🗑️ Delete", key=f"del_{cfg_id}",
                                 use_container_width=True):
                        config_manager.delete_config(cur, cfg_id)
                        st.session_state["_editing_table"] = None
                        st.rerun()

    cur.close()
