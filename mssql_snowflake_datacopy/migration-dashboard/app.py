"""app.py — MSSQL → Snowflake Migration Console (UI Layer).
All business logic lives in core/ modules. This file is Streamlit UI only.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Ensure azcopy is discoverable regardless of how Streamlit is launched
_azcopy_dir = Path.home() / "Tools" / "azcopy"
if _azcopy_dir.exists() and str(_azcopy_dir) not in os.environ.get("PATH", ""):
    os.environ["PATH"] = os.environ.get("PATH", "") + os.pathsep + str(_azcopy_dir)

# Ensure core/ is importable
sys.path.insert(0, str(Path(__file__).parent))
from core.config import load_config, save_config, pull_from_snowflake
from core.connections import sf_query, get_mssql_conn
from core.logger import get_next_batch_id
from core.pipeline import run_single_table

# ─── Brand Tokens ────────────────────────────────────────────────────────────
TA_ORANGE = "#F15A22"
TA_NAVY = "#1A1A2E"
ST_SUCCESS = "#4CAF50"
ST_FAILED = "#E53935"
LOGO_DARK = str(Path(__file__).parent / "assets" / "logos" / "ta_logo_dark.svg")
LOGO_LIGHT = str(Path(__file__).parent / "assets" / "logos" / "ta_logo_light.svg")

# ─── Page Config ─────────────────────────────────────────────────────────────
st.set_page_config(page_title="Tiger Analytics | MSSQL → Snowflake", layout="wide")

# ─── CSS (Tiger Analytics Branding) ──────────────────────────────────────────
st.markdown(f"""
<style>
    /* ═══ SIDEBAR ═══ */
    section[data-testid="stSidebar"] {{
        background-color: {TA_NAVY};
    }}
    section[data-testid="stSidebar"] > div:first-child {{
        border-top: 4px solid {TA_ORANGE};
    }}
    section[data-testid="stSidebar"] .stMarkdown,
    section[data-testid="stSidebar"] .stMarkdown p,
    section[data-testid="stSidebar"] .stCaption {{
        color: #FFFFFF !important;
    }}

    /* ═══ TABS ═══ */
    .stTabs [data-baseweb="tab-list"] {{gap:0;}}
    .stTabs [data-baseweb="tab"] {{
        padding:10px 28px; font-weight:600;
        border-bottom:3px solid transparent;
    }}
    .stTabs [aria-selected="true"] {{
        border-bottom-color: {TA_ORANGE} !important;
        color: {TA_ORANGE} !important;
    }}

    /* ═══ METRIC CARDS ═══ */
    div[data-testid="stMetric"] {{
        background: #F8F9FA;
        border-left: 4px solid {TA_ORANGE};
        border-radius: 8px;
        padding: 12px 16px;
    }}

    /* ═══ BUTTONS ═══ */
    .stButton > button[kind="primary"] {{
        background-color: {TA_ORANGE};
        color: #FFFFFF;
        border: none;
    }}
    .stButton > button[kind="primary"]:hover {{
        background-color: #D94E1C;
    }}

    /* ═══ SPACING ═══ */
    div.block-container {{ padding-top: 1.5rem; }}
</style>
""", unsafe_allow_html=True)

# ─── Sidebar ─────────────────────────────────────────────────────────────────
with st.sidebar:
    if Path(LOGO_DARK).exists():
        st.image(LOGO_DARK, width=140)
    st.markdown("---")
    st.markdown("**MSSQL → Snowflake**\n\nData Migration Console")
    st.markdown("---")
    st.caption("Pipeline: BCP → GZip → Blob → COPY → MERGE")

# ─── Header ──────────────────────────────────────────────────────────────────
st.markdown(
    f'<div style="background:linear-gradient(135deg,{TA_NAVY},#16213E);padding:16px 24px;border-radius:10px;margin-bottom:16px;">'
    f'<h2 style="margin:0;color:#FFF;">MSSQL → Snowflake Migration Console</h2>'
    f'<p style="margin:4px 0 0;color:#94A3B8;font-size:0.85rem;">BCP Export → GZip → Azure Blob → COPY INTO (WRK) → MERGE (SCD 0/1/2)</p>'
    f'</div>', unsafe_allow_html=True)
# ═══════════════════════════════════════════════════════════════════════════════
# TABS
# ═══════════════════════════════════════════════════════════════════════════════
tab_config, tab_run, tab_results = st.tabs(["⚙️ Configuration", "🚀 Run", "📊 Results"])


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════
with tab_config:
    cfg = load_config()
    tables = cfg.get("tables", [])

    # KPIs
    total_t = len(tables)
    active_t = sum(1 for t in tables if t.get("active"))
    incr_t = sum(1 for t in tables if t.get("load_type") == "incremental")
    kc1, kc2, kc3, kc4 = st.columns(4)
    kc1.metric("Total Tables", total_t)
    kc2.metric("Active", active_t)
    kc3.metric("Inactive", total_t - active_t)

    # Overview table
    if tables:
        st.dataframe(pd.DataFrame([{
            "Source": f"{t.get('source_db')}.{t.get('source_schema','dbo')}.{t.get('source_table')}",
            "Target": f"{t.get('target_db','')}.{t.get('target_schema','')}.{t.get('target_table')}",
            "Load": (t.get("load_type") or "full").upper(),
            "SCD": t.get("scd_type", 0),
            "CDC Columns": t.get("cdc_columns") or "—",
            "CDC Type": (t.get("cdc_type") or "—"),
            "PK": t.get("primary_key") or "—",
            "Last Loaded": t.get("last_loaded_at") or "—",
            "Active": "✅" if t.get("active") else "❌",
        } for t in tables]), hide_index=True, use_container_width=True)

    st.markdown("---")

    # ── Discover ──────────────────────────────────────────────────────────────
    with st.expander("🔍 Discover Tables from MSSQL", expanded=not bool(tables)):
        dc1, dc2 = st.columns(2)
        with dc1:
            disc_db = st.text_input("Database", placeholder="TestDB", key="d_db")
        with dc2:
            disc_sch = st.text_input("Schema", value="dbo", key="d_sch")

        if st.button("Scan", key="d_scan"):
            if disc_db:
                try:
                    mcon = get_mssql_conn(disc_db)
                    cur = mcon.cursor()
                    cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_TYPE='BASE TABLE'", (disc_sch,))
                    found = [r[0] for r in cur.fetchall()]
                    discovered = []
                    for t in found:
                        cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND CONSTRAINT_NAME LIKE 'PK%'", (disc_sch, t))
                        pks = [r[0] for r in cur.fetchall()]
                        cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND (COLUMN_NAME LIKE '%modified%' OR COLUMN_NAME LIKE '%updated%')", (disc_sch, t))
                        wms = [r[0] for r in cur.fetchall()]
                        pk = pks[0].upper() if pks else None
                        wm = wms[0].upper() if wms else None
                        discovered.append({"table": t, "pk": pk, "wm": wm, "load": "incremental" if (pk and wm) else "full"})
                    cur.close(); mcon.close()
                    st.session_state["disc"] = discovered
                    st.session_state["disc_db"] = disc_db
                    st.session_state["disc_sch"] = disc_sch
                except Exception as e:
                    st.error(f"Scan failed: {e}")

        if "disc" in st.session_state and st.session_state["disc"]:
            disc = st.session_state["disc"]
            st.success(f"Found {len(disc)} table(s)")

            existing = {(t.get("source_db"), t.get("source_table")) for t in tables}
            new_tables = [d for d in disc if (st.session_state["disc_db"], d["table"]) not in existing]
            already_added = [d for d in disc if (st.session_state["disc_db"], d["table"]) in existing]

            if already_added:
                st.caption(f"{len(already_added)} table(s) already in config (skipped)")

            if new_tables:
                st.markdown("**Select tables to onboard:**")
                # Create selection checkboxes
                select_all = st.checkbox("Select All", value=False, key="d_select_all")
                selected = []
                for i, d in enumerate(new_tables):
                    load_hint = f"🔄 incremental ({d['pk']}, {d['wm']})" if d["load"] == "incremental" else "📦 full"
                    checked = st.checkbox(
                        f"**{d['table']}** — {load_hint}",
                        value=select_all,
                        key=f"d_chk_{i}",
                    )
                    if checked:
                        selected.append(d)

                if selected and st.button(f"➕ Add {len(selected)} selected table(s)", key="d_add"):
                    for d in selected:
                        tables.append({
                            "source_db": st.session_state["disc_db"],
                            "source_schema": st.session_state["disc_sch"],
                            "source_table": d["table"],
                            "target_db": "ANALYTICS", "target_schema": "PUBLIC",
                            "target_table": d["table"].upper(),
                            "primary_key": d["pk"], "load_type": d["load"],
                            "cdc_columns": d["wm"], "cdc_type": "TIMESTAMP",
                            "scd_type": 0, "execution_mode": "FULL", "delimiter": "|",
                            "filter_condition": None, "trim": "N", "encryption_columns": None,
                            "custom_sql": None, "cloud_path": os.getenv("CLOUD_PATH", ""),
                            "warehouse_name": os.getenv("SF_WAREHOUSE", "COMPUTE_WH"),
                            "active": True, "last_run_status": None,
                        })
                    cfg["tables"] = tables
                    save_config(cfg)
                    del st.session_state["disc"]
                    st.rerun()
            else:
                st.info("All discovered tables are already in the config.")

    # ── Add Manually (Full Form) ──────────────────────────────────────────────
    with st.expander("➕ Add Table Manually"):
        with st.form("add_form"):
            st.markdown("**Source (MSSQL)**")
            ac1, ac2, ac3 = st.columns(3)
            with ac1: a_db = st.text_input("Database *", key="a_db")
            with ac2: a_sch = st.text_input("Schema", value="dbo", key="a_sch")
            with ac3: a_tbl = st.text_input("Table *", key="a_tbl")

            st.markdown("**Target & Settings**")
            tc1, tc2, tc3 = st.columns(3)
            with tc1: a_tgt_db = st.text_input("Target DB", value="ANALYTICS", key="a_tgt_db")
            with tc2: a_pk = st.text_input("Primary Key *", key="a_pk")
            with tc3: a_load = st.selectbox("Load Type", ["full", "incremental", "filter"], key="a_load")

            tc4, tc5, tc6 = st.columns(3)
            with tc4: a_scd = st.selectbox("SCD Type", [0, 1, 2], key="a_scd")
            with tc5: a_cdc = st.text_input("CDC Column(s)", key="a_cdc")
            with tc6: a_cdc_type = st.selectbox("CDC Type", ["TIMESTAMP", "ID"], key="a_cdc_type")

            a_filter = st.text_input("Filter Condition", key="a_filter")
            a_sql = st.text_area("Custom SQL", key="a_sql", height=68)

            if st.form_submit_button("Add", type="primary"):
                if a_db and a_tbl and a_pk:
                    tables.append({
                        "source_db": a_db, "source_schema": a_sch, "source_table": a_tbl,
                        "target_db": a_tgt_db, "target_schema": "PUBLIC", "target_table": a_tbl.upper(),
                        "primary_key": a_pk.upper(), "load_type": a_load,
                        "cdc_columns": a_cdc.upper() if a_cdc else None, "cdc_type": a_cdc_type,
                        "scd_type": a_scd, "execution_mode": "FULL", "delimiter": "|",
                        "filter_condition": a_filter or None, "trim": "N",
                        "encryption_columns": None, "custom_sql": a_sql or None,
                        "cloud_path": os.getenv("CLOUD_PATH", ""),
                        "warehouse_name": os.getenv("SF_WAREHOUSE", "COMPUTE_WH"),
                        "active": True, "last_run_status": None,
                    })
                    cfg["tables"] = tables; save_config(cfg); st.rerun()
                else:
                    st.error("Database, Table, and Primary Key are required.")
    # ── Manage (Edit/Enable/Disable/Delete) ───────────────────────────────────
    if tables:
        with st.expander("🔄 Manage Tables"):
            labels = [f"{'🟢' if t.get('active') else '🔴'} {t['source_db']}.{t['source_table']}" for t in tables]
            idx = st.selectbox("Table", range(len(labels)), format_func=lambda i: labels[i], key="m_sel")

            mc1, mc2, mc3 = st.columns(3)
            with mc1:
                if st.button("Enable", key="m_en"):
                    tables[idx]["active"] = True; cfg["tables"] = tables; save_config(cfg); st.rerun()
            with mc2:
                if st.button("Disable", key="m_dis"):
                    tables[idx]["active"] = False; cfg["tables"] = tables; save_config(cfg); st.rerun()
            with mc3:
                if st.button("Delete", key="m_del"):
                    tables.pop(idx); cfg["tables"] = tables; save_config(cfg); st.rerun()

            # Inline edit
            st.markdown("---")
            t = tables[idx]
            with st.form("edit_form"):
                e1, e2, e3 = st.columns(3)
                with e1: e_load = st.selectbox("Load Type", ["full", "incremental", "filter"], index=["full", "incremental", "filter"].index(t.get("load_type", "full")), key="e_load")
                with e2: e_scd = st.selectbox("SCD Type", [0, 1, 2], index=t.get("scd_type", 0), key="e_scd")
                with e3: e_cdc_type = st.selectbox("CDC Type", ["TIMESTAMP", "ID"], index=["TIMESTAMP", "ID"].index((t.get("cdc_type") or "TIMESTAMP").upper()), key="e_cdc_type")
                e4, e5 = st.columns(2)
                with e4: e_pk = st.text_input("Primary Key", value=t.get("primary_key", "") or "", key="e_pk")
                with e5: e_cdc = st.text_input("CDC Columns", value=t.get("cdc_columns", "") or "", key="e_cdc")

                e6, e7 = st.columns(2)
                with e6: e_filter = st.text_input("Filter", value=t.get("filter_condition", "") or "", key="e_filter")
                with e7: e_delim = st.text_input("Delimiter", value=t.get("delimiter", "|"), key="e_delim")

                e_sql = st.text_area("Custom SQL", value=t.get("custom_sql", "") or "", key="e_sql", height=68)

                if st.form_submit_button("💾 Save", type="primary"):
                    tables[idx].update({
                        "load_type": e_load, "scd_type": e_scd, "cdc_type": e_cdc_type,
                        "primary_key": e_pk.upper() if e_pk else None,
                        "cdc_columns": e_cdc.upper() if e_cdc else None,
                        "filter_condition": e_filter or None, "delimiter": e_delim or "|",
                        "custom_sql": e_sql or None,
                    })
                    cfg["tables"] = tables; save_config(cfg); st.success("Saved."); st.rerun()



# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — RUN
# ═══════════════════════════════════════════════════════════════════════════════
with tab_run:
    cfg = load_config()
    tables = cfg.get("tables", [])
    active = [t for t in tables if t.get("active")]

    if not active:
        st.warning("No active tables. Configure tables in the Configuration tab first.")
    else:
        st.markdown(f"**{len(active)} active table(s)** ready.")
        table_labels = [f"{t['source_db']}.{t.get('source_schema','dbo')}.{t['source_table']}" for t in active]
        selected = st.multiselect("Tables", range(len(table_labels)), default=list(range(len(table_labels))), format_func=lambda i: table_labels[i])
        r1, r2 = st.columns(2)
        with r1:
            mode = st.radio("Mode", ["FULL", "EXPORT", "LOAD"], horizontal=True)
        with r2:
            desc = {"FULL": "BCP → GZip → Upload → COPY INTO WRK → MERGE", "EXPORT": "BCP → GZip → Upload only", "LOAD": "COPY INTO WRK → MERGE only"}
            st.info(desc[mode])
        if st.button("🚀 Start Migration", type="primary", disabled=not selected):
            from concurrent.futures import ThreadPoolExecutor, as_completed
            import threading

            batch_id = get_next_batch_id()
            run_tables = [(active[i], i + 1) for i in selected]
            total_count = len(run_tables)
            max_workers = min(total_count, 12)

            st.markdown(f"#### Batch `{batch_id}` — {mode} mode — {total_count} table(s) — {max_workers} parallel workers")

            # Progress tracking
            progress_bar = st.progress(0)
            progress_text = st.empty()
            completed_count = {"n": 0}
            lock = threading.Lock()

            results = []

            def _run_and_track(tbl, job_id):
                result = run_single_table(tbl, mode, batch_id, job_id)
                with lock:
                    completed_count["n"] += 1
                    n = completed_count["n"]
                return result

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(_run_and_track, tbl, job_id): tbl
                    for tbl, job_id in run_tables
                }
                for future in as_completed(futures):
                    tbl = futures[future]
                    try:
                        results.append(future.result())
                    except Exception as e:
                        results.append({
                            "table": tbl["source_table"], "status": "FAILED",
                            "duration": 0, "row_count": 0, "sf_count": 0,
                            "logs": [f"Pipeline exception: {e}"],
                        })
                    # Update progress
                    done = len(results)
                    progress_bar.progress(done / total_count)
                    s = sum(1 for r in results if r["status"] == "SUCCESS")
                    f = sum(1 for r in results if r["status"] == "FAILED")
                    progress_text.text(f"Progress: {done}/{total_count} — ✅ {s} success · ❌ {f} failed")

            progress_bar.progress(1.0)
            progress_text.empty()

            # Display results with logs after all complete
            for result in sorted(results, key=lambda r: r["table"]):
                with st.container(border=True):
                    if result["status"] == "SUCCESS":
                        # Build delta summary
                        delta_parts = []
                        if result.get("rows_extracted"):
                            delta_parts.append(f"{result['rows_extracted']:,} extracted")
                        if result.get("rows_inserted"):
                            delta_parts.append(f"{result['rows_inserted']:,} inserted")
                        if result.get("rows_updated"):
                            delta_parts.append(f"{result['rows_updated']:,} updated")
                        if result.get("rows_expired"):
                            delta_parts.append(f"{result['rows_expired']:,} expired")
                        delta_str = " · ".join(delta_parts) if delta_parts else "no changes"

                        st.markdown(
                            f"✅ **{result['table']}** — {result['duration']}s · "
                            f"{delta_str} · "
                            f"MSSQL: {result['row_count']:,} / SF: {result['sf_count']:,}"
                        )
                    else:
                        st.markdown(f"❌ **{result['table']}** — failed")

                    with st.expander("Log", expanded=(result["status"] != "SUCCESS")):
                        if result["logs"]:
                            for line in result["logs"]:
                                st.text(line)
                        else:
                            st.text("No log output captured.")

            # Save config (last_run_status updated)
            save_config(cfg)

            # Summary
            st.markdown("---")
            s_count = sum(1 for r in results if r["status"] == "SUCCESS")
            f_count = sum(1 for r in results if r["status"] == "FAILED")
            sc1, sc2, sc3, sc4 = st.columns(4)
            sc1.metric("Total", len(results))
            sc2.metric("Success", s_count)
            sc3.metric("Failed", f_count)
            sc4.metric("Wall Time", f"{max(r['duration'] for r in results)}s" if results else "—")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — RESULTS
# ═══════════════════════════════════════════════════════════════════════════════
with tab_results:
    st.markdown("### Migration History")

    try:
        log_df = sf_query("SELECT * FROM DATA_MIGRATION.CONTROL.LOG_TABLE ORDER BY BATCH_ID DESC, JOB_ID")
    except Exception as e:
        log_df = pd.DataFrame()
        st.warning(f"Could not load LOG_TABLE: {e}")

    if log_df.empty:
        st.info("No runs yet. Use the **Run** tab to start a migration.")
    else:
        total = len(log_df)
        success = len(log_df[log_df["FINAL_STATUS"] == "SUCCESS"])
        failed = len(log_df[log_df["FINAL_STATUS"] == "FAILED"])
        avg_dur = log_df[log_df["JOB_DURATION"].notna()]["JOB_DURATION"].mean()

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Jobs", total)
        c2.metric("Successful", success)
        c3.metric("Failed", failed)
        c4.metric("Avg Duration", f"{avg_dur:.0f}s" if pd.notna(avg_dur) else "—")
        st.markdown("<br>", unsafe_allow_html=True)
        display_cols = ["BATCH_ID", "JOB_ID", "MSSQL_TABLE_NAME", "SF_TABLE_NAME",
                        "EXECUTION_MODE", "LOAD_TYPE", "FINAL_STATUS",
                        "MSSQL_TABLE_COUNT", "SF_TABLE_COUNT",
                        "ROWS_EXTRACTED", "ROWS_INSERTED", "ROWS_UPDATED", "ROWS_EXPIRED",
                        "JOB_DURATION"]
        available = [c for c in display_cols if c in log_df.columns]
        st.dataframe(log_df[available], hide_index=True, use_container_width=True)
# ─── Footer ──────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown(f'<p style="text-align:center;color:#9CA3AF;font-size:0.8rem;">Powered by <span style="color:{TA_ORANGE};font-weight:600;">Tiger Analytics</span> · Built on Snowflake</p>', unsafe_allow_html=True)