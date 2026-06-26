# Dashboard page — pipeline health with Tiger Analytics styled cards and metrics.
# Co-authored with CoCo
"""pages/dashboard.py — Pipeline health at a glance.

Shows styled metric cards, per-table status cards with colored borders,
filtering, search, pagination, and Table/Cards view toggle.
"""
from __future__ import annotations

import streamlit as st
import pandas as pd

import config_manager
import connection_manager


# Import brand tokens from app (available at runtime since app.py imports us)
TA_NAVY = "#0F1B2D"
TXT_PRIMARY = "#F0F4F8"
TXT_SECONDARY = "#A8B8CC"
TXT_LABEL = "#7E96B0"
ST_SUCCESS = "#34D058"
ST_FAILED = "#F85149"
ST_SKIPPED = "#F0A742"
ST_PENDING = "#58A6FF"
BORDER = "#263245"


def _status_class(status: str | None) -> str:
    return {"success": "success", "failed": "failed", "skipped": "skipped"}.get(
        (status or "").lower(), "pending")


def _status_icon(status: str | None) -> str:
    return {"success": "✅", "failed": "❌", "skipped": "⏭️"}.get(
        (status or "").lower(), "⏳")


def _load_pill(load_type: str) -> str:
    cls = "pill-incr" if load_type == "incremental" else "pill-full"
    return f'<span class="pill {cls}">{load_type}</span>'


def _source_pill(source_type: str) -> str:
    return f'<span class="pill pill-source">{source_type or "mysql"}</span>'


def _render_metric_row(tables: list[dict]):
    """Render the summary metric cards."""
    n_total = len(tables)
    n_success = sum(1 for t in tables if (t.get("LAST_RUN_STATUS") or "").lower() == "success")
    n_failed = sum(1 for t in tables if (t.get("LAST_RUN_STATUS") or "").lower() == "failed")
    n_pending = sum(1 for t in tables if not t.get("LAST_RUN_STATUS"))
    n_active = sum(1 for t in tables if t.get("ACTIVE"))

    st.markdown(f"""
    <div class="cfg-summary">
        <div class="cfg-mini-card" style="border-left:3px solid {ST_SUCCESS}">
            <div class="mc-val" style="color:{ST_SUCCESS}">{n_success}</div>
            <div class="mc-lbl">Success</div></div>
        <div class="cfg-mini-card" style="border-left:3px solid {ST_FAILED}">
            <div class="mc-val" style="color:{ST_FAILED}">{n_failed}</div>
            <div class="mc-lbl">Failed</div></div>
        <div class="cfg-mini-card" style="border-left:3px solid {ST_PENDING}">
            <div class="mc-val" style="color:{ST_PENDING}">{n_pending}</div>
            <div class="mc-lbl">Pending</div></div>
        <div class="cfg-mini-card" style="border-left:3px solid {TXT_LABEL}">
            <div class="mc-val" style="color:{TXT_PRIMARY}">{n_active}/{n_total}</div>
            <div class="mc-lbl">Active</div></div>
    </div>
    """, unsafe_allow_html=True)


def _render_card(tbl: dict):
    """Render a single table as a styled card."""
    status = (tbl.get("LAST_RUN_STATUS") or "pending").lower()
    cls = _status_class(status)
    icon = _status_icon(status)
    source = f"{tbl.get('SOURCE_DB', '?')}.{tbl.get('SOURCE_TABLE', '?')}"
    target = f"{tbl.get('TARGET_DB', '?')}.RAW.{tbl.get('TARGET_TABLE', '?')}"
    load_type = tbl.get("LOAD_TYPE", "full")
    storage = tbl.get("STORAGE_TYPE", "internal_stage")
    wm = tbl.get("LAST_LOADED_AT")
    wm_txt = f"🕐 Last sync: {str(wm)[:19]}" if wm else "🕐 Never run"
    failed_step = tbl.get("LAST_FAILED_STEP")
    failed_badge = f' · ⚠️ failed at: <b>{failed_step}</b>' if failed_step else ""
    active_badge = "" if tbl.get("ACTIVE") else '<span class="pill" style="background:#1a1a1a;color:#666;border:1px solid #333">INACTIVE</span>'

    st.markdown(f"""
        <div class="table-card {cls}">
          <span class="tstatus {cls}">{icon} {status.upper()}</span>
          <div class="tname">{source}</div>
          <div class="tmeta">→ {target}</div>
          <div class="tmeta" style="margin-top:6px">
            {_load_pill(load_type)} {active_badge}
            <span style="color:{TXT_LABEL}">Storage: {storage}</span></div>
          <div class="tmeta" style="margin-top:4px;color:{TXT_LABEL}">
            {wm_txt} &nbsp;·&nbsp; PK: {tbl.get("PRIMARY_KEY") or "—"}
            &nbsp;·&nbsp; Parts: {tbl.get("PARTITION_NUM", 8)}{failed_badge}</div>
        </div>
    """, unsafe_allow_html=True)


def render(conn):
    """Main render function for the Dashboard page."""
    st.markdown('<div class="section-header">Dashboard — Run Summary</div>',
                unsafe_allow_html=True)

    cur = conn.cursor()

    # Filter by sidebar-selected connection profile
    _profile = st.session_state.get("selected_profile", "All Connections")
    profile_filter = None if _profile == "All Connections" else _profile
    tables = config_manager.list_all(cur, connection_profile=profile_filter)

    if not tables:
        from shared import empty_state
        empty_state("📊", "No Tables Configured",
                    "Go to <b>⚙️ Config</b> to add tables, or select a connection in the sidebar.")
        cur.close()
        return

    _render_metric_row(tables)

    # -- Filters --
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="section-header">Per-Table Status</div>',
                unsafe_allow_html=True)

    d1, d2, d3, d4 = st.columns([3, 2, 2, 1])
    dq = d1.text_input("🔍 Search", placeholder="Filter by name…",
                       label_visibility="collapsed", key="dash_search")
    dstatus = d2.selectbox("Status", ["All", "success", "failed", "pending", "skipped"],
                           label_visibility="collapsed", key="dash_status")
    dload = d3.selectbox("Load Type", ["All", "full", "incremental"],
                         label_visibility="collapsed", key="dash_load")
    dview = d4.selectbox("View", ["Cards", "Table"],
                         index=1 if len(tables) > 50 else 0,
                         label_visibility="collapsed", key="dash_view")

    # Apply filters
    filtered = tables
    if dstatus != "All":
        if dstatus == "pending":
            filtered = [t for t in filtered if not t.get("LAST_RUN_STATUS")]
        else:
            filtered = [t for t in filtered if (t.get("LAST_RUN_STATUS") or "").lower() == dstatus]
    if dload != "All":
        filtered = [t for t in filtered if (t.get("LOAD_TYPE") or "").lower() == dload]
    if dq:
        s = dq.strip().lower()
        filtered = [t for t in filtered
                    if s in (t.get("SOURCE_TABLE") or "").lower()
                    or s in (t.get("TARGET_TABLE") or "").lower()
                    or s in (t.get("SOURCE_DB") or "").lower()]

    st.caption(f"Showing {len(filtered)} of {len(tables)} tables")

    if not filtered:
        from shared import empty_state
        empty_state("🔍", "No Matches", "No tables match the current filters.")
    elif dview == "Table":
        df = pd.DataFrame([{
            "Source": f"{t.get('SOURCE_DB')}.{t.get('SOURCE_TABLE')}",
            "Target": f"{t.get('TARGET_DB')}.RAW.{t.get('TARGET_TABLE')}",
            "Load": t.get("LOAD_TYPE", "full"),
            "Status": t.get("LAST_RUN_STATUS") or "pending",
            "Last Sync": str(t.get("LAST_LOADED_AT"))[:19] if t.get("LAST_LOADED_AT") else "Never",
            "Storage": t.get("STORAGE_TYPE", "internal_stage"),
            "Active": "✅" if t.get("ACTIVE") else "—",
        } for t in filtered])
        st.dataframe(df, use_container_width=True, hide_index=True, height=480)
    else:
        # Cards view with pagination
        PAGE = 20
        npages = max(1, (len(filtered) + PAGE - 1) // PAGE)
        pg = st.number_input("Page", 1, npages, 1, key="dash_page") if npages > 1 else 1
        page_items = filtered[(pg - 1) * PAGE: pg * PAGE]

        cols = st.columns(2)
        for i, tbl in enumerate(page_items):
            with cols[i % 2]:
                _render_card(tbl)

        if npages > 1:
            st.caption(f"Page {pg} of {npages}")

    cur.close()
