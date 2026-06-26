# Run history page — styled audit log with batch grouping and AI failure explainer.
"""pages/history.py — View pipeline execution history.

Tiger Analytics styled with batch grouping, trend chart, summary hero cards,
and AI failure explanation via Cortex.
"""
from __future__ import annotations

import streamlit as st
import pandas as pd

# Brand tokens
ST_SUCCESS = "#34D058"
ST_FAILED = "#F85149"
ST_SKIPPED = "#F0A742"
ST_PENDING = "#58A6FF"
TXT_PRIMARY = "#F0F4F8"
TXT_LABEL = "#7E96B0"


def _parse_hours(window: str) -> int:
    """Convert time window label to hours."""
    mapping = {"7d": 168, "4d": 96, "3d": 72, "2d": 48, "24hrs": 24, "8hrs": 8, "2hrs": 2}
    return mapping.get(window, 168)


def _fetch_history(cur, time_window: str = "7d", connection_profile: str | None = None) -> pd.DataFrame:
    """Fetch from RUN_LOG filtered by time window and optionally by connection profile."""
    try:
        hours = _parse_hours(time_window)
        conditions = [f"INSERTED_AT >= DATEADD('hour', -{hours}, CURRENT_TIMESTAMP())"]
        params: list = []
        if connection_profile:
            conditions.append("CONNECTION_PROFILE = %s")
            params.append(connection_profile)
        where = "WHERE " + " AND ".join(conditions)
        cur.execute(f"""
            SELECT INSERTED_AT, BATCH_ID, CONNECTION_PROFILE, SOURCE_DB, SOURCE_TABLE,
                   TARGET_DB, TARGET_TABLE, LOAD_TYPE, ENGINE, STATUS, FAILED_STEP,
                   DURATION_SEC, ROWS_EXTRACTED, ROWS_LOADED,
                   WATERMARK_FROM, WATERMARK_TO, WATERMARK_TYPE, ERROR_MESSAGE
            FROM HISTLOAD_DB.META.RUN_LOG
            {where}
            ORDER BY INSERTED_AT DESC
        """, params or None)
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    except Exception as e:
        st.error(f"Could not read RUN_LOG: {e}")
        return pd.DataFrame()


def render(conn):
    """Main render function for the History page."""
    st.markdown('<div class="section-header">Run History — HISTLOAD_DB.META.RUN_LOG</div>',
                unsafe_allow_html=True)

    cur = conn.cursor()

    # Scope to selected connection profile (consistent with Config/Run pages)
    _profile = st.session_state.get("selected_profile", "All Connections")
    profile_filter = None if _profile == "All Connections" else _profile

    if not profile_filter:
        from shared import empty_state
        empty_state("🔌", "Select a Source Connection",
                    "Choose a connection from the <b>Source Connection</b> dropdown "
                    "in the sidebar to view its run history.")
        cur.close()
        return

    # Controls — Refresh + Time slicer
    TIME_OPTIONS = ["7d", "4d", "3d", "2d", "24hrs", "8hrs", "2hrs"]
    c1, c2 = st.columns([1, 5])
    if c1.button("🔄 Refresh", use_container_width=True, key="hist_refresh"):
        for k in [k for k in st.session_state if k.startswith("_hist_")]:
            del st.session_state[k]

    # Segmented time slicer (radio buttons styled inline)
    selected_window = c2.radio(
        "Time window", TIME_OPTIONS, index=0, horizontal=True,
        label_visibility="collapsed", key="hist_time_window"
    )

    # Fetch (cached in session); invalidate if profile or window changed
    cache_key = f"_hist_{_profile}_{selected_window}"
    if cache_key not in st.session_state:
        # Clear stale caches from other profiles/windows
        for k in [k for k in st.session_state if k.startswith("_hist_") and k != "hist_time_window"]:
            del st.session_state[k]
        st.session_state[cache_key] = _fetch_history(cur, selected_window, connection_profile=profile_filter)

    hist = st.session_state[cache_key]
    if hist.empty:
        from shared import empty_state
        empty_state("📜", "No Run History",
                    "Run a pipeline from the <b>▶️ Run</b> page to see execution history here.")
        cur.close()
        return

    # Summary metrics
    total_runs = len(hist)
    n_success = len(hist[hist["STATUS"] == "success"])
    n_failed = len(hist[hist["STATUS"] == "failed"])
    success_pct = round(n_success / total_runs * 100, 1) if total_runs else 0
    avg_dur = round(hist["DURATION_SEC"].dropna().mean(), 1) if not hist["DURATION_SEC"].isna().all() else 0

    st.markdown(f"""<div class="cfg-summary">
        <div class="cfg-mini-card" style="border-left:3px solid {ST_PENDING}">
            <div class="mc-val" style="color:{ST_PENDING}">{total_runs}</div>
            <div class="mc-lbl">Total Runs</div></div>
        <div class="cfg-mini-card" style="border-left:3px solid {ST_SUCCESS}">
            <div class="mc-val" style="color:{ST_SUCCESS}">{success_pct}%</div>
            <div class="mc-lbl">Success Rate</div></div>
        <div class="cfg-mini-card" style="border-left:3px solid {ST_FAILED}">
            <div class="mc-val" style="color:{ST_FAILED}">{n_failed}</div>
            <div class="mc-lbl">Failed</div></div>
        <div class="cfg-mini-card" style="border-left:3px solid {TXT_LABEL}">
            <div class="mc-val" style="color:{TXT_PRIMARY}">{avg_dur}s</div>
            <div class="mc-lbl">Avg Duration</div></div>
    </div>""", unsafe_allow_html=True)

    # Filters
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="section-header">Filters</div>', unsafe_allow_html=True)
    hf1, hf2, hf3, hf4 = st.columns(4)
    hist_tables = sorted(hist["SOURCE_TABLE"].dropna().unique().tolist())
    hist_table_f = hf1.selectbox("Table", ["All"] + hist_tables, key="hist_tbl_f")
    hist_statuses = ["All"] + sorted(hist["STATUS"].dropna().unique().tolist())
    hist_status_f = hf2.selectbox("Status", hist_statuses, key="hist_stat_f")
    hist_loads = ["All"] + sorted(hist["LOAD_TYPE"].dropna().unique().tolist())
    hist_load_f = hf3.selectbox("Load Type", hist_loads, key="hist_load_f")
    # Date range
    hist["_dt"] = pd.to_datetime(hist["INSERTED_AT"], errors="coerce")
    min_dt = hist["_dt"].min()
    max_dt = hist["_dt"].max()
    if pd.notna(min_dt) and pd.notna(max_dt):
        date_range = hf4.date_input("Date Range", value=(min_dt.date(), max_dt.date()),
                                    key="hist_date_f")
    else:
        date_range = None

    # Apply filters
    filtered = hist.copy()
    if hist_table_f != "All":
        filtered = filtered[filtered["SOURCE_TABLE"] == hist_table_f]
    if hist_status_f != "All":
        filtered = filtered[filtered["STATUS"] == hist_status_f]
    if hist_load_f != "All":
        filtered = filtered[filtered["LOAD_TYPE"] == hist_load_f]
    if date_range and len(date_range) == 2:
        d_start, d_end = date_range
        filtered = filtered[
            (filtered["_dt"].dt.date >= d_start) & (filtered["_dt"].dt.date <= d_end)]

    st.caption(f"Showing {len(filtered)} of {total_runs} rows")

    # Trend chart
    if not filtered.empty and filtered["_dt"].notna().any():
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown('<div class="section-header">Daily Run Trend</div>',
                    unsafe_allow_html=True)
        chart_df = filtered[["_dt", "STATUS"]].copy()
        chart_df["date"] = chart_df["_dt"].dt.date
        pivot = chart_df.groupby(["date", "STATUS"]).size().reset_index(name="count")
        pivot_wide = pivot.pivot(index="date", columns="STATUS", values="count").fillna(0)
        col_order = [c for c in ["success", "failed", "skipped", "mismatch"]
                     if c in pivot_wide.columns]
        col_order += [c for c in pivot_wide.columns if c not in col_order]
        pivot_wide = pivot_wide[col_order]
        st.bar_chart(pivot_wide,
                     color=[ST_SUCCESS, ST_FAILED, ST_SKIPPED, ST_PENDING][:len(col_order)])

    # Batch grouping with pagination
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="section-header">Run Batches</div>', unsafe_allow_html=True)

    if not filtered.empty and "BATCH_ID" in filtered.columns:
        batches = list(filtered.groupby("BATCH_ID", sort=False))
        PAGE_SIZE = 10
        total_batches = len(batches)
        total_pages = max(1, (total_batches + PAGE_SIZE - 1) // PAGE_SIZE)

        # Page state
        if "hist_page" not in st.session_state:
            st.session_state["hist_page"] = 0
        # Clamp page to valid range
        st.session_state["hist_page"] = max(0, min(st.session_state["hist_page"], total_pages - 1))
        current_page = st.session_state["hist_page"]

        # Slice batches for current page
        start = current_page * PAGE_SIZE
        page_batches = batches[start:start + PAGE_SIZE]

        for batch_id, batch_df in page_batches:
            batch_time = str(batch_df["INSERTED_AT"].iloc[0])[:19]
            n_batch = len(batch_df)
            n_ok = len(batch_df[batch_df["STATUS"] == "success"])
            n_err = len(batch_df[batch_df["STATUS"] == "failed"])
            batch_icon = "✅" if n_err == 0 else "❌"
            batch_dur = batch_df["DURATION_SEC"].dropna().sum()
            label = (f"{batch_icon} Batch {batch_id} — {batch_time} — "
                     f"{n_batch} table(s) ({n_ok} ok, {n_err} failed) — {batch_dur:.1f}s")

            with st.expander(label, expanded=(n_err > 0)):
                show_cols = ["SOURCE_TABLE", "LOAD_TYPE", "STATUS", "DURATION_SEC",
                             "ROWS_EXTRACTED", "ROWS_LOADED", "FAILED_STEP", "ERROR_MESSAGE"]
                show_cols = [c for c in show_cols if c in batch_df.columns]
                st.dataframe(batch_df[show_cols], use_container_width=True, hide_index=True)

                # Show errors with AI explainer
                errs = batch_df[batch_df["STATUS"] == "failed"]
                if not errs.empty:
                    for ei, (_, row) in enumerate(errs.iterrows()):
                        st.error(f"**{row.get('SOURCE_TABLE', '?')}** — "
                                 f"step: {row.get('FAILED_STEP', '?')}")
                        st.code(row.get("ERROR_MESSAGE") or "No error message", language="text")

                        # AI failure explainer
                        try:
                            from shared import ai_enabled, cortex_complete
                            if ai_enabled() and row.get("ERROR_MESSAGE"):
                                ekey = f"_ai_err_{row.get('SOURCE_DB','')}.{row.get('SOURCE_TABLE','')}.{ei}"
                                if st.button("🤖 Explain failure", key=f"btn{ekey}"):
                                    with st.spinner("Asking Cortex…"):
                                        prompt = (
                                            "You are a Snowflake/MySQL data-migration expert. "
                                            "Explain this failure in 2-3 sentences and give a fix.\n"
                                            f"Table: {row.get('SOURCE_DB','?')}.{row.get('SOURCE_TABLE','?')}\n"
                                            f"Load type: {row.get('LOAD_TYPE','?')}\n"
                                            f"Failed step: {row.get('FAILED_STEP','?')}\n"
                                            f"Error: {row.get('ERROR_MESSAGE','')}")
                                        st.session_state[ekey] = cortex_complete(prompt)
                                if st.session_state.get(ekey):
                                    st.info(f"🤖 **Cortex:** {st.session_state[ekey]}")
                        except ImportError:
                            pass

        # Pagination controls (bottom)
        st.markdown("<br>", unsafe_allow_html=True)
        p1, p2, p3 = st.columns([1, 2, 1])
        p1.button("◀ Previous", key="hist_prev",
                  disabled=(current_page == 0),
                  on_click=lambda p=current_page: st.session_state.update(hist_page=p - 1))
        p2.markdown(f"<div style='text-align:center;padding-top:6px;color:{TXT_LABEL}'>"
                    f"Page {current_page + 1} of {total_pages} "
                    f"({total_batches} batches)</div>", unsafe_allow_html=True)
        p3.button("Next ▶", key="hist_next",
                  disabled=(current_page >= total_pages - 1),
                  on_click=lambda p=current_page: st.session_state.update(hist_page=p + 1))
    else:
        from shared import empty_state
        empty_state("🔍", "No Matches", "No runs match the current filters.")

    cur.close()
