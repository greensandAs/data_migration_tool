# Observability page — unified Run Logs + Health Dashboard + Alerts & Rules.
# Co-authored with CoCo
"""views/observability.py — Single pane of glass for pipeline observability.

Three tabs:
  1. Run Logs — chronological execution history with AI failure explainer
  2. Health Dashboard — proactive health checks (failed, stale, mismatches)
  3. Alerts & Rules — configurable alert rules + webhook notifications
"""
from __future__ import annotations

import streamlit as st
import pandas as pd

from utils.shared import empty_state

# Brand tokens
ST_SUCCESS = "#34D058"
ST_FAILED = "#F85149"
ST_SKIPPED = "#F0A742"
ST_PENDING = "#58A6FF"
TXT_PRIMARY = "#F0F4F8"
TXT_LABEL = "#7E96B0"
BORDER = "#263245"
TA_ORANGE = "#F15A22"

_ALERT_TABLE = "HISTLOAD_DB.META.DMT_ALERT_RULES"
_ALERT_LOG = "HISTLOAD_DB.META.DMT_ALERT_LOG"


def _query_safe(cur, sql: str, params=None) -> pd.DataFrame:
    """Execute SQL and return DataFrame, or empty DataFrame on error."""
    try:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    except Exception:
        return pd.DataFrame()


def _parse_hours(window: str) -> int:
    mapping = {"30d": 720, "15d": 360, "7d": 168, "4d": 96, "3d": 72, "2d": 48,
               "24h": 24, "8h": 8, "2h": 2, "12h": 12, "48h": 48, "72h": 72}
    return mapping.get(window, 24)


# ══════════════════════════════════════════════════════════════════════════════
# Tab 1: Run Logs
# ══════════════════════════════════════════════════════════════════════════════
def _render_run_logs(cur, conn):
    _profile = st.session_state.get("selected_profile", "All Connections")
    profile_filter = None if _profile == "All Connections" else _profile

    # Controls
    TIME_OPTIONS = ["30d", "15d", "7d", "4d", "3d", "2d", "24h", "8h", "2h"]
    c1, c2 = st.columns([5, 1])
    selected_window = c1.segmented_control(
        "Time Window", TIME_OPTIONS, default="24h", key="obs_time_window") or "24h"
    c2.markdown("<div style='padding-top:26px;'>", unsafe_allow_html=True)
    if c2.button("🔄", key="obs_refresh", help="Refresh data"):
        for k in [k for k in st.session_state if k.startswith("_obs_hist_")]:
            del st.session_state[k]
        st.rerun()

    # Fetch
    hours = _parse_hours(selected_window)
    conditions = [f"INSERTED_AT >= DATEADD('hour', -{hours}, CURRENT_TIMESTAMP())"]
    params = []
    if profile_filter:
        conditions.append("CONNECTION_PROFILE = %s")
        params.append(profile_filter)
    where = "WHERE " + " AND ".join(conditions)

    cache_key = f"_obs_hist_{_profile}_{selected_window}"
    if cache_key not in st.session_state:
        for k in [k for k in st.session_state if k.startswith("_obs_hist_") and k != cache_key]:
            del st.session_state[k]
        try:
            cur.execute(f"""
                SELECT INSERTED_AT, BATCH_ID, CONNECTION_PROFILE, SOURCE_DB, SOURCE_TABLE,
                       TARGET_DB, TARGET_TABLE, LOAD_TYPE, ENGINE, STATUS, FAILED_STEP,
                       DURATION_SEC, ROWS_EXTRACTED, ROWS_LOADED,
                       WATERMARK_FROM, WATERMARK_TO, ERROR_MESSAGE
                FROM HISTLOAD_DB.META.RUN_LOG {where}
                ORDER BY INSERTED_AT DESC
            """, params or None)
            cols = [d[0] for d in cur.description]
            st.session_state[cache_key] = pd.DataFrame(cur.fetchall(), columns=cols)
        except Exception as e:
            st.error(f"Could not read RUN_LOG: {e}")
            st.session_state[cache_key] = pd.DataFrame()

    hist = st.session_state[cache_key]
    if hist.empty:
        empty_state("📜", "No Run History",
                    "Run a pipeline from the <b>▶️ Run</b> page to see execution logs here.")
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
    hf1, hf2, hf3 = st.columns(3)
    hist_tables = sorted(hist["SOURCE_TABLE"].dropna().unique().tolist())
    hist_table_f = hf1.selectbox("Table", ["All"] + hist_tables, key="obs_tbl_f")
    hist_statuses = ["All"] + sorted(hist["STATUS"].dropna().unique().tolist())
    hist_status_f = hf2.selectbox("Status", hist_statuses, key="obs_stat_f")
    hist_loads = ["All"] + sorted(hist["LOAD_TYPE"].dropna().unique().tolist())
    hist_load_f = hf3.selectbox("Load Type", hist_loads, key="obs_load_f")

    filtered = hist.copy()
    if hist_table_f != "All":
        filtered = filtered[filtered["SOURCE_TABLE"] == hist_table_f]
    if hist_status_f != "All":
        filtered = filtered[filtered["STATUS"] == hist_status_f]
    if hist_load_f != "All":
        filtered = filtered[filtered["LOAD_TYPE"] == hist_load_f]

    st.caption(f"Showing {len(filtered)} of {total_runs} rows")

    # Trend chart
    if not filtered.empty:
        filtered["_dt"] = pd.to_datetime(filtered["INSERTED_AT"], errors="coerce")
        if filtered["_dt"].notna().any():
            chart_df = filtered[["_dt", "STATUS"]].copy()
            chart_df["STATUS"] = chart_df["STATUS"].str.lower()
            chart_df["date"] = chart_df["_dt"].dt.date
            pivot = chart_df.groupby(["date", "STATUS"]).size().reset_index(name="count")
            pivot_wide = pivot.pivot(index="date", columns="STATUS", values="count").fillna(0)

            # Map colors by status — colors must be in alphabetical
            # column order because st.bar_chart sorts internally
            STATUS_COLORS = {
                "failed": ST_FAILED,      # red
                "mismatch": ST_PENDING,   # blue
                "skipped": ST_SKIPPED,    # yellow/amber
                "success": ST_SUCCESS,    # green
            }
            cols_sorted = sorted(pivot_wide.columns)
            colors = [STATUS_COLORS.get(c, "#888888") for c in cols_sorted]
            st.bar_chart(pivot_wide[cols_sorted], color=colors,
                         y_label="Runs", x_label="Date")

    # Batch grouping
    st.markdown("<br>", unsafe_allow_html=True)
    if not filtered.empty and "BATCH_ID" in filtered.columns:
        batches = list(filtered.groupby("BATCH_ID", sort=False))
        PAGE_SIZE = 10
        total_batches = len(batches)
        total_pages = max(1, (total_batches + PAGE_SIZE - 1) // PAGE_SIZE)
        page = max(0, min(st.session_state.get("obs_page", 0), total_pages - 1))
        st.session_state["obs_page"] = page

        for batch_id, batch_df in batches[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]:
            batch_time = str(batch_df["INSERTED_AT"].iloc[0])[:19]
            n_ok = len(batch_df[batch_df["STATUS"] == "success"])
            n_err = len(batch_df[batch_df["STATUS"] == "failed"])
            batch_icon = "✅" if n_err == 0 else "❌"
            dur = batch_df["DURATION_SEC"].dropna().sum()
            label = (f"{batch_icon} {batch_id} — {batch_time} — "
                     f"{len(batch_df)} table(s) ({n_ok} ok, {n_err} err) — {dur:.0f}s")

            with st.expander(label, expanded=(n_err > 0)):
                show_cols = [c for c in ["SOURCE_TABLE", "LOAD_TYPE", "STATUS",
                             "DURATION_SEC", "ROWS_EXTRACTED", "ROWS_LOADED",
                             "FAILED_STEP", "ERROR_MESSAGE"] if c in batch_df.columns]
                st.dataframe(batch_df[show_cols], use_container_width=True, hide_index=True)

                errs = batch_df[batch_df["STATUS"] == "failed"]
                for ei, (_, row) in enumerate(errs.iterrows()):
                    st.error(f"**{row.get('SOURCE_TABLE', '?')}** — step: {row.get('FAILED_STEP', '?')}")
                    st.code(row.get("ERROR_MESSAGE") or "—", language="text")
                    try:
                        from utils.shared import ai_enabled, cortex_complete
                        if ai_enabled() and row.get("ERROR_MESSAGE"):
                            ekey = f"_ai_obs_{batch_id}_{ei}"
                            if st.button("🤖 Explain", key=f"btn{ekey}"):
                                with st.spinner("AI…"):
                                    prompt = (
                                        "Explain this data migration failure in 2-3 sentences and suggest a fix.\n"
                                        f"Table: {row.get('SOURCE_DB','?')}.{row.get('SOURCE_TABLE','?')}\n"
                                        f"Step: {row.get('FAILED_STEP','?')}\n"
                                        f"Error: {row.get('ERROR_MESSAGE','')}")
                                    st.session_state[ekey] = cortex_complete(prompt, feature="history", conn=conn)
                            if st.session_state.get(ekey):
                                st.info(f"🤖 {st.session_state[ekey]}")
                    except ImportError:
                        pass

        if total_pages > 1:
            p1, p2, p3 = st.columns([1, 2, 1])
            p1.button("◀ Prev", key="obs_prev", disabled=(page == 0),
                      on_click=lambda: st.session_state.update(obs_page=page - 1))
            p2.markdown(f"<div style='text-align:center;padding-top:6px;color:{TXT_LABEL}'>"
                        f"Page {page + 1}/{total_pages}</div>", unsafe_allow_html=True)
            p3.button("Next ▶", key="obs_next", disabled=(page >= total_pages - 1),
                      on_click=lambda: st.session_state.update(obs_page=page + 1))


# ══════════════════════════════════════════════════════════════════════════════
# Tab 2: Health Dashboard
# ══════════════════════════════════════════════════════════════════════════════
def _render_health(cur):
    TIME_OPTIONS = ["30d", "15d", "7d", "3d", "24h", "12h"]
    c1, c2 = st.columns([5, 1])
    selected_window = c1.segmented_control(
        "Alert Window", TIME_OPTIONS, default="24h", key="obs_health_window") or "24h"
    c2.markdown("<div style='padding-top:26px;'>", unsafe_allow_html=True)
    if c2.button("🔄", key="obs_health_refresh", help="Refresh"):
        st.rerun()
    hours = _parse_hours(selected_window)

    failed_df = _query_safe(cur, f"""
        SELECT BATCH_ID, SOURCE_DB, SOURCE_TABLE, TARGET_TABLE, LOAD_TYPE,
               ENGINE, ERROR_MESSAGE, FAILED_STEP, RUN_START, RUN_END
        FROM HISTLOAD_DB.META.RUN_LOG
        WHERE STATUS = 'failed' AND INSERTED_AT > DATEADD('hour', -{hours}, CURRENT_TIMESTAMP())
        ORDER BY INSERTED_AT DESC
    """)

    stale_df = _query_safe(cur, f"""
        SELECT SOURCE_DB, SOURCE_TABLE, TARGET_DB, TARGET_TABLE,
               LAST_RUN_STATUS, LAST_LOADED_AT, LAST_FAILED_STEP
        FROM HISTLOAD_DB.META.MIGRATION_CONFIG
        WHERE ACTIVE = TRUE
          AND (LAST_LOADED_AT IS NULL
               OR LAST_LOADED_AT < DATEADD('hour', -{hours}, CURRENT_TIMESTAMP()))
        ORDER BY LAST_LOADED_AT NULLS FIRST
    """)

    mismatch_df = _query_safe(cur, f"""
        SELECT SOURCE_DB, SOURCE_TABLE, ROWS_EXTRACTED AS SOURCE_ROWS,
               ROWS_LOADED AS TARGET_ROWS, ERROR_MESSAGE, INSERTED_AT
        FROM HISTLOAD_DB.META.RUN_LOG
        WHERE LOAD_TYPE = 'validate' AND STATUS = 'mismatch'
          AND INSERTED_AT > DATEADD('hour', -{hours}, CURRENT_TIMESTAMP())
        ORDER BY INSERTED_AT DESC
    """)

    # Metric cards
    st.markdown(f"""<div class="cfg-summary">
        <div class="cfg-mini-card" style="border-left:3px solid {ST_FAILED}">
            <div class="mc-val" style="color:{ST_FAILED}">{len(failed_df)}</div>
            <div class="mc-lbl">Failed ({hours}h)</div></div>
        <div class="cfg-mini-card" style="border-left:3px solid {ST_SKIPPED}">
            <div class="mc-val" style="color:{ST_SKIPPED}">{len(stale_df)}</div>
            <div class="mc-lbl">Stale Tables</div></div>
        <div class="cfg-mini-card" style="border-left:3px solid {ST_PENDING}">
            <div class="mc-val" style="color:{ST_PENDING}">{len(mismatch_df)}</div>
            <div class="mc-lbl">Row Mismatches</div></div>
    </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Sub-tabs for each category
    h1, h2, h3, h4, h5 = st.tabs([
        "❌ Failed Runs", "⏰ Stale Tables", "⚠️ Mismatches",
        "🔁 Error Patterns", "📊 Step Failures"
    ])

    with h1:
        if failed_df.empty:
            st.success(f"No failures in the last {hours} hours.")
        else:
            st.dataframe(failed_df, use_container_width=True, hide_index=True)

    with h2:
        if stale_df.empty:
            st.success("All active tables have recent successful runs.")
        else:
            st.warning(f"{len(stale_df)} active table(s) haven't run in {hours}h")
            st.dataframe(stale_df, use_container_width=True, hide_index=True)

    with h3:
        if mismatch_df.empty:
            st.success("No validation mismatches.")
        else:
            st.dataframe(mismatch_df, use_container_width=True, hide_index=True)

    with h4:
        err_df = _query_safe(cur, """
            SELECT LEFT(ERROR_MESSAGE, 120) AS ERROR_PATTERN,
                   COUNT(*) AS OCCURRENCES, MAX(INSERTED_AT) AS LAST_SEEN
            FROM HISTLOAD_DB.META.RUN_LOG
            WHERE STATUS = 'failed' AND ERROR_MESSAGE IS NOT NULL
              AND INSERTED_AT > DATEADD('day', -7, CURRENT_TIMESTAMP())
            GROUP BY ERROR_PATTERN ORDER BY OCCURRENCES DESC LIMIT 15
        """)
        if err_df.empty:
            st.success("No recurring errors in 7 days.")
        else:
            st.dataframe(err_df, use_container_width=True, hide_index=True)
            st.bar_chart(err_df.set_index("ERROR_PATTERN")["OCCURRENCES"])

    with h5:
        step_df = _query_safe(cur, """
            SELECT STEP_NAME, COUNT(*) AS FAILURES
            FROM HISTLOAD_DB.META.PIPELINE_STEP_LOG
            WHERE STATUS = 'failed'
              AND STARTED_AT > DATEADD('day', -7, CURRENT_TIMESTAMP())
            GROUP BY STEP_NAME ORDER BY FAILURES DESC
        """)
        if step_df.empty:
            st.success("No step failures in 7 days.")
        else:
            st.dataframe(step_df, use_container_width=True, hide_index=True)
            st.bar_chart(step_df.set_index("STEP_NAME")["FAILURES"])


# ══════════════════════════════════════════════════════════════════════════════
# Tab 3: Alerts & Rules
# ══════════════════════════════════════════════════════════════════════════════
def _render_alerts(cur, conn):
    st.markdown(
        f'<div style="font-size:.75rem;color:{TXT_LABEL};margin-bottom:12px;">'
        f'Configure alert rules to get notified when pipelines fail, tables go stale, '
        f'or row counts drift beyond thresholds.</div>', unsafe_allow_html=True)

    # ── Active Rules ─────────────────────────────────────────────────────────
    rules_df = _query_safe(cur, f"SELECT * FROM {_ALERT_TABLE} ORDER BY CREATED_AT DESC")

    if not rules_df.empty:
        st.markdown("#### Active Rules")
        for _, rule in rules_df.iterrows():
            active_icon = "🟢" if rule.get("ACTIVE") else "⚫"
            st.markdown(
                f'<div style="background:{BORDER}44;border-radius:6px;padding:10px 14px;'
                f'margin-bottom:8px;border-left:3px solid {TA_ORANGE if rule.get("ACTIVE") else TXT_LABEL};">'
                f'<div style="font-size:.82rem;font-weight:600;color:{TXT_PRIMARY};">'
                f'{active_icon} {rule.get("RULE_NAME", "—")}</div>'
                f'<div style="font-size:.7rem;color:{TXT_LABEL};margin-top:4px;">'
                f'Condition: <code>{rule.get("CONDITION_TYPE", "?")}</code> · '
                f'Threshold: <code>{rule.get("THRESHOLD", "?")}</code> · '
                f'Action: <code>{rule.get("ACTION_TYPE", "?")}</code></div>'
                f'</div>', unsafe_allow_html=True)

            r1, r2, r3 = st.columns([1, 1, 4])
            if rule.get("ACTIVE"):
                if r1.button("⏸ Disable", key=f"dis_{rule.get('RULE_ID')}",
                             use_container_width=True):
                    try:
                        cur.execute(f"UPDATE {_ALERT_TABLE} SET ACTIVE = FALSE WHERE RULE_ID = %s",
                                    (rule["RULE_ID"],))
                        conn.commit()
                        st.rerun()
                    except Exception:
                        pass
            else:
                if r1.button("▶ Enable", key=f"en_{rule.get('RULE_ID')}",
                             use_container_width=True):
                    try:
                        cur.execute(f"UPDATE {_ALERT_TABLE} SET ACTIVE = TRUE WHERE RULE_ID = %s",
                                    (rule["RULE_ID"],))
                        conn.commit()
                        st.rerun()
                    except Exception:
                        pass
            if r2.button("🗑️ Delete", key=f"del_{rule.get('RULE_ID')}",
                         use_container_width=True):
                try:
                    cur.execute(f"DELETE FROM {_ALERT_TABLE} WHERE RULE_ID = %s",
                                (rule["RULE_ID"],))
                    conn.commit()
                    st.rerun()
                except Exception:
                    pass

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Create New Rule ──────────────────────────────────────────────────────
    st.markdown("#### Create Alert Rule")
    with st.expander("➕ New Rule", expanded=rules_df.empty):
        rc1, rc2 = st.columns(2)
        rule_name = rc1.text_input("Rule Name", placeholder="e.g. Stale Orders Alert",
                                   key="obs_rule_name")
        condition_type = rc2.selectbox("Condition", [
            "TABLE_STALE", "RUN_FAILED", "ROW_DRIFT_PCT", "CONSECUTIVE_FAILURES"
        ], key="obs_cond_type",
            format_func=lambda x: {
                "TABLE_STALE": "Table not loaded in X hours",
                "RUN_FAILED": "Any run failure",
                "ROW_DRIFT_PCT": "Row count drift > X%",
                "CONSECUTIVE_FAILURES": "N consecutive failures",
            }.get(x, x))

        rc3, rc4 = st.columns(2)
        threshold = rc3.number_input("Threshold",
                                     min_value=1, value=24, key="obs_threshold",
                                     help="Hours for stale, % for drift, count for consecutive")
        table_scope = rc4.text_input("Table Scope (optional)",
                                     placeholder="Leave blank for all tables",
                                     key="obs_table_scope")

        st.markdown(f'<div style="font-size:.65rem;letter-spacing:1.5px;text-transform:uppercase;'
                    f'color:{TXT_LABEL};font-weight:600;margin:12px 0 6px;">Action</div>',
                    unsafe_allow_html=True)

        ac1, ac2 = st.columns(2)
        action_type = ac1.selectbox("Action Type", [
            "LOG_ONLY", "WEBHOOK_SLACK", "WEBHOOK_TEAMS", "WEBHOOK_CUSTOM"
        ], key="obs_action_type",
            format_func=lambda x: {
                "LOG_ONLY": "Log only (visible in alert history)",
                "WEBHOOK_SLACK": "Slack webhook",
                "WEBHOOK_TEAMS": "Microsoft Teams webhook",
                "WEBHOOK_CUSTOM": "Custom webhook URL",
            }.get(x, x))

        webhook_url = ""
        if action_type != "LOG_ONLY":
            webhook_url = ac2.text_input("Webhook URL",
                                         placeholder="https://hooks.slack.com/...",
                                         key="obs_webhook_url")

        if st.button("💾 Save Rule", type="primary", use_container_width=True,
                     key="obs_save_rule"):
            if not rule_name.strip():
                st.error("Rule name is required.")
            else:
                try:
                    cur.execute(f"""
                        INSERT INTO {_ALERT_TABLE}
                        (RULE_ID, RULE_NAME, CONDITION_TYPE, THRESHOLD,
                         TABLE_SCOPE, ACTION_TYPE, WEBHOOK_URL, ACTIVE, CREATED_AT)
                        VALUES (UUID_STRING(), %s, %s, %s, %s, %s, %s, TRUE, CURRENT_TIMESTAMP())
                    """, (rule_name.strip(), condition_type, int(threshold),
                          table_scope.strip() or None, action_type,
                          webhook_url.strip() or None))
                    conn.commit()
                    st.success(f"Rule '{rule_name}' created.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to save rule: {e}")

    # ── Alert History ────────────────────────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("#### Alert History")
    alert_log = _query_safe(cur, f"""
        SELECT TRIGGERED_AT, RULE_NAME, CONDITION_TYPE, TABLE_NAME,
               MESSAGE, ACTION_TAKEN
        FROM {_ALERT_LOG}
        ORDER BY TRIGGERED_AT DESC LIMIT 50
    """)
    if alert_log.empty:
        st.info("No alerts have triggered yet. Rules are evaluated on each pipeline run.")
    else:
        st.dataframe(alert_log, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# Main render
# ══════════════════════════════════════════════════════════════════════════════
def render(conn):
    """Main render function for the Observability page."""
    st.markdown('<div class="section-header">Observability</div>',
                unsafe_allow_html=True)

    cur = conn.cursor()

    tab_logs, tab_health, tab_alerts = st.tabs([
        "📜 Run Logs", "🩺 Health Dashboard", "🔔 Alerts & Rules"
    ])

    with tab_logs:
        _render_run_logs(cur, conn)

    with tab_health:
        _render_health(cur)

    with tab_alerts:
        _render_alerts(cur, conn)

    cur.close()
