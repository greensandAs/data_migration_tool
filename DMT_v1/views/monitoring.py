# Monitoring page — operational health with Tiger Analytics branded styling.
# Co-authored with CoCo
"""pages/monitoring.py — Operational health monitoring.

Tiger Analytics styled tabs: failed runs, stale tables, validation mismatches,
error patterns, step failure distribution, file manifest.
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


def _query_safe(cur, sql: str) -> pd.DataFrame:
    """Execute SQL and return DataFrame, or empty DataFrame on error."""
    try:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    except Exception:
        return pd.DataFrame()


def render(conn):
    """Main render function for the Monitoring page."""
    st.markdown('<div class="section-header">Monitoring & Health</div>',
                unsafe_allow_html=True)

    cur = conn.cursor()
    hours = st.selectbox("Alert Window (hours)", [12, 24, 48, 72], index=1, key="mon_hours")

    # Metrics
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
               ROWS_LOADED AS RAW_LIVE_ROWS, ERROR_MESSAGE, INSERTED_AT
        FROM HISTLOAD_DB.META.RUN_LOG
        WHERE LOAD_TYPE = 'validate' AND STATUS = 'mismatch'
          AND INSERTED_AT > DATEADD('hour', -{hours}, CURRENT_TIMESTAMP())
        ORDER BY INSERTED_AT DESC
    """)

    st.markdown(f"""<div class="cfg-summary">
        <div class="cfg-mini-card" style="border-left:3px solid {ST_FAILED}">
            <div class="mc-val" style="color:{ST_FAILED}">{len(failed_df)}</div>
            <div class="mc-lbl">Failed ({hours}h)</div></div>
        <div class="cfg-mini-card" style="border-left:3px solid {ST_SKIPPED}">
            <div class="mc-val" style="color:{ST_SKIPPED}">{len(stale_df)}</div>
            <div class="mc-lbl">Stale Tables</div></div>
        <div class="cfg-mini-card" style="border-left:3px solid {ST_PENDING}">
            <div class="mc-val" style="color:{ST_PENDING}">{len(mismatch_df)}</div>
            <div class="mc-lbl">Mismatches</div></div>
    </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "❌ Failed Runs", "⏰ Stale Tables", "⚠️ Validation",
        "🔁 Error Patterns", "📊 Step Failures", "📁 File Manifest"
    ])

    with tab1:
        st.markdown('<div class="section-header">Failed Runs</div>', unsafe_allow_html=True)
        if failed_df.empty:
            st.success(f"No failures in the last {hours} hours.")
        else:
            st.dataframe(failed_df, use_container_width=True, hide_index=True)

    with tab2:
        st.markdown('<div class="section-header">Stale Tables</div>', unsafe_allow_html=True)
        if stale_df.empty:
            st.success("All active tables have recent successful runs.")
        else:
            st.warning(f"{len(stale_df)} active table(s) haven't run successfully in {hours}h")
            st.dataframe(stale_df, use_container_width=True, hide_index=True)

    with tab3:
        st.markdown('<div class="section-header">Validation Mismatches</div>',
                    unsafe_allow_html=True)
        if mismatch_df.empty:
            st.success("No validation mismatches detected.")
        else:
            st.dataframe(mismatch_df, use_container_width=True, hide_index=True)

    with tab4:
        st.markdown('<div class="section-header">Recurring Error Patterns</div>',
                    unsafe_allow_html=True)
        err_df = _query_safe(cur, """
            SELECT LEFT(ERROR_MESSAGE, 120) AS ERROR_PREFIX,
                   COUNT(*) AS OCCURRENCES,
                   MAX(INSERTED_AT) AS LAST_SEEN
            FROM HISTLOAD_DB.META.RUN_LOG
            WHERE STATUS = 'failed' AND ERROR_MESSAGE IS NOT NULL
              AND INSERTED_AT > DATEADD('day', -7, CURRENT_TIMESTAMP())
            GROUP BY ERROR_PREFIX
            ORDER BY OCCURRENCES DESC LIMIT 20
        """)
        if err_df.empty:
            st.success("No recurring errors in the last 7 days.")
        else:
            st.dataframe(err_df, use_container_width=True, hide_index=True)
            if not err_df.empty and "OCCURRENCES" in err_df.columns:
                st.bar_chart(err_df.set_index("ERROR_PREFIX")["OCCURRENCES"])

    with tab5:
        st.markdown('<div class="section-header">Step Failure Distribution</div>',
                    unsafe_allow_html=True)
        step_df = _query_safe(cur, """
            SELECT STEP_NAME, COUNT(*) AS FAILURE_COUNT
            FROM HISTLOAD_DB.META.PIPELINE_STEP_LOG
            WHERE STATUS = 'failed'
              AND STARTED_AT > DATEADD('day', -7, CURRENT_TIMESTAMP())
            GROUP BY STEP_NAME ORDER BY FAILURE_COUNT DESC
        """)
        if step_df.empty:
            st.success("No step failures in the last 7 days.")
        else:
            st.dataframe(step_df, use_container_width=True, hide_index=True)
            st.bar_chart(step_df.set_index("STEP_NAME")["FAILURE_COUNT"])

    with tab6:
        st.markdown('<div class="section-header">File Manifest (Last 7 Days)</div>',
                    unsafe_allow_html=True)
        manifest_df = _query_safe(cur, """
            SELECT STATUS, STORAGE_TYPE, FILE_FORMAT, COUNT(*) AS FILE_COUNT,
                   SUM(FILE_SIZE_BYTES) AS TOTAL_BYTES,
                   SUM(ROW_COUNT) AS TOTAL_ROWS
            FROM HISTLOAD_DB.META.FILE_MANIFEST
            WHERE EXTRACTED_AT > DATEADD('day', -7, CURRENT_TIMESTAMP())
            GROUP BY STATUS, STORAGE_TYPE, FILE_FORMAT
            ORDER BY STATUS, STORAGE_TYPE
        """)
        if manifest_df.empty:
            from shared import empty_state
            empty_state("📁", "No Files", "No files in manifest for the last 7 days.")
        else:
            st.dataframe(manifest_df, use_container_width=True, hide_index=True)

    cur.close()
