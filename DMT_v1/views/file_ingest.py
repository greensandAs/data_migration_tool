# File Ingestion page — cloud file → Snowflake loading.
# Co-authored with CoCo
"""views/file_ingest.py — File Ingestion configuration and execution.

Provides UI for managing cloud file ingestion jobs:
  - List active jobs with status
  - Add/edit job config (stage, path, pattern, format, target)
  - Run ingestion (single job or all)
  - View run history
"""
from __future__ import annotations

import streamlit as st
import pandas as pd

from metadata import file_ingest_config
from utils.shared import empty_state

# Brand tokens
ST_SUCCESS = "#34D058"
ST_FAILED = "#F85149"
ST_SKIPPED = "#F0A742"
TXT_PRIMARY = "#F0F4F8"
TXT_LABEL = "#7E96B0"


def render(conn):
    """Main render function for the File Ingest page."""
    st.markdown('<div class="section-header">File Ingestion</div>',
                unsafe_allow_html=True)

    cur = conn.cursor()

    # Tabs
    tab_jobs, tab_add, tab_history = st.tabs([
        "Active Jobs", "Add / Edit Job", "Run History"
    ])

    # ── Tab 1: Active Jobs ────────────────────────────────────────────────────
    with tab_jobs:
        configs = file_ingest_config.list_configs(cur, active_only=False)

        if not configs:
            empty_state("", "No File Ingestion Jobs",
                        "Switch to the <b>Add / Edit Job</b> tab to create one.")
        else:
            # Summary metrics
            active = [c for c in configs if c.get("ACTIVE")]
            last_ok = sum(1 for c in active
                          if (c.get("LAST_RUN_STATUS") or "").lower() == "success")
            last_fail = sum(1 for c in active
                           if (c.get("LAST_RUN_STATUS") or "").lower() == "failed")

            c1, c2, c3 = st.columns(3)
            c1.metric("Active Jobs", len(active))
            c2.metric("Last Run OK", last_ok)
            c3.metric("Last Run Failed", last_fail)

            st.markdown("<br>", unsafe_allow_html=True)

            # Run controls
            col_run, col_single = st.columns([1, 2])
            if col_run.button("Run All Active Jobs", type="primary",
                              use_container_width=True):
                with st.spinner("Running all active ingestion jobs..."):
                    from core.file_ingester import run_all
                    results = run_all(conn)
                    for r in results:
                        status = r.get("STATUS", "?")
                        icon = {"success": "OK", "failed": "ERR",
                                "skipped": "SKIP"}.get(status, "?")
                        st.write(f"[{icon}] {r.get('JOB_NAME')}: "
                                 f"{r.get('ROWS_LOADED', 0):,} rows")
                    st.rerun()

            job_names = [c["JOB_NAME"] for c in active]
            if job_names:
                sel_job = col_single.selectbox(
                    "Run single job", [""] + job_names,
                    key="run_single_ingest")
                if sel_job and col_single.button(f"Run '{sel_job}'"):
                    with st.spinner(f"Running {sel_job}..."):
                        from core.file_ingester import run_all
                        results = run_all(conn, only_job=sel_job)
                        if results:
                            r = results[0]
                            if r["STATUS"] == "success":
                                st.success(
                                    f"Loaded {r.get('ROWS_LOADED', 0):,} rows "
                                    f"from {r.get('FILES_LOADED', 0)} file(s)")
                            elif r["STATUS"] == "skipped":
                                st.warning(r.get("ERROR_MESSAGE", "Skipped"))
                            else:
                                st.error(r.get("ERROR_MESSAGE", "Failed"))
                        st.rerun()

            st.markdown("<br>", unsafe_allow_html=True)

            # Job table
            display_cols = ["JOB_NAME", "FILE_TYPE", "STAGE_NAME", "CLOUD_PATH",
                            "FILE_PATTERN", "TARGET_TABLE", "LOAD_MODE",
                            "ACTIVE", "LAST_RUN_STATUS", "LAST_ROW_COUNT",
                            "LAST_RUN_AT"]
            df = pd.DataFrame(configs)
            available = [c for c in display_cols if c in df.columns]
            st.dataframe(df[available], use_container_width=True, hide_index=True)

    # ── Tab 2: Add / Edit Job ─────────────────────────────────────────────────
    with tab_add:
        st.markdown("#### Configure File Ingestion Job")

        # Select existing to edit, or create new
        existing = file_ingest_config.list_configs(cur, active_only=False)
        edit_options = ["-- New Job --"] + [c["JOB_NAME"] for c in existing]
        edit_sel = st.selectbox("Edit existing or create new",
                                edit_options, key="fi_edit_sel")

        editing = None
        if edit_sel != "-- New Job --":
            editing = next((c for c in existing if c["JOB_NAME"] == edit_sel), None)

        with st.form("file_ingest_form", clear_on_submit=False):
            col1, col2 = st.columns(2)

            # Basic info
            job_name = col1.text_input(
                "Job Name *",
                value=editing["JOB_NAME"] if editing else "",
                placeholder="daily_sales_feed")
            active = col2.checkbox(
                "Active",
                value=editing["ACTIVE"] if editing else True)

            st.markdown("---")
            st.markdown("**Source (Cloud Files)**")
            s1, s2 = st.columns(2)
            stage_name = s1.text_input(
                "Stage Name (FQN) *",
                value=editing.get("STAGE_NAME", "") if editing else "",
                placeholder="ANALYTICS.RAW.S3_STAGE")
            cloud_provider = s2.selectbox(
                "Cloud Provider",
                ["S3", "AZURE", "GCS"],
                index=["S3", "AZURE", "GCS"].index(
                    editing.get("CLOUD_PROVIDER", "S3")) if editing else 0)

            s3, s4 = st.columns(2)
            cloud_path = s3.text_input(
                "Cloud Path (subfolder)",
                value=editing.get("CLOUD_PATH", "") if editing else "",
                placeholder="data-feeds/sales/")
            file_pattern = s4.text_input(
                "File Pattern (regex) *",
                value=editing.get("FILE_PATTERN", "") if editing else "",
                placeholder=".*sales.*\\.csv")

            s5, s6 = st.columns(2)
            file_type = s5.selectbox(
                "File Type",
                ["CSV", "PARQUET", "JSON", "AVRO"],
                index=["CSV", "PARQUET", "JSON", "AVRO"].index(
                    editing.get("FILE_TYPE", "CSV")) if editing else 0)
            date_partition = s6.checkbox(
                "Date-partitioned path (append YYYYMMDD)",
                value=editing.get("DATE_PARTITION", False) if editing else False)

            st.markdown("---")
            st.markdown("**Target (Snowflake)**")
            t1, t2, t3 = st.columns(3)
            target_db = t1.text_input(
                "Target Database *",
                value=editing.get("TARGET_DB", "") if editing else "",
                placeholder="ANALYTICS")
            target_schema = t2.text_input(
                "Target Schema",
                value=editing.get("TARGET_SCHEMA", "RAW") if editing else "RAW")
            target_table = t3.text_input(
                "Target Table *",
                value=editing.get("TARGET_TABLE", "") if editing else "",
                placeholder="DAILY_SALES")

            t4, t5 = st.columns(2)
            load_mode = t4.selectbox(
                "Load Mode",
                ["APPEND", "OVERWRITE", "MERGE"],
                index=["APPEND", "OVERWRITE", "MERGE"].index(
                    editing.get("LOAD_MODE", "APPEND")) if editing else 0)
            table_exists = t5.checkbox(
                "Table already exists",
                value=editing.get("TABLE_EXISTS", True) if editing else True,
                help="Uncheck to auto-create table from file schema")

            st.markdown("---")
            st.markdown("**Format Options** (CSV)")
            f1, f2, f3, f4 = st.columns(4)
            delimiter = f1.text_input(
                "Delimiter",
                value=editing.get("FIELD_DELIMITER", ",") if editing else ",")
            enclosed = f2.text_input(
                "Enclosed By",
                value=editing.get("FIELD_ENCLOSED_BY", '"') if editing else '"')
            escape = f3.text_input(
                "Escape Char",
                value=editing.get("ESCAPE_CHARACTER", "\\") if editing else "\\")
            skip_header = f4.number_input(
                "Skip Header",
                value=int(editing.get("SKIP_HEADER", 1)) if editing else 1,
                min_value=0, max_value=10)

            st.markdown("---")
            st.markdown("**COPY INTO Options**")
            o1, o2 = st.columns(2)
            on_error = o1.selectbox(
                "ON_ERROR",
                ["ABORT_STATEMENT", "CONTINUE", "SKIP_FILE"],
                index=["ABORT_STATEMENT", "CONTINUE", "SKIP_FILE"].index(
                    editing.get("ON_ERROR", "ABORT_STATEMENT")) if editing else 0)
            match_by = o2.selectbox(
                "MATCH_BY_COLUMN_NAME",
                ["", "CASE_INSENSITIVE", "CASE_SENSITIVE"],
                index=["", "CASE_INSENSITIVE", "CASE_SENSITIVE"].index(
                    editing.get("MATCH_BY_COLUMN_NAME") or "") if editing else 0)
            purge = st.checkbox(
                "Purge files after load",
                value=editing.get("PURGE_FILES", False) if editing else False)

            submitted = st.form_submit_button(
                "Save Job" if not editing else "Update Job",
                type="primary", use_container_width=True)

            if submitted:
                if not job_name or not stage_name or not file_pattern or not target_db or not target_table:
                    st.error("Please fill all required fields (marked with *)")
                else:
                    save_data = {
                        "JOB_NAME": job_name.strip(),
                        "ACTIVE": active,
                        "CLOUD_PROVIDER": cloud_provider,
                        "STAGE_NAME": stage_name.strip(),
                        "CLOUD_PATH": cloud_path.strip(),
                        "FILE_PATTERN": file_pattern.strip(),
                        "FILE_TYPE": file_type,
                        "TARGET_DB": target_db.strip().upper(),
                        "TARGET_SCHEMA": target_schema.strip().upper() or "RAW",
                        "TARGET_TABLE": target_table.strip().upper(),
                        "LOAD_MODE": load_mode,
                        "TABLE_EXISTS": table_exists,
                        "FIELD_DELIMITER": delimiter,
                        "FIELD_ENCLOSED_BY": enclosed,
                        "ESCAPE_CHARACTER": escape,
                        "SKIP_HEADER": int(skip_header),
                        "ON_ERROR": on_error,
                        "MATCH_BY_COLUMN_NAME": match_by or None,
                        "PURGE_FILES": purge,
                        "DATE_PARTITION": date_partition,
                    }
                    if editing:
                        save_data["CONFIG_ID"] = editing["CONFIG_ID"]
                    file_ingest_config.upsert(cur, save_data)
                    conn.commit()
                    st.success(f"Job '{job_name}' saved successfully!")
                    st.rerun()

    # ── Tab 3: Run History ────────────────────────────────────────────────────
    with tab_history:
        logs = file_ingest_config.get_recent_logs(cur, limit=100)

        if not logs:
            empty_state("", "No Run History",
                        "Run an ingestion job to see results here.")
        else:
            # Summary
            ok = sum(1 for l in logs if l.get("STATUS") == "success")
            fail = sum(1 for l in logs if l.get("STATUS") == "failed")
            total_rows = sum(int(l.get("ROWS_LOADED") or 0) for l in logs
                            if l.get("STATUS") == "success")

            c1, c2, c3 = st.columns(3)
            c1.metric("Successful Runs", ok)
            c2.metric("Failed Runs", fail)
            c3.metric("Total Rows Loaded", f"{total_rows:,}")

            st.markdown("<br>", unsafe_allow_html=True)

            display_cols = ["BATCH_ID", "JOB_NAME", "FILE_TYPE", "LOAD_MODE",
                            "FILES_MATCHED", "FILES_LOADED", "ROWS_LOADED",
                            "STATUS", "DURATION_SEC", "ERROR_MESSAGE",
                            "RUN_START"]
            df = pd.DataFrame(logs)
            available = [c for c in display_cols if c in df.columns]
            st.dataframe(df[available], use_container_width=True, hide_index=True)

    cur.close()
