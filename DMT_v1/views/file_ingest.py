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
    tab_jobs, tab_add, tab_upload, tab_stages, tab_history = st.tabs([
        "Active Jobs", "Add / Edit Job", "Upload & Ingest", "Stages & Integrations", "Run History"
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

            # ── Run Controls (styled card) ────────────────────────────────────
            from utils.ui_theme import section_card_start as _scs2, section_card_end as _sce2
            _scs2("Run Ingestion", "▶️", border_color=ST_SUCCESS)

            run_mode = st.radio(
                "Execution Mode",
                ["Run All Active", "Run Single Job"],
                horizontal=True, key="fi_run_mode")

            if run_mode == "Run All Active":
                st.caption(f"Runs all {len(active)} active job(s) sequentially.")
                if st.button("Run All Active Jobs", type="primary",
                             use_container_width=True, key="fi_run_all"):
                    with st.spinner("Running all active ingestion jobs..."):
                        from core.file_ingester import run_all
                        results = run_all(conn)
                        for r in results:
                            status = r.get("STATUS", "?")
                            icon = {"success": "✅", "failed": "❌",
                                    "skipped": "⏭️"}.get(status, "⏳")
                            st.write(f"{icon} **{r.get('JOB_NAME')}**: "
                                     f"{int(r.get('ROWS_LOADED') or 0):,} rows")
                        st.rerun()
            else:
                job_names = [c["JOB_NAME"] for c in active]
                if job_names:
                    sel_job = st.selectbox(
                        "Select job", job_names, key="fi_run_single_sel")
                    if st.button(f"Run '{sel_job}'", type="primary",
                                 use_container_width=True, key="fi_run_single_btn"):
                        with st.spinner(f"Running {sel_job}..."):
                            from core.file_ingester import run_all
                            results = run_all(conn, only_job=sel_job)
                            if results:
                                r = results[0]
                                if r["STATUS"] == "success":
                                    st.success(
                                        f"Loaded {int(r.get('ROWS_LOADED') or 0):,} rows "
                                        f"from {int(r.get('FILES_LOADED') or 0)} file(s)")
                                elif r["STATUS"] == "skipped":
                                    st.warning(r.get("ERROR_MESSAGE", "Skipped"))
                                else:
                                    st.error(r.get("ERROR_MESSAGE", "Failed"))
                            st.rerun()
                else:
                    st.info("No active jobs to run. Activate a job first.")

            _sce2()

            st.markdown("<br>", unsafe_allow_html=True)

            # Job table with action buttons (paginated)
            JOBS_PER_PAGE = 10
            total_jobs = len(configs)
            total_job_pages = max(1, (total_jobs + JOBS_PER_PAGE - 1) // JOBS_PER_PAGE)
            job_page = st.session_state.get("fi_job_page", 0)
            job_page = min(job_page, total_job_pages - 1)

            start_j = job_page * JOBS_PER_PAGE
            end_j = min(start_j + JOBS_PER_PAGE, total_jobs)
            page_configs = configs[start_j:end_j]

            for cfg in page_configs:
                is_active = cfg.get("ACTIVE")
                last_status = (cfg.get("LAST_RUN_STATUS") or "").lower()
                status_cls = "success" if last_status == "success" else "failed" if last_status == "failed" else "pending"
                status_icon = {"success": "✅", "failed": "❌"}.get(last_status, "⏳")
                row_count = int(cfg.get('LAST_ROW_COUNT') or 0)
                last_run = cfg.get('LAST_RUN_AT') or 'never'

                # Job card using theme's table-card style
                st.markdown(
                    f'<div class="table-card {status_cls}">'
                    f'<span class="tstatus {status_cls}">{status_icon} {status_cls.upper()}</span>'
                    f'<div class="tname">{"🟢" if is_active else "⚫"} {cfg["JOB_NAME"]}</div>'
                    f'<div class="tmeta">'
                    f'<span class="pill pill-source">{cfg.get("FILE_TYPE", "?")}</span> '
                    f'<span class="pill pill-full">{cfg.get("LOAD_MODE", "APPEND")}</span> '
                    f'→ {cfg.get("TARGET_TABLE", "?")}</div>'
                    f'<div class="tmeta" style="margin-top:6px;">'
                    f'Stage: <code>{cfg.get("STAGE_NAME", "")}/{cfg.get("CLOUD_PATH", "")}</code> · '
                    f'Pattern: <code>{cfg.get("FILE_PATTERN", "")}</code></div>'
                    f'<div class="tmeta">Last run: {row_count:,} rows at {last_run}</div>'
                    f'</div>',
                    unsafe_allow_html=True)

                # Action buttons below each card
                a1, a2, a3 = st.columns([1, 1, 4])
                if is_active:
                    if a1.button("⏸ Pause", key=f"deact_{cfg['CONFIG_ID']}",
                                 use_container_width=True):
                        file_ingest_config.deactivate(cur, cfg["CONFIG_ID"])
                        conn.commit()
                        st.rerun()
                else:
                    if a1.button("▶ Resume", key=f"act_{cfg['CONFIG_ID']}",
                                 use_container_width=True):
                        file_ingest_config.activate(cur, cfg["CONFIG_ID"])
                        conn.commit()
                        st.rerun()
                if a2.button("🗑️ Delete", key=f"del_{cfg['CONFIG_ID']}",
                             use_container_width=True):
                    file_ingest_config.delete_config(cur, cfg["CONFIG_ID"])
                    conn.commit()
                    st.rerun()

            # Jobs pagination controls
            if total_job_pages > 1:
                jp1, jp2, jp3 = st.columns([1, 2, 1])
                jp1.button("◀ Prev", key="fi_job_prev",
                           disabled=(job_page == 0),
                           on_click=lambda: st.session_state.update(fi_job_page=job_page - 1))
                jp2.markdown(
                    f"<div style='text-align:center;padding-top:6px;color:{TXT_LABEL}'>"
                    f"Page {job_page + 1} of {total_job_pages} ({total_jobs} jobs)</div>",
                    unsafe_allow_html=True)
                jp3.button("Next ▶", key="fi_job_next",
                           disabled=(job_page >= total_job_pages - 1),
                           on_click=lambda: st.session_state.update(fi_job_page=job_page + 1))

            # ── Stage File Browser ───────────────────────────────────────────
            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown("#### Stage File Browser")
            st.caption("View files currently on a stage for any configured job.")

            browse_jobs = [c["JOB_NAME"] for c in configs if c.get("STAGE_NAME")]
            if browse_jobs:
                browse_sel = st.selectbox("Select job to browse stage",
                                         [""] + browse_jobs, key="fi_browse_sel")
                if browse_sel:
                    browse_cfg = next(c for c in configs if c["JOB_NAME"] == browse_sel)
                    stage_name = browse_cfg.get("STAGE_NAME", "")
                    cloud_path = (browse_cfg.get("CLOUD_PATH") or "").strip("/")
                    browse_path = f"@{stage_name}/{cloud_path}/" if cloud_path else f"@{stage_name}/"

                    try:
                        cur.execute(f"LIST {browse_path}")
                        list_results = cur.fetchall()
                        list_cols = [d[0] for d in cur.description]

                        if list_results:
                            list_df = pd.DataFrame(list_results, columns=list_cols)
                            show_cols = [c for c in ["name", "size", "last_modified"]
                                        if c in list_df.columns]
                            display_df = list_df[show_cols] if show_cols else list_df

                            # Paginate stage files
                            FILES_PER_PAGE = 20
                            total_files = len(display_df)
                            total_file_pages = max(1, (total_files + FILES_PER_PAGE - 1) // FILES_PER_PAGE)
                            file_page = st.session_state.get("fi_file_page", 0)
                            file_page = min(file_page, total_file_pages - 1)

                            start_f = file_page * FILES_PER_PAGE
                            end_f = min(start_f + FILES_PER_PAGE, total_files)

                            st.dataframe(display_df.iloc[start_f:end_f],
                                        use_container_width=True, hide_index=True)
                            st.caption(f"Showing {start_f + 1}–{end_f} of "
                                       f"{total_files} file(s) on `{browse_path}`")

                            if total_file_pages > 1:
                                fp1, fp2, fp3 = st.columns([1, 2, 1])
                                fp1.button("◀ Prev", key="fi_file_prev",
                                           disabled=(file_page == 0),
                                           on_click=lambda: st.session_state.update(fi_file_page=file_page - 1))
                                fp2.markdown(
                                    f"<div style='text-align:center;padding-top:6px;color:{TXT_LABEL}'>"
                                    f"Page {file_page + 1} of {total_file_pages}</div>",
                                    unsafe_allow_html=True)
                                fp3.button("Next ▶", key="fi_file_next",
                                           disabled=(file_page >= total_file_pages - 1),
                                           on_click=lambda: st.session_state.update(fi_file_page=file_page + 1))
                        else:
                            st.info(f"No files found on `{browse_path}`")
                    except Exception as e:
                        st.warning(f"Cannot list stage: {e}")

    # ── Tab 2: Add / Edit Job ─────────────────────────────────────────────────
    with tab_add:
        from utils.ui_theme import (section_card_start, section_card_end,
                                    info_box, ST_PENDING, ST_SUCCESS, TA_ORANGE,
                                    BORDER, TA_NAVY, TXT_SECONDARY as _TXT_SEC)

        st.markdown("#### Configure File Ingestion Job")

        # Select existing to edit, or create new
        existing = file_ingest_config.list_configs(cur, active_only=False)
        edit_options = ["-- New Job --"] + [c["JOB_NAME"] for c in existing]
        edit_sel = st.selectbox("Edit existing or create new",
                                edit_options, key="fi_edit_sel")

        editing = None
        if edit_sel != "-- New Job --":
            editing = next((c for c in existing if c["JOB_NAME"] == edit_sel), None)

        # ═══════════════════════════════════════════════════════════════════════
        # Section 1: JOB IDENTITY
        # ═══════════════════════════════════════════════════════════════════════
        section_card_start("Job Identity", "📋", border_color=TA_ORANGE)
        id1, id2 = st.columns([3, 1])
        job_name = id1.text_input(
            "Job Name *",
            value=editing["JOB_NAME"] if editing else "",
            placeholder="daily_sales_feed", key="fi_job_name")
        id2.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
        active = id2.checkbox(
            "Active",
            value=editing["ACTIVE"] if editing else True, key="fi_active")
        section_card_end()

        # ═══════════════════════════════════════════════════════════════════════
        # Section 2: SOURCE
        # ═══════════════════════════════════════════════════════════════════════
        section_card_start("Source (Cloud Files)", "📂", border_color=ST_PENDING)

        # Dynamic stage picker
        s1, s2 = st.columns(2)
        try:
            cur.execute("SHOW STAGES IN ACCOUNT")
            stage_rows = cur.fetchall()
            stage_name_idx = next(i for i, d in enumerate(cur.description) if d[0] == "name")
            stage_db_idx = next(i for i, d in enumerate(cur.description) if d[0] == "database_name")
            stage_schema_idx = next(i for i, d in enumerate(cur.description) if d[0] == "schema_name")
            available_stages = [
                f"{r[stage_db_idx]}.{r[stage_schema_idx]}.{r[stage_name_idx]}"
                for r in stage_rows
            ]
        except Exception:
            available_stages = []

        if available_stages:
            stage_options = ["-- Type manually --"] + available_stages
            default_idx = 0
            if editing and editing.get("STAGE_NAME"):
                try:
                    default_idx = stage_options.index(editing["STAGE_NAME"])
                except ValueError:
                    default_idx = 0
            stage_sel = s1.selectbox("Stage Name *", stage_options,
                                     index=default_idx, key="fi_stage_sel")
            if stage_sel == "-- Type manually --":
                stage_name = s1.text_input("Stage FQN",
                                           value=editing.get("STAGE_NAME", "") if editing else "",
                                           placeholder="DB.SCHEMA.STAGE", key="fi_stage_manual")
            else:
                stage_name = stage_sel
        else:
            stage_name = s1.text_input(
                "Stage Name (FQN) *",
                value=editing.get("STAGE_NAME", "") if editing else "",
                placeholder="ANALYTICS.RAW.S3_STAGE", key="fi_stage_name")

        # Auto-detect cloud provider from stage URL
        cloud_provider = "INTERNAL"
        if stage_name and isinstance(stage_name, str) and stage_name.strip():
            try:
                cur.execute(f"DESCRIBE STAGE {stage_name}")
                desc_rows = cur.fetchall()
                # Check all columns in all rows for cloud URL patterns
                for row in desc_rows:
                    row_text = " ".join(str(c).lower() for c in row if c)
                    if "s3://" in row_text:
                        cloud_provider = "S3"
                        break
                    elif "azure://" in row_text or "blob.core.windows.net" in row_text:
                        cloud_provider = "AZURE"
                        break
                    elif "gcs://" in row_text or "storage.googleapis.com" in row_text:
                        cloud_provider = "GCS"
                        break
            except Exception:
                pass
        provider_colors = {"S3": "#FF9900", "AZURE": "#0078D4", "GCS": "#4285F4", "INTERNAL": "#34D058"}
        s2.markdown(f"<div style='height:28px'></div>", unsafe_allow_html=True)
        s2.markdown(
            f'<div style="background:#0F1B2D;border:1px solid #263245;border-radius:6px;'
            f'padding:8px 14px;text-align:center;">'
            f'<span style="font-size:.68rem;letter-spacing:1px;text-transform:uppercase;'
            f'color:#7E96B0;">Provider</span><br>'
            f'<span style="font-size:.95rem;font-weight:700;color:{provider_colors.get(cloud_provider, "#F0F4F8")}">'
            f'{cloud_provider}</span></div>',
            unsafe_allow_html=True)

        s3, s4 = st.columns(2)
        cloud_path = s3.text_input(
            "Cloud Path (subfolder)",
            value=editing.get("CLOUD_PATH", "") if editing else "",
            placeholder="data-feeds/sales/", key="fi_cloud_path")
        file_pattern = s4.text_input(
            "File Pattern (regex) *",
            value=editing.get("FILE_PATTERN", "") if editing else "",
            placeholder=r".*sales.*\.csv", key="fi_file_pattern")

        s5, s6 = st.columns(2)
        file_type = s5.selectbox(
            "File Type",
            ["CSV", "PARQUET", "JSON", "AVRO"],
            index=["CSV", "PARQUET", "JSON", "AVRO"].index(
                editing.get("FILE_TYPE", "CSV")) if editing else 0,
            key="fi_file_type")
        s6.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
        date_partition = s6.checkbox(
            "Date-partitioned path",
            value=editing.get("DATE_PARTITION", False) if editing else False,
            key="fi_date_part")

        date_format = ""
        if date_partition:
            from datetime import datetime as _dt
            _today = _dt.now()
            date_format_options = [
                ("%Y%m%d", f"%Y%m%d → {_today.strftime('%Y%m%d')}"),
                ("%Y/%m/%d", f"%Y/%m/%d → {_today.strftime('%Y/%m/%d')}"),
                ("%Y-%m-%d", f"%Y-%m-%d → {_today.strftime('%Y-%m-%d')}"),
                ("%Y%m%d/%H", f"%Y%m%d/%H → {_today.strftime('%Y%m%d/%H')}"),
                ("year=%Y/month=%m/day=%d", f"year=%Y/month=%m/day=%d → {_today.strftime('year=%Y/month=%m/day=%d')}"),
                ("%Y/%m", f"%Y/%m → {_today.strftime('%Y/%m')}"),
            ]
            fmt_values = [f[0] for f in date_format_options]
            fmt_labels = [f[1] for f in date_format_options]

            default_fmt = editing.get("DATE_FORMAT", "%Y%m%d") if editing else "%Y%m%d"
            if default_fmt in fmt_values:
                fmt_idx = fmt_values.index(default_fmt)
            else:
                fmt_idx = 0

            date_format = st.selectbox(
                "Date Format",
                fmt_values,
                index=fmt_idx,
                format_func=lambda x: fmt_labels[fmt_values.index(x)],
                key="fi_date_fmt")

        section_card_end()

        # ═══════════════════════════════════════════════════════════════════════
        # Section 3: TARGET
        # ═══════════════════════════════════════════════════════════════════════
        section_card_start("Target (Snowflake)", "🎯", border_color=ST_SUCCESS)

        # Fetch available databases
        try:
            cur.execute("SHOW DATABASES")
            db_rows = cur.fetchall()
            db_name_idx = next(i for i, d in enumerate(cur.description) if d[0] == "name")
            available_dbs = [r[db_name_idx] for r in db_rows]
        except Exception:
            available_dbs = []

        t1, t2, t3 = st.columns(3)

        # Database picker
        if available_dbs:
            db_options = available_dbs
            default_db = editing.get("TARGET_DB", "HISTLOAD_DB") if editing else "HISTLOAD_DB"
            db_idx = db_options.index(default_db) if default_db in db_options else 0
            target_db = t1.selectbox("Target Database *", db_options,
                                     index=db_idx, key="fi_tgt_db")
        else:
            target_db = t1.text_input("Target Database *",
                                      value=editing.get("TARGET_DB", "") if editing else "",
                                      key="fi_tgt_db_txt")

        # Schema picker (dynamic based on selected DB)
        try:
            cur.execute(f"SHOW SCHEMAS IN DATABASE {target_db}")
            sch_rows = cur.fetchall()
            sch_name_idx = next(i for i, d in enumerate(cur.description) if d[0] == "name")
            available_schemas = [r[sch_name_idx] for r in sch_rows
                                 if r[sch_name_idx] not in ("INFORMATION_SCHEMA",)]
        except Exception:
            available_schemas = []

        if available_schemas:
            sch_options = available_schemas + ["-- New Schema --"]
            default_sch = editing.get("TARGET_SCHEMA", "RAW") if editing else "RAW"
            sch_idx = sch_options.index(default_sch) if default_sch in sch_options else 0
            target_schema_sel = t2.selectbox("Target Schema", sch_options,
                                             index=sch_idx, key="fi_tgt_schema")
            if target_schema_sel == "-- New Schema --":
                target_schema = t2.text_input("New Schema Name",
                                              placeholder="RAW", key="fi_tgt_schema_new")
            else:
                target_schema = target_schema_sel
        else:
            target_schema = t2.text_input("Target Schema",
                                          value=editing.get("TARGET_SCHEMA", "RAW") if editing else "RAW",
                                          key="fi_tgt_schema_txt")

        # Table picker (dynamic based on selected DB.Schema)
        try:
            cur.execute(f"SHOW TABLES IN {target_db}.{target_schema}")
            tbl_rows = cur.fetchall()
            tbl_name_idx = next(i for i, d in enumerate(cur.description) if d[0] == "name")
            available_tables = [r[tbl_name_idx] for r in tbl_rows]
        except Exception:
            available_tables = []

        if available_tables:
            tbl_options = ["-- New Table --"] + available_tables
            default_tbl = editing.get("TARGET_TABLE", "") if editing else ""
            tbl_idx = tbl_options.index(default_tbl) if default_tbl in tbl_options else 0
            target_table_sel = t3.selectbox("Target Table *", tbl_options,
                                            index=tbl_idx, key="fi_tgt_table")
            if target_table_sel == "-- New Table --":
                target_table = t3.text_input("New Table Name",
                                             placeholder="DAILY_SALES", key="fi_tgt_table_new")
                table_exists = False
            else:
                target_table = target_table_sel
                table_exists = True
        else:
            target_table = t3.text_input("Target Table *",
                                         value=editing.get("TARGET_TABLE", "") if editing else "",
                                         placeholder="DAILY_SALES", key="fi_tgt_table_txt")
            table_exists = False

        # Load mode + auto-create
        m1, m2 = st.columns(2)
        load_mode = m1.selectbox(
            "Load Mode",
            ["APPEND", "OVERWRITE", "MERGE"],
            index=["APPEND", "OVERWRITE", "MERGE"].index(
                editing.get("LOAD_MODE", "APPEND")) if editing else 0,
            key="fi_load_mode")
        m2.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
        auto_create = m2.checkbox(
            "Auto-create table (if not exists)",
            value=not table_exists,
            key="fi_auto_create")

        # Merge keys (shown only when MERGE selected)
        merge_keys_input = ""
        if load_mode == "MERGE":
            merge_keys_input = st.text_input(
                "Merge Keys (comma-separated) *",
                value=editing.get("MERGE_KEYS", "") if editing else "",
                placeholder="e.g. ID, CUSTOMER_ID",
                help="Primary key column(s) for MERGE deduplication",
                key="fi_merge_keys")

        # Schema info card (when existing table selected)
        if table_exists and target_table:
            try:
                tgt_fqn = f"{target_db}.{target_schema}.{target_table}"
                cur.execute(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS "
                            f"WHERE TABLE_CATALOG='{target_db}' AND TABLE_SCHEMA='{target_schema}' "
                            f"AND TABLE_NAME='{target_table}'")
                col_count = cur.fetchone()[0] or 0
                info_box(f"Table `{tgt_fqn}` exists — {col_count} columns", "✅")
            except Exception:
                pass

        section_card_end()

        # ═══════════════════════════════════════════════════════════════════════
        # Section 4: ADVANCED OPTIONS (collapsed)
        # ═══════════════════════════════════════════════════════════════════════
        with st.expander("⚙️ Advanced Options", expanded=False):
            # CSV Format options
            if file_type == "CSV":
                st.markdown(
                    f'<div style="font-size:.7rem;letter-spacing:1.5px;text-transform:uppercase;'
                    f'color:{TXT_LABEL};font-weight:600;margin-bottom:8px;">CSV FORMAT</div>',
                    unsafe_allow_html=True)
                f1, f2, f3, f4 = st.columns(4)
                delimiter = f1.text_input("Delimiter",
                                          value=editing.get("FIELD_DELIMITER", ",") if editing else ",",
                                          key="fi_delim")
                enclosed = f2.text_input("Enclosed By",
                                         value=editing.get("FIELD_ENCLOSED_BY", '"') if editing else '"',
                                         key="fi_enclosed")
                escape = f3.text_input("Escape Char",
                                       value=editing.get("ESCAPE_CHARACTER", "\\") if editing else "\\",
                                       key="fi_escape")
                skip_header = f4.number_input("Skip Header",
                                              value=int(editing.get("SKIP_HEADER", 1)) if editing else 1,
                                              min_value=0, max_value=10, key="fi_skip")
            else:
                delimiter = ","
                enclosed = '"'
                escape = "\\"
                skip_header = 0

            # COPY INTO options
            st.markdown(
                f'<div style="font-size:.7rem;letter-spacing:1.5px;text-transform:uppercase;'
                f'color:{TXT_LABEL};font-weight:600;margin:12px 0 8px 0;">COPY OPTIONS</div>',
                unsafe_allow_html=True)
            o1, o2 = st.columns(2)
            on_error = o1.selectbox(
                "ON_ERROR",
                ["ABORT_STATEMENT", "CONTINUE", "SKIP_FILE"],
                index=["ABORT_STATEMENT", "CONTINUE", "SKIP_FILE"].index(
                    editing.get("ON_ERROR", "ABORT_STATEMENT")) if editing else 0,
                key="fi_on_error")
            match_by = o2.selectbox(
                "MATCH_BY_COLUMN_NAME",
                ["", "CASE_INSENSITIVE", "CASE_SENSITIVE"],
                index=["", "CASE_INSENSITIVE", "CASE_SENSITIVE"].index(
                    editing.get("MATCH_BY_COLUMN_NAME") or "") if editing else 0,
                key="fi_match_by")
            purge = st.checkbox(
                "Purge files after load",
                value=editing.get("PURGE_FILES", False) if editing else False,
                key="fi_purge")

        # ═══════════════════════════════════════════════════════════════════════
        # Save Button
        # ═══════════════════════════════════════════════════════════════════════
        st.markdown('<div style="margin-top:12px;"></div>', unsafe_allow_html=True)
        if st.button("💾 Save Job" if not editing else "💾 Update Job",
                     type="primary", use_container_width=True, key="fi_save_btn"):
            # Validation
            errors = []
            if not job_name.strip():
                errors.append("Job Name is required")
            if not stage_name or (isinstance(stage_name, str) and not stage_name.strip()):
                errors.append("Stage Name is required")
            if not file_pattern.strip():
                errors.append("File Pattern is required")
            if not target_db or (isinstance(target_db, str) and not target_db.strip()):
                errors.append("Target Database is required")
            if not target_table or (isinstance(target_table, str) and not target_table.strip()):
                errors.append("Target Table is required")
            if load_mode == "MERGE" and not merge_keys_input.strip():
                errors.append("MERGE mode requires Merge Keys")

            if errors:
                for err in errors:
                    st.error(f"❌ {err}")
            else:
                save_data = {
                    "JOB_NAME": job_name.strip(),
                    "ACTIVE": active,
                    "STAGE_NAME": stage_name.strip() if isinstance(stage_name, str) else stage_name,
                    "CLOUD_PATH": cloud_path.strip(),
                    "FILE_PATTERN": file_pattern.strip(),
                    "FILE_TYPE": file_type,
                    "TARGET_DB": target_db.strip().upper() if isinstance(target_db, str) else target_db,
                    "TARGET_SCHEMA": (target_schema.strip().upper() if isinstance(target_schema, str) else target_schema) or "RAW",
                    "TARGET_TABLE": target_table.strip().upper() if isinstance(target_table, str) else target_table,
                    "LOAD_MODE": load_mode,
                    "TABLE_EXISTS": not auto_create,
                    "FIELD_DELIMITER": delimiter,
                    "FIELD_ENCLOSED_BY": enclosed,
                    "ESCAPE_CHARACTER": escape,
                    "SKIP_HEADER": int(skip_header),
                    "ON_ERROR": on_error,
                    "MATCH_BY_COLUMN_NAME": match_by or None,
                    "PURGE_FILES": purge,
                    "DATE_PARTITION": date_partition,
                    "DATE_FORMAT": date_format.strip() or None,
                    "MERGE_KEYS": merge_keys_input.strip() or None,
                }
                if editing:
                    save_data["CONFIG_ID"] = editing["CONFIG_ID"]
                file_ingest_config.upsert(cur, save_data)
                conn.commit()
                st.success(f"✅ Job '{job_name}' saved successfully!")
                st.rerun()


    # ── Tab 3: Upload & Ingest ─────────────────────────────────────────────────
    with tab_upload:
        st.markdown("#### Upload Files to Snowflake Stage")
        st.caption("Upload local files directly into a Snowflake internal stage, "
                   "then load them into a target table. No cloud storage needed.")

        _UPLOAD_STAGE = "HISTLOAD_DB.META.DMT_UPLOAD_STAGE"

        # Ensure stage exists
        try:
            cur.execute(f"CREATE STAGE IF NOT EXISTS {_UPLOAD_STAGE}")
        except Exception:
            pass

        # File uploader
        uploaded_files = st.file_uploader(
            "Drop files here (max 200MB per file)",
            type=["csv", "tsv", "parquet", "json", "avro", "txt", "gz"],
            accept_multiple_files=True,
            key="fi_upload")

        if uploaded_files:
            st.info(f"{len(uploaded_files)} file(s) selected: "
                    f"{', '.join(f.name for f in uploaded_files[:5])}"
                    f"{'...' if len(uploaded_files) > 5 else ''}")

            # ── Auto-detect file type from extension ─────────────────────────
            first_file = uploaded_files[0]
            ext = first_file.name.rsplit(".", 1)[-1].lower() if "." in first_file.name else ""
            # Handle .gz (look at second extension)
            if ext == "gz" and first_file.name.count(".") >= 2:
                ext = first_file.name.rsplit(".", 2)[-2].lower()

            auto_type_map = {"csv": "CSV", "tsv": "CSV", "txt": "CSV",
                             "parquet": "PARQUET", "json": "JSON", "ndjson": "JSON",
                             "avro": "AVRO"}
            detected_type = auto_type_map.get(ext, "CSV")

            # ── Auto-suggest table name from filename ────────────────────────
            import re
            base_name = first_file.name.rsplit(".", 1)[0] if "." in first_file.name else first_file.name
            # Remove date patterns, numbers, clean to valid identifier
            suggested_table = re.sub(r'[\d_-]{6,}', '', base_name)  # strip date-like
            suggested_table = re.sub(r'[^a-zA-Z0-9_]', '_', suggested_table)  # clean
            suggested_table = re.sub(r'_+', '_', suggested_table).strip('_').upper()
            if not suggested_table:
                suggested_table = "UPLOADED_DATA"

            # ── File Preview ─────────────────────────────────────────────────
            st.markdown("---")
            st.markdown("**File Preview** (first 5 rows)")
            try:
                if detected_type == "CSV":
                    import io
                    content = first_file.getvalue().decode("utf-8", errors="replace")
                    preview_df = pd.read_csv(io.StringIO(content), nrows=5)
                    st.dataframe(preview_df, use_container_width=True, hide_index=True)
                    st.caption(f"Detected: **{len(preview_df.columns)} columns**, "
                               f"delimiter=`,` — {first_file.name}")
                elif detected_type == "PARQUET":
                    import pyarrow.parquet as pq
                    import io
                    pf = pq.read_table(io.BytesIO(first_file.getvalue())).to_pandas()
                    st.dataframe(pf.head(5), use_container_width=True, hide_index=True)
                    st.caption(f"Detected: **{len(pf.columns)} columns** — {first_file.name}")
                elif detected_type == "JSON":
                    import io, json as _json
                    content = first_file.getvalue().decode("utf-8", errors="replace")
                    # Try array or newline-delimited
                    try:
                        data = _json.loads(content)
                        if isinstance(data, list):
                            preview_df = pd.DataFrame(data[:5])
                        else:
                            preview_df = pd.DataFrame([data])
                    except _json.JSONDecodeError:
                        lines = [_json.loads(l) for l in content.strip().split("\n")[:5] if l.strip()]
                        preview_df = pd.DataFrame(lines)
                    st.dataframe(preview_df, use_container_width=True, hide_index=True)
                    st.caption(f"Detected: **{len(preview_df.columns)} columns** — {first_file.name}")
                else:
                    st.caption("Preview not available for Avro files")
            except Exception as prev_err:
                st.warning(f"Preview failed: {prev_err}")

            # ── Target config ────────────────────────────────────────────────
            st.markdown("---")
            st.markdown("**Load Settings**")
            u1, u2, u3 = st.columns(3)
            upload_target_db = u1.text_input("Target Database",
                                            value="HISTLOAD_DB", key="up_tgt_db")
            upload_target_schema = u2.text_input("Target Schema",
                                                value="RAW", key="up_tgt_schema")
            upload_target_table = u3.text_input("Target Table",
                                               value=suggested_table,
                                               key="up_tgt_table")

            u4, u5, u6 = st.columns(3)
            type_options = ["CSV", "PARQUET", "JSON", "AVRO"]
            upload_file_type = u4.selectbox("File Type", type_options,
                                           index=type_options.index(detected_type),
                                           key="up_ftype")
            upload_load_mode = u5.selectbox("Load Mode",
                                           ["APPEND", "OVERWRITE"], key="up_lmode")
            upload_auto_create = u6.checkbox("Auto-create table",
                                            value=True, key="up_auto_create")

            # Schema preview for existing tables
            if not upload_auto_create and upload_target_table.strip():
                try:
                    tgt_fqn = (f"{upload_target_db.strip().upper()}."
                               f"{upload_target_schema.strip().upper()}."
                               f"{upload_target_table.strip().upper()}")
                    cur.execute(f"SHOW COLUMNS IN TABLE {tgt_fqn}")
                    tgt_cols = cur.fetchall()
                    if tgt_cols:
                        col_name_idx = next(i for i, d in enumerate(cur.description)
                                           if d[0] == "column_name")
                        col_type_idx = next(i for i, d in enumerate(cur.description)
                                           if d[0] == "data_type")
                        st.markdown("**Target Table Schema:**")
                        schema_data = [{"Column": r[col_name_idx].strip('"'),
                                       "Type": r[col_type_idx]}
                                      for r in tgt_cols]
                        st.dataframe(pd.DataFrame(schema_data),
                                    use_container_width=True, hide_index=True)
                except Exception:
                    pass

            # CSV options (shown only for CSV)
            if upload_file_type == "CSV":
                uc1, uc2, uc3 = st.columns(3)
                up_delimiter = uc1.text_input("Delimiter", value=",", key="up_delim")
                up_enclosed = uc2.text_input("Enclosed By", value='"', key="up_enc")
                up_skip = uc3.number_input("Skip Header", value=1, min_value=0,
                                           max_value=10, key="up_skip")
            else:
                up_delimiter = ","
                up_enclosed = '"'
                up_skip = 0

            # Upload & Load button
            st.markdown("---")
            if st.button("Upload & Load", type="primary",
                         use_container_width=True, key="up_go"):
                if not upload_target_table.strip():
                    st.error("Target Table is required.")
                else:
                    import tempfile
                    import os

                    upload_path = f"upload/{upload_target_table.strip().lower()}"
                    loaded_files = []
                    progress = st.progress(0, text="Uploading files...")

                    for idx, uf in enumerate(uploaded_files):
                        # Write to temp file, then PUT to stage
                        with tempfile.NamedTemporaryFile(
                                delete=False, suffix=f"_{uf.name}") as tmp:
                            tmp.write(uf.getbuffer())
                            tmp_path = tmp.name

                        try:
                            # Normalize path for Windows: backslashes → forward slashes
                            normalized_path = tmp_path.replace("\\", "/")
                            put_sql = (
                                f"PUT 'file://{normalized_path}' "
                                f"@{_UPLOAD_STAGE}/{upload_path}/ "
                                f"AUTO_COMPRESS = FALSE OVERWRITE = TRUE")
                            cur.execute(put_sql)
                            loaded_files.append(uf.name)
                        except Exception as e:
                            st.error(f"PUT failed for {uf.name}: {e}")
                        finally:
                            os.unlink(tmp_path)

                        progress.progress((idx + 1) / len(uploaded_files),
                                         text=f"Uploaded {idx + 1}/{len(uploaded_files)}: {uf.name}")

                    progress.empty()

                    if loaded_files:
                        st.success(f"Uploaded {len(loaded_files)} file(s) to stage")

                        # Build file extension pattern
                        ftype = upload_file_type.lower()
                        if ftype == "csv":
                            pattern = ".*\\.(csv|tsv|txt).*"
                        elif ftype == "parquet":
                            pattern = ".*\\.parquet.*"
                        elif ftype == "json":
                            pattern = ".*\\.(json|ndjson).*"
                        else:
                            pattern = ".*\\.avro.*"

                        # Run ingestion
                        with st.spinner("Loading into Snowflake..."):
                            from core.file_ingester import run_ingestion
                            ingest_config = {
                                "CONFIG_ID": f"upload_{upload_target_table.strip().lower()}",
                                "JOB_NAME": f"upload_{upload_target_table.strip().lower()}",
                                "STAGE_NAME": _UPLOAD_STAGE,
                                "CLOUD_PATH": upload_path,
                                "FILE_PATTERN": pattern,
                                "FILE_TYPE": upload_file_type,
                                "TARGET_DB": upload_target_db.strip().upper(),
                                "TARGET_SCHEMA": upload_target_schema.strip().upper(),
                                "TARGET_TABLE": upload_target_table.strip().upper(),
                                "TABLE_EXISTS": not upload_auto_create,
                                "LOAD_MODE": upload_load_mode,
                                "FIELD_DELIMITER": up_delimiter,
                                "FIELD_ENCLOSED_BY": up_enclosed,
                                "SKIP_HEADER": int(up_skip),
                                "ON_ERROR": "ABORT_STATEMENT",
                            }
                            result = run_ingestion(conn, ingest_config,
                                                   skip_config_update=True)

                        if result["STATUS"] == "success":
                            st.success(
                                f"Loaded **{int(result.get('ROWS_LOADED') or 0):,}** rows "
                                f"from {int(result.get('FILES_LOADED') or 0)} file(s) into "
                                f"`{upload_target_db.strip().upper()}."
                                f"{upload_target_schema.strip().upper()}."
                                f"{upload_target_table.strip().upper()}`")
                        elif result["STATUS"] == "skipped":
                            st.warning(f"Skipped: {result.get('ERROR_MESSAGE', '?')}")
                        else:
                            st.error(f"Failed: {result.get('ERROR_MESSAGE', '?')}")

        else:
            st.markdown(
                '<div style="text-align:center;padding:40px;color:#7E96B0;">'
                '<div style="font-size:3rem;">📁</div>'
                '<div style="margin-top:8px;">Drop CSV, Parquet, JSON, or Avro files above</div>'
                '<div style="font-size:0.8rem;margin-top:4px;">'
                'Files are uploaded to an internal Snowflake stage — no cloud storage needed'
                '</div></div>', unsafe_allow_html=True)

    # ── Tab 4: Stages & Integrations ────────────────────────────────────────────
    with tab_stages:
        from utils.ui_theme import section_card_start as _scs, section_card_end as _sce

        st.markdown("#### Stages & Storage Integrations")
        st.caption("View existing integrations and create external stages for file ingestion.")

        # ── Storage Integrations (read-only) ─────────────────────────────────
        _scs("Storage Integrations", "🔗", border_color="#a78bfa")
        st.caption("Storage integrations are created in Snowflake directly "
                   "(requires ACCOUNTADMIN + cloud IAM setup).")
        try:
            cur.execute("SHOW STORAGE INTEGRATIONS")
            si_rows = cur.fetchall()
            si_cols = [d[0] for d in cur.description]
            if si_rows:
                si_df = pd.DataFrame(si_rows, columns=si_cols)
                display_si = [c for c in ["name", "type", "enabled", "comment"]
                              if c in si_df.columns]
                st.dataframe(si_df[display_si] if display_si else si_df,
                             use_container_width=True, hide_index=True)
                st.caption(f"{len(si_rows)} storage integration(s) found")
            else:
                st.info("No storage integrations found. Create one in Snowflake worksheet first.")
        except Exception as e:
            st.warning(f"Cannot list storage integrations: {e}")
        _sce()

        # ── Existing Stages ──────────────────────────────────────────────────
        _scs("Existing Stages", "📦", border_color="#58A6FF")
        try:
            cur.execute("SHOW STAGES IN ACCOUNT")
            stg_rows = cur.fetchall()
            stg_cols = [d[0] for d in cur.description]
            if stg_rows:
                stg_df = pd.DataFrame(stg_rows, columns=stg_cols)
                display_stg = [c for c in ["name", "database_name", "schema_name",
                                           "url", "type", "cloud", "owner"]
                               if c in stg_df.columns]

                STG_PER_PAGE = 15
                total_stg = len(stg_df)
                total_stg_pages = max(1, (total_stg + STG_PER_PAGE - 1) // STG_PER_PAGE)
                stg_page = st.session_state.get("fi_stg_page", 0)
                stg_page = min(stg_page, total_stg_pages - 1)
                start_s = stg_page * STG_PER_PAGE
                end_s = min(start_s + STG_PER_PAGE, total_stg)

                st.dataframe(
                    (stg_df[display_stg] if display_stg else stg_df).iloc[start_s:end_s],
                    use_container_width=True, hide_index=True)
                st.caption(f"Showing {start_s + 1}–{end_s} of {total_stg} stage(s)")

                if total_stg_pages > 1:
                    sp1, sp2, sp3 = st.columns([1, 2, 1])
                    sp1.button("◀ Prev", key="fi_stg_prev",
                               disabled=(stg_page == 0),
                               on_click=lambda: st.session_state.update(fi_stg_page=stg_page - 1))
                    sp2.markdown(
                        f"<div style='text-align:center;padding-top:6px;color:{TXT_LABEL}'>"
                        f"Page {stg_page + 1} of {total_stg_pages}</div>",
                        unsafe_allow_html=True)
                    sp3.button("Next ▶", key="fi_stg_next",
                               disabled=(stg_page >= total_stg_pages - 1),
                               on_click=lambda: st.session_state.update(fi_stg_page=stg_page + 1))
            else:
                st.info("No stages found in this account.")
        except Exception as e:
            st.warning(f"Cannot list stages: {e}")
        _sce()

        # ── Create External Stage ────────────────────────────────────────────
        _scs("Create External Stage", "➕", border_color=ST_SUCCESS)

        # Auth method selection
        auth_method = st.radio(
            "Authentication Method",
            ["Storage Integration", "Direct Credentials (AWS Keys / Azure SAS)"],
            horizontal=True, key="fi_stg_auth_method")

        # Common fields
        cs1, cs2, cs3 = st.columns(3)
        new_stg_db = cs1.text_input("Database *", value="HISTLOAD_DB", key="fi_new_stg_db")
        new_stg_schema = cs2.text_input("Schema *", value="META", key="fi_new_stg_schema")
        new_stg_name = cs3.text_input("Stage Name *", placeholder="MY_EXT_STAGE",
                                      key="fi_new_stg_name")

        if auth_method == "Storage Integration":
            # ── Option 1: Using Storage Integration ───────────────────────────
            st.caption("Uses a pre-configured storage integration (recommended for production).")

            try:
                cur.execute("SHOW STORAGE INTEGRATIONS")
                si_rows2 = cur.fetchall()
                si_name_idx = next(i for i, d in enumerate(cur.description) if d[0] == "name")
                integrations = [r[si_name_idx] for r in si_rows2]
            except Exception:
                integrations = []

            cs4, cs5 = st.columns(2)
            if integrations:
                sel_integration = cs4.selectbox("Storage Integration *", integrations,
                                               key="fi_stg_integration")
            else:
                sel_integration = cs4.text_input("Storage Integration *",
                                                placeholder="my_s3_integration",
                                                key="fi_stg_int_txt")
            stg_url = cs5.text_input("Stage URL *",
                                     placeholder="s3://bucket/path/ or azure://account.blob.core.windows.net/container/",
                                     key="fi_stg_url_int")

            if st.button("Create Stage (Integration)", type="primary",
                         use_container_width=True, key="fi_create_stg_int"):
                if not new_stg_name.strip() or not stg_url.strip() or not sel_integration:
                    st.error("All fields are required.")
                else:
                    fqn = f"{new_stg_db.strip()}.{new_stg_schema.strip()}.{new_stg_name.strip().upper()}"
                    ddl = (
                        f"CREATE STAGE IF NOT EXISTS {fqn}\n"
                        f"  STORAGE_INTEGRATION = {sel_integration}\n"
                        f"  URL = '{stg_url.strip()}';"
                    )
                    st.code(ddl, language="sql")
                    try:
                        cur.execute(f"CREATE DATABASE IF NOT EXISTS {new_stg_db.strip()}")
                        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {new_stg_db.strip()}.{new_stg_schema.strip()}")
                        cur.execute(ddl)
                        conn.commit()
                        st.success(f"Stage `{fqn}` created!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed: {e}")

        else:
            # ── Option 2: Direct Credentials ─────────────────────────────────
            st.caption("Uses access keys or SAS tokens directly (simpler setup, less secure).")

            cloud_type = st.selectbox("Cloud Provider", ["AWS S3", "Azure Blob"],
                                     key="fi_stg_direct_cloud")

            if cloud_type == "AWS S3":
                dc1, dc2 = st.columns(2)
                stg_url_direct = dc1.text_input("S3 URL *",
                                                placeholder="s3://my-bucket/path/",
                                                key="fi_stg_s3_url")
                aws_key = dc2.text_input("AWS Key ID *",
                                         placeholder="AKIAIOSFODNN7EXAMPLE",
                                         key="fi_stg_aws_key")
                dc3, dc4 = st.columns(2)
                aws_secret = dc3.text_input("AWS Secret Key *",
                                            type="password",
                                            key="fi_stg_aws_secret")
                aws_token = dc4.text_input("AWS Session Token (optional)",
                                           type="password",
                                           key="fi_stg_aws_token",
                                           help="Only for temporary credentials")

                if st.button("Create Stage (AWS Direct)", type="primary",
                             use_container_width=True, key="fi_create_stg_aws"):
                    if not new_stg_name.strip() or not stg_url_direct.strip() or not aws_key.strip() or not aws_secret.strip():
                        st.error("Stage Name, URL, Key ID, and Secret Key are required.")
                    else:
                        fqn = f"{new_stg_db.strip()}.{new_stg_schema.strip()}.{new_stg_name.strip().upper()}"
                        cred_block = (
                            f"  CREDENTIALS = (\n"
                            f"    AWS_KEY_ID = '{aws_key.strip()}'\n"
                            f"    AWS_SECRET_KEY = '{aws_secret.strip()}'"
                        )
                        if aws_token.strip():
                            cred_block += f"\n    AWS_TOKEN = '{aws_token.strip()}'"
                        cred_block += "\n  )"

                        ddl = (
                            f"CREATE STAGE IF NOT EXISTS {fqn}\n"
                            f"  URL = '{stg_url_direct.strip()}'\n"
                            f"{cred_block};"
                        )
                        st.code("-- DDL generated (credentials masked in display)\n"
                                f"CREATE STAGE IF NOT EXISTS {fqn}\n"
                                f"  URL = '{stg_url_direct.strip()}'\n"
                                f"  CREDENTIALS = (AWS_KEY_ID = '***' AWS_SECRET_KEY = '***');",
                                language="sql")
                        try:
                            cur.execute(f"CREATE DATABASE IF NOT EXISTS {new_stg_db.strip()}")
                            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {new_stg_db.strip()}.{new_stg_schema.strip()}")
                            cur.execute(ddl)
                            conn.commit()
                            st.success(f"Stage `{fqn}` created with AWS credentials!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Failed: {e}")

            else:  # Azure Blob
                dc1, dc2 = st.columns(2)
                stg_url_direct = dc1.text_input("Azure URL *",
                                                placeholder="azure://account.blob.core.windows.net/container/path/",
                                                key="fi_stg_az_url")
                az_sas = dc2.text_input("SAS Token *",
                                        type="password",
                                        placeholder="?sv=2021-06-08&ss=b&srt=sco&sp=rl...",
                                        key="fi_stg_az_sas")

                if st.button("Create Stage (Azure Direct)", type="primary",
                             use_container_width=True, key="fi_create_stg_az"):
                    if not new_stg_name.strip() or not stg_url_direct.strip() or not az_sas.strip():
                        st.error("Stage Name, Azure URL, and SAS Token are required.")
                    else:
                        fqn = f"{new_stg_db.strip()}.{new_stg_schema.strip()}.{new_stg_name.strip().upper()}"
                        # Strip leading ? from SAS if user included it
                        sas_clean = az_sas.strip().lstrip("?")
                        ddl = (
                            f"CREATE STAGE IF NOT EXISTS {fqn}\n"
                            f"  URL = '{stg_url_direct.strip()}'\n"
                            f"  CREDENTIALS = (AZURE_SAS_TOKEN = '?{sas_clean}');"
                        )
                        st.code("-- DDL generated (credentials masked in display)\n"
                                f"CREATE STAGE IF NOT EXISTS {fqn}\n"
                                f"  URL = '{stg_url_direct.strip()}'\n"
                                f"  CREDENTIALS = (AZURE_SAS_TOKEN = '***');",
                                language="sql")
                        try:
                            cur.execute(f"CREATE DATABASE IF NOT EXISTS {new_stg_db.strip()}")
                            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {new_stg_db.strip()}.{new_stg_schema.strip()}")
                            cur.execute(ddl)
                            conn.commit()
                            st.success(f"Stage `{fqn}` created with Azure SAS token!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Failed: {e}")

        _sce()

    # ── Tab 5: Run History ────────────────────────────────────────────────────
    with tab_history:
        logs = file_ingest_config.get_recent_logs(cur, limit=100)

        if not logs:
            empty_state("", "No Run History",
                        "Run an ingestion job to see results here.")
        else:
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

            HIST_PER_PAGE = 15
            total_hist = len(df)
            total_hist_pages = max(1, (total_hist + HIST_PER_PAGE - 1) // HIST_PER_PAGE)
            hist_page = st.session_state.get("fi_hist_page", 0)
            hist_page = min(hist_page, total_hist_pages - 1)
            start_h = hist_page * HIST_PER_PAGE
            end_h = min(start_h + HIST_PER_PAGE, total_hist)

            st.dataframe(df[available].iloc[start_h:end_h],
                         use_container_width=True, hide_index=True)

            if total_hist_pages > 1:
                hp1, hp2, hp3 = st.columns([1, 2, 1])
                hp1.button("◀ Prev", key="fi_hist_prev",
                           disabled=(hist_page == 0),
                           on_click=lambda: st.session_state.update(fi_hist_page=hist_page - 1))
                hp2.markdown(
                    f"<div style='text-align:center;padding-top:6px;color:{TXT_LABEL}'>"
                    f"Page {hist_page + 1} of {total_hist_pages} "
                    f"({total_hist} runs)</div>",
                    unsafe_allow_html=True)
                hp3.button("Next ▶", key="fi_hist_next",
                           disabled=(hist_page >= total_hist_pages - 1),
                           on_click=lambda: st.session_state.update(fi_hist_page=hist_page + 1))

    cur.close()
