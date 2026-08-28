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
    tab_jobs, tab_add, tab_upload, tab_history = st.tabs([
        "Active Jobs", "Add / Edit Job", "Upload & Ingest", "Run History"
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
                status_color = (ST_SUCCESS if cfg.get("LAST_RUN_STATUS") == "success"
                                else ST_FAILED if cfg.get("LAST_RUN_STATUS") == "failed"
                                else TXT_LABEL)
                with st.expander(
                    f"{'🟢' if cfg.get('ACTIVE') else '⚫'} **{cfg['JOB_NAME']}** "
                    f"— {cfg.get('FILE_TYPE', '?')} → {cfg.get('TARGET_TABLE', '?')} "
                    f"({cfg.get('LOAD_MODE', 'APPEND')})", expanded=False):
                    c_info, c_actions = st.columns([3, 1])
                    with c_info:
                        st.caption(
                            f"Stage: `{cfg.get('STAGE_NAME', '')}/{cfg.get('CLOUD_PATH', '')}` · "
                            f"Pattern: `{cfg.get('FILE_PATTERN', '')}` · "
                            f"Last: {cfg.get('LAST_ROW_COUNT', 0):,} rows at {cfg.get('LAST_RUN_AT', 'never')}")
                    with c_actions:
                        a1, a2 = st.columns(2)
                        if cfg.get("ACTIVE"):
                            if a1.button("⏸ Deactivate", key=f"deact_{cfg['CONFIG_ID']}",
                                         use_container_width=True):
                                file_ingest_config.deactivate(cur, cfg["CONFIG_ID"])
                                conn.commit()
                                st.rerun()
                        else:
                            if a1.button("▶ Activate", key=f"act_{cfg['CONFIG_ID']}",
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
                "Date-partitioned path",
                value=editing.get("DATE_PARTITION", False) if editing else False)

            # Date format (shown only when date partition is enabled)
            date_format = ""
            if date_partition:
                date_format = st.text_input(
                    "Date Format (Python strftime)",
                    value=editing.get("DATE_FORMAT", "%Y%m%d") if editing else "%Y%m%d",
                    placeholder="%Y%m%d or %Y/%m/%d",
                    help="Format for date subfolder: %Y=year, %m=month, %d=day. "
                         "E.g. %Y%m%d → 20260826, %Y/%m/%d → 2026/08/26")

            # MERGE keys (shown only when load mode is MERGE)
            merge_keys_input = ""

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

            if load_mode == "MERGE":
                merge_keys_input = st.text_input(
                    "Merge Keys (comma-separated) *",
                    value=editing.get("MERGE_KEYS", "") if editing else "",
                    placeholder="e.g. ID, CUSTOMER_ID",
                    help="Primary key column(s) for MERGE deduplication")

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
                elif load_mode == "MERGE" and not merge_keys_input.strip():
                    st.error("MERGE mode requires Merge Keys to be specified.")
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
                        "DATE_FORMAT": date_format.strip() or None,
                        "MERGE_KEYS": merge_keys_input.strip() or None,
                    }
                    if editing:
                        save_data["CONFIG_ID"] = editing["CONFIG_ID"]
                    file_ingest_config.upsert(cur, save_data)
                    conn.commit()
                    st.success(f"Job '{job_name}' saved successfully!")
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
                    content = first_file.read().decode("utf-8", errors="replace")
                    first_file.seek(0)  # reset for later PUT
                    preview_df = pd.read_csv(io.StringIO(content), nrows=5)
                    st.dataframe(preview_df, use_container_width=True, hide_index=True)
                    st.caption(f"Detected: **{len(preview_df.columns)} columns**, "
                               f"delimiter=`,` — {first_file.name}")
                elif detected_type == "PARQUET":
                    import pyarrow.parquet as pq
                    import io
                    pf = pq.read_table(io.BytesIO(first_file.read())).to_pandas()
                    first_file.seek(0)
                    st.dataframe(pf.head(5), use_container_width=True, hide_index=True)
                    st.caption(f"Detected: **{len(pf.columns)} columns** — {first_file.name}")
                elif detected_type == "JSON":
                    import io, json as _json
                    content = first_file.read().decode("utf-8", errors="replace")
                    first_file.seek(0)
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
                            put_sql = (
                                f"PUT 'file://{tmp_path}' "
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
                            result = run_ingestion(conn, ingest_config)

                        if result["STATUS"] == "success":
                            st.success(
                                f"Loaded **{result.get('ROWS_LOADED', 0):,}** rows "
                                f"from {result.get('FILES_LOADED', 0)} file(s) into "
                                f"`{upload_target_db.strip().upper()}."
                                f"{upload_target_schema.strip().upper()}."
                                f"{upload_target_table.strip().upper()}`")
                            st.balloons()
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

    # ── Tab 4: Run History ────────────────────────────────────────────────────
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

            # Paginate history
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
