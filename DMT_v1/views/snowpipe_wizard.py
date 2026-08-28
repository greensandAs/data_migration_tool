# Snowpipe Setup Wizard — guided creation of storage integrations, stages, and pipes.
# Co-authored with CoCo
"""views/snowpipe_wizard.py — Snowpipe Setup Wizard (BETA).

Guided wizard for setting up automated file ingestion via Snowpipe:
  - Create storage integrations (S3, Azure, GCS)
  - Create external/internal stages
  - Generate Snowpipe DDL with auto-ingest
  - Monitor COPY_HISTORY for loaded files
"""
from __future__ import annotations

import streamlit as st
import pandas as pd

from utils.ui_theme import (
    section_card_start, section_card_end, info_box,
    TA_ORANGE, ST_SUCCESS, ST_PENDING, ST_FAILED, TXT_LABEL, TXT_SECONDARY,
    BORDER, TA_NAVY,
)


def render(conn):
    """Main render function for the Snowpipe Wizard page."""
    st.markdown(
        '<div class="section-header">Snowpipe Setup Wizard '
        '<span style="background:#2e1e0d;color:#F0A742;font-size:.6rem;'
        'padding:2px 8px;border-radius:4px;margin-left:8px;letter-spacing:1px;">'
        'BETA</span></div>',
        unsafe_allow_html=True)
    st.caption("Guided setup for automated file ingestion. "
               "Creates the Snowflake objects needed for Snowpipe auto-ingest.")

    cur = conn.cursor()

    tab_integration, tab_stage, tab_pipe, tab_monitor = st.tabs([
        "1. Storage Integration", "2. Create Stage", "3. Generate Snowpipe", "4. Monitor"
    ])

    # ══════════════════════════════════════════════════════════════════════════
    # Tab 1: Storage Integration
    # ══════════════════════════════════════════════════════════════════════════
    with tab_integration:
        section_card_start("Existing Storage Integrations", "🔗", border_color="#a78bfa")
        try:
            cur.execute("SHOW STORAGE INTEGRATIONS")
            si_rows = cur.fetchall()
            si_cols = [d[0] for d in cur.description]
            if si_rows:
                si_df = pd.DataFrame(si_rows, columns=si_cols)
                display_si = [c for c in ["name", "type", "enabled", "comment",
                                          "created_on"] if c in si_df.columns]
                st.dataframe(si_df[display_si] if display_si else si_df,
                             use_container_width=True, hide_index=True)
                st.caption(f"{len(si_rows)} integration(s) found")
            else:
                st.info("No storage integrations found. Create one below.")
        except Exception as e:
            st.warning(f"Cannot list integrations: {e}")
        section_card_end()

        # Create new integration
        section_card_start("Create Storage Integration", "➕", border_color=ST_PENDING)
        st.caption("Creates a storage integration to allow Snowflake access to your cloud bucket.")

        provider = st.selectbox("Cloud Provider", ["AWS S3", "Azure Blob", "GCS"],
                                key="spw_si_provider")

        si_name = st.text_input("Integration Name *",
                                placeholder="my_s3_integration", key="spw_si_name")

        if provider == "AWS S3":
            si_role_arn = st.text_input(
                "Storage AWS Role ARN *",
                placeholder="arn:aws:iam::123456789012:role/snowflake-access",
                key="spw_si_arn")
            si_locations = st.text_input(
                "Allowed Locations (comma-separated) *",
                placeholder="s3://my-bucket/, s3://my-bucket/data/",
                key="spw_si_locations")
            si_blocked = st.text_input(
                "Blocked Locations (optional)",
                placeholder="s3://my-bucket/sensitive/",
                key="spw_si_blocked")

            if st.button("Generate & Execute DDL", type="primary",
                         use_container_width=True, key="spw_si_create_s3"):
                if not si_name.strip() or not si_role_arn.strip() or not si_locations.strip():
                    st.error("Integration Name, Role ARN, and Allowed Locations are required.")
                else:
                    locations = ", ".join(f"'{l.strip()}'" for l in si_locations.split(","))
                    ddl = (
                        f"CREATE STORAGE INTEGRATION {si_name.strip().upper()}\n"
                        f"  TYPE = EXTERNAL_STAGE\n"
                        f"  STORAGE_PROVIDER = 'S3'\n"
                        f"  ENABLED = TRUE\n"
                        f"  STORAGE_AWS_ROLE_ARN = '{si_role_arn.strip()}'\n"
                        f"  STORAGE_ALLOWED_LOCATIONS = ({locations})"
                    )
                    if si_blocked.strip():
                        blocked = ", ".join(f"'{l.strip()}'" for l in si_blocked.split(","))
                        ddl += f"\n  STORAGE_BLOCKED_LOCATIONS = ({blocked})"
                    ddl += ";"

                    st.code(ddl, language="sql")
                    try:
                        cur.execute(ddl)
                        conn.commit()
                        st.success(f"Integration `{si_name.strip().upper()}` created!")

                        # Show the trust relationship info
                        cur.execute(f"DESCRIBE INTEGRATION {si_name.strip().upper()}")
                        desc_rows = cur.fetchall()
                        desc_cols = [d[0] for d in cur.description]
                        desc_df = pd.DataFrame(desc_rows, columns=desc_cols)
                        st.markdown("**Configure Trust Relationship:**")
                        st.caption("Copy the values below into your AWS IAM role's trust policy.")
                        st.dataframe(desc_df, use_container_width=True, hide_index=True)
                    except Exception as e:
                        st.error(f"Failed: {e}")

        elif provider == "Azure Blob":
            si_tenant = st.text_input(
                "Azure Tenant ID *",
                placeholder="your-tenant-id-here", key="spw_si_tenant")
            si_locations = st.text_input(
                "Allowed Locations *",
                placeholder="azure://account.blob.core.windows.net/container/",
                key="spw_si_az_locations")

            if st.button("Generate & Execute DDL", type="primary",
                         use_container_width=True, key="spw_si_create_az"):
                if not si_name.strip() or not si_tenant.strip() or not si_locations.strip():
                    st.error("All fields are required.")
                else:
                    locations = ", ".join(f"'{l.strip()}'" for l in si_locations.split(","))
                    ddl = (
                        f"CREATE STORAGE INTEGRATION {si_name.strip().upper()}\n"
                        f"  TYPE = EXTERNAL_STAGE\n"
                        f"  STORAGE_PROVIDER = 'AZURE'\n"
                        f"  ENABLED = TRUE\n"
                        f"  AZURE_TENANT_ID = '{si_tenant.strip()}'\n"
                        f"  STORAGE_ALLOWED_LOCATIONS = ({locations});"
                    )
                    st.code(ddl, language="sql")
                    try:
                        cur.execute(ddl)
                        conn.commit()
                        st.success(f"Integration `{si_name.strip().upper()}` created!")
                        cur.execute(f"DESCRIBE INTEGRATION {si_name.strip().upper()}")
                        desc_rows = cur.fetchall()
                        desc_cols = [d[0] for d in cur.description]
                        st.markdown("**Configure Azure Consent:**")
                        st.dataframe(pd.DataFrame(desc_rows, columns=desc_cols),
                                     use_container_width=True, hide_index=True)
                    except Exception as e:
                        st.error(f"Failed: {e}")

        else:  # GCS
            si_locations = st.text_input(
                "Allowed Locations *",
                placeholder="gcs://my-bucket/",
                key="spw_si_gcs_locations")

            if st.button("Generate & Execute DDL", type="primary",
                         use_container_width=True, key="spw_si_create_gcs"):
                if not si_name.strip() or not si_locations.strip():
                    st.error("All fields are required.")
                else:
                    locations = ", ".join(f"'{l.strip()}'" for l in si_locations.split(","))
                    ddl = (
                        f"CREATE STORAGE INTEGRATION {si_name.strip().upper()}\n"
                        f"  TYPE = EXTERNAL_STAGE\n"
                        f"  STORAGE_PROVIDER = 'GCS'\n"
                        f"  ENABLED = TRUE\n"
                        f"  STORAGE_ALLOWED_LOCATIONS = ({locations});"
                    )
                    st.code(ddl, language="sql")
                    try:
                        cur.execute(ddl)
                        conn.commit()
                        st.success(f"Integration `{si_name.strip().upper()}` created!")
                        cur.execute(f"DESCRIBE INTEGRATION {si_name.strip().upper()}")
                        desc_rows = cur.fetchall()
                        desc_cols = [d[0] for d in cur.description]
                        st.markdown("**Service Account for GCS:**")
                        st.dataframe(pd.DataFrame(desc_rows, columns=desc_cols),
                                     use_container_width=True, hide_index=True)
                    except Exception as e:
                        st.error(f"Failed: {e}")

        section_card_end()

    # ══════════════════════════════════════════════════════════════════════════
    # Tab 2: Create Stage
    # ══════════════════════════════════════════════════════════════════════════
    with tab_stage:
        section_card_start("Existing Stages", "📦", border_color=ST_PENDING)
        try:
            cur.execute("SHOW STAGES IN ACCOUNT")
            stg_rows = cur.fetchall()
            stg_cols = [d[0] for d in cur.description]
            if stg_rows:
                stg_df = pd.DataFrame(stg_rows, columns=stg_cols)
                display_stg = [c for c in ["name", "database_name", "schema_name",
                                           "url", "type", "owner"]
                               if c in stg_df.columns]
                st.dataframe(stg_df[display_stg] if display_stg else stg_df,
                             use_container_width=True, hide_index=True)
                st.caption(f"{len(stg_rows)} stage(s) found")
            else:
                st.info("No stages found.")
        except Exception as e:
            st.warning(f"Cannot list stages: {e}")
        section_card_end()

        # Create stage form
        section_card_start("Create External Stage", "➕", border_color=ST_SUCCESS)

        sc1, sc2, sc3 = st.columns(3)
        stg_db = sc1.text_input("Database *", value="HISTLOAD_DB", key="spw_stg_db")
        stg_schema = sc2.text_input("Schema *", value="META", key="spw_stg_schema")
        stg_name = sc3.text_input("Stage Name *", placeholder="MY_S3_STAGE",
                                  key="spw_stg_name")

        # Integration picker
        try:
            cur.execute("SHOW STORAGE INTEGRATIONS")
            si_rows2 = cur.fetchall()
            si_name_idx = next(i for i, d in enumerate(cur.description) if d[0] == "name")
            integrations = [r[si_name_idx] for r in si_rows2]
        except Exception:
            integrations = []

        sc4, sc5 = st.columns(2)
        if integrations:
            sel_integration = sc4.selectbox("Storage Integration *", integrations,
                                           key="spw_stg_integration")
        else:
            sel_integration = sc4.text_input("Storage Integration *",
                                            placeholder="my_s3_integration",
                                            key="spw_stg_int_txt")

        stg_url = sc5.text_input("Stage URL *",
                                 placeholder="s3://bucket/path/ or azure://...",
                                 key="spw_stg_url")

        if st.button("Create Stage", type="primary",
                     use_container_width=True, key="spw_stg_create"):
            if not stg_name.strip() or not stg_url.strip() or not sel_integration:
                st.error("All fields are required.")
            else:
                fqn = f"{stg_db.strip()}.{stg_schema.strip()}.{stg_name.strip().upper()}"
                ddl = (
                    f"CREATE STAGE IF NOT EXISTS {fqn}\n"
                    f"  STORAGE_INTEGRATION = {sel_integration}\n"
                    f"  URL = '{stg_url.strip()}';"
                )
                st.code(ddl, language="sql")
                try:
                    cur.execute(f"CREATE DATABASE IF NOT EXISTS {stg_db.strip()}")
                    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {stg_db.strip()}.{stg_schema.strip()}")
                    cur.execute(ddl)
                    conn.commit()
                    st.success(f"Stage `{fqn}` created!")
                except Exception as e:
                    st.error(f"Failed: {e}")

        section_card_end()

    # ══════════════════════════════════════════════════════════════════════════
    # Tab 3: Generate Snowpipe
    # ══════════════════════════════════════════════════════════════════════════
    with tab_pipe:
        section_card_start("Existing Pipes", "🔄", border_color=TA_ORANGE)
        try:
            cur.execute("SHOW PIPES IN ACCOUNT")
            pipe_rows = cur.fetchall()
            pipe_cols = [d[0] for d in cur.description]
            if pipe_rows:
                pipe_df = pd.DataFrame(pipe_rows, columns=pipe_cols)
                display_pipes = [c for c in ["name", "database_name", "schema_name",
                                             "definition", "owner", "notification_channel"]
                                 if c in pipe_df.columns]
                st.dataframe(pipe_df[display_pipes] if display_pipes else pipe_df,
                             use_container_width=True, hide_index=True)
                st.caption(f"{len(pipe_rows)} pipe(s) found")
            else:
                st.info("No pipes found. Create one below.")
        except Exception as e:
            st.warning(f"Cannot list pipes: {e}")
        section_card_end()

        # Create pipe
        section_card_start("Generate Snowpipe DDL", "➕", border_color=TA_ORANGE)
        st.caption("Generate a Snowpipe that auto-ingests files as they land on stage.")

        pc1, pc2, pc3 = st.columns(3)
        pipe_db = pc1.text_input("Database", value="HISTLOAD_DB", key="spw_pipe_db")
        pipe_schema = pc2.text_input("Schema", value="META", key="spw_pipe_schema")
        pipe_name = pc3.text_input("Pipe Name *", placeholder="MY_SNOWPIPE",
                                   key="spw_pipe_name")

        # Target table
        pt1, pt2, pt3 = st.columns(3)
        pipe_tgt_db = pt1.text_input("Target Database *", value="HISTLOAD_DB",
                                     key="spw_pipe_tgt_db")
        pipe_tgt_schema = pt2.text_input("Target Schema *", value="RAW",
                                         key="spw_pipe_tgt_schema")
        pipe_tgt_table = pt3.text_input("Target Table *", placeholder="MY_TABLE",
                                        key="spw_pipe_tgt_table")

        # Stage picker
        try:
            cur.execute("SHOW STAGES IN ACCOUNT")
            stg_rows3 = cur.fetchall()
            stg_name_idx = next(i for i, d in enumerate(cur.description) if d[0] == "name")
            stg_db_idx = next(i for i, d in enumerate(cur.description) if d[0] == "database_name")
            stg_sch_idx = next(i for i, d in enumerate(cur.description) if d[0] == "schema_name")
            all_stages = [f"{r[stg_db_idx]}.{r[stg_sch_idx]}.{r[stg_name_idx]}"
                          for r in stg_rows3]
        except Exception:
            all_stages = []

        ps1, ps2 = st.columns(2)
        if all_stages:
            pipe_stage = ps1.selectbox("Source Stage *", all_stages, key="spw_pipe_stage")
        else:
            pipe_stage = ps1.text_input("Source Stage (FQN) *",
                                        placeholder="DB.SCHEMA.STAGE", key="spw_pipe_stage_txt")

        pipe_file_type = ps2.selectbox("File Format", ["CSV", "PARQUET", "JSON", "AVRO"],
                                       key="spw_pipe_fmt")

        # CSV options
        if pipe_file_type == "CSV":
            pf1, pf2, pf3 = st.columns(3)
            pipe_delim = pf1.text_input("Delimiter", value=",", key="spw_pipe_delim")
            pipe_skip = pf2.number_input("Skip Header", value=1, min_value=0, key="spw_pipe_skip")
            pipe_enclosed = pf3.text_input("Enclosed By", value='"', key="spw_pipe_enc")

        pipe_auto_ingest = st.checkbox("Auto-ingest (requires event notification)",
                                       value=True, key="spw_pipe_auto")
        pipe_pattern = st.text_input("File Pattern (optional)",
                                     placeholder=".*\\.csv", key="spw_pipe_pattern")

        # Generate DDL
        if st.button("Generate Snowpipe DDL", type="primary",
                     use_container_width=True, key="spw_pipe_gen"):
            if not pipe_name.strip() or not pipe_tgt_table.strip() or not pipe_stage:
                st.error("Pipe Name, Target Table, and Source Stage are required.")
            else:
                pipe_fqn = f"{pipe_db.strip()}.{pipe_schema.strip()}.{pipe_name.strip().upper()}"
                tgt_fqn = f"{pipe_tgt_db.strip()}.{pipe_tgt_schema.strip()}.{pipe_tgt_table.strip().upper()}"

                # Build file format inline
                if pipe_file_type == "CSV":
                    fmt_clause = (
                        f"FILE_FORMAT = (TYPE = 'CSV' "
                        f"FIELD_DELIMITER = '{pipe_delim}' "
                        f"SKIP_HEADER = {pipe_skip} "
                        f"FIELD_OPTIONALLY_ENCLOSED_BY = '{pipe_enclosed}')")
                elif pipe_file_type == "PARQUET":
                    fmt_clause = "FILE_FORMAT = (TYPE = 'PARQUET')"
                elif pipe_file_type == "JSON":
                    fmt_clause = "FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)"
                else:
                    fmt_clause = "FILE_FORMAT = (TYPE = 'AVRO')"

                # Build COPY INTO
                copy_stmt = f"COPY INTO {tgt_fqn}\n    FROM @{pipe_stage}/\n    {fmt_clause}"
                if pipe_pattern.strip():
                    copy_stmt += f"\n    PATTERN = '{pipe_pattern.strip()}'"
                if pipe_file_type in ("PARQUET", "JSON", "AVRO"):
                    copy_stmt += "\n    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"

                auto_str = "AUTO_INGEST = TRUE" if pipe_auto_ingest else "AUTO_INGEST = FALSE"
                ddl = (
                    f"CREATE PIPE IF NOT EXISTS {pipe_fqn}\n"
                    f"  {auto_str}\n"
                    f"  AS\n"
                    f"  {copy_stmt};"
                )

                st.code(ddl, language="sql")

                # Execute or copy
                ex1, ex2 = st.columns(2)
                if ex1.button("Execute DDL", key="spw_pipe_exec", use_container_width=True):
                    try:
                        cur.execute(ddl)
                        conn.commit()
                        st.success(f"Pipe `{pipe_fqn}` created!")

                        # Show notification channel for auto-ingest setup
                        if pipe_auto_ingest:
                            cur.execute(f"SHOW PIPES LIKE '{pipe_name.strip().upper()}'")
                            pipe_info = cur.fetchall()
                            pipe_info_cols = [d[0] for d in cur.description]
                            if pipe_info:
                                pipe_dict = dict(zip(pipe_info_cols, pipe_info[0]))
                                notif = pipe_dict.get("notification_channel", "")
                                if notif:
                                    info_box(
                                        f"Configure your S3 event notification to send to: "
                                        f"<code>{notif}</code>",
                                        icon="🔔")
                    except Exception as e:
                        st.error(f"Failed: {e}")
                ex2.button("Copy to Clipboard", key="spw_pipe_copy",
                           use_container_width=True)

        section_card_end()

    # ══════════════════════════════════════════════════════════════════════════
    # Tab 4: Monitor (COPY_HISTORY)
    # ══════════════════════════════════════════════════════════════════════════
    with tab_monitor:
        section_card_start("COPY_HISTORY Monitor", "📊", border_color=ST_SUCCESS)
        st.caption("View recent file loads from Snowpipe and COPY INTO operations.")

        # Filters
        mc1, mc2, mc3 = st.columns(3)
        mon_hours = mc1.selectbox("Time Range", [1, 6, 12, 24, 48, 168],
                                  index=3, format_func=lambda x: f"Last {x}h",
                                  key="spw_mon_hours")
        mon_status = mc2.selectbox("Status Filter",
                                   ["All", "Loaded", "Load Failed", "Partially Loaded"],
                                   key="spw_mon_status")
        mon_table = mc3.text_input("Filter by Table (optional)",
                                   placeholder="MY_TABLE", key="spw_mon_table")

        # Query COPY_HISTORY
        try:
            query = (
                "SELECT TABLE_NAME, FILE_NAME, STATUS, ROW_COUNT, ROW_PARSED, "
                "ERROR_COUNT, FIRST_ERROR_MESSAGE, "
                "LAST_LOAD_TIME "
                "FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY "
                f"WHERE LAST_LOAD_TIME >= DATEADD('hours', -{mon_hours}, CURRENT_TIMESTAMP())\n"
            )
            if mon_status != "All":
                status_map = {"Loaded": "Loaded", "Load Failed": "Load_Failed",
                              "Partially Loaded": "Partially_Loaded"}
                query += f"AND STATUS = '{status_map.get(mon_status, mon_status)}'\n"
            if mon_table.strip():
                query += f"AND TABLE_NAME ILIKE '%{mon_table.strip()}%'\n"
            query += "ORDER BY LAST_LOAD_TIME DESC LIMIT 200;"

            cur.execute(query)
            hist_rows = cur.fetchall()
            hist_cols = [d[0] for d in cur.description]

            if hist_rows:
                hist_df = pd.DataFrame(hist_rows, columns=hist_cols)

                # Summary metrics
                hm1, hm2, hm3, hm4 = st.columns(4)
                total_loaded = hist_df[hist_df["STATUS"] == "Loaded"].shape[0]
                total_failed = hist_df[hist_df["STATUS"] != "Loaded"].shape[0]
                total_rows_loaded = int(hist_df["ROW_COUNT"].sum())
                total_errors = int(hist_df["ERROR_COUNT"].sum())

                hm1.metric("Files Loaded", total_loaded)
                hm2.metric("Files Failed", total_failed)
                hm3.metric("Total Rows", f"{total_rows_loaded:,}")
                hm4.metric("Errors", total_errors)

                st.markdown("<br>", unsafe_allow_html=True)

                # Paginate
                HIST_PER_PAGE = 20
                total_h = len(hist_df)
                total_h_pages = max(1, (total_h + HIST_PER_PAGE - 1) // HIST_PER_PAGE)
                h_page = st.session_state.get("spw_hist_page", 0)
                h_page = min(h_page, total_h_pages - 1)
                start = h_page * HIST_PER_PAGE
                end = min(start + HIST_PER_PAGE, total_h)

                display_h = [c for c in ["TABLE_NAME", "FILE_NAME", "STATUS",
                                         "ROW_COUNT", "ERROR_COUNT",
                                         "FIRST_ERROR_MESSAGE", "LAST_LOAD_TIME"]
                             if c in hist_df.columns]
                st.dataframe(hist_df[display_h].iloc[start:end],
                             use_container_width=True, hide_index=True)

                if total_h_pages > 1:
                    hp1, hp2, hp3 = st.columns([1, 2, 1])
                    hp1.button("◀ Prev", key="spw_hist_prev",
                               disabled=(h_page == 0),
                               on_click=lambda: st.session_state.update(spw_hist_page=h_page - 1))
                    hp2.markdown(
                        f"<div style='text-align:center;padding-top:6px;color:{TXT_LABEL}'>"
                        f"Page {h_page + 1} of {total_h_pages} ({total_h} records)</div>",
                        unsafe_allow_html=True)
                    hp3.button("Next ▶", key="spw_hist_next",
                               disabled=(h_page >= total_h_pages - 1),
                               on_click=lambda: st.session_state.update(spw_hist_page=h_page + 1))
            else:
                st.info(f"No COPY_HISTORY records in the last {mon_hours} hours.")

        except Exception as e:
            st.warning(f"Cannot query COPY_HISTORY: {e}")
            st.caption("Uses SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY (up to 45-min latency). "
                       "Requires ACCOUNTADMIN or IMPORTED PRIVILEGES on SNOWFLAKE database.")

        section_card_end()

    cur.close()
