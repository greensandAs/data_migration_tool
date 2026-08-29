# Pipeline execution page — subprocess runner with live log and stop button.
# Co-authored with CoCo
"""pages/run.py — Execute migration pipelines with Tiger Analytics styled log.

Uses the subprocess-based job runner from app.py (background process +
daemon thread reader). Supports st.fragment live refresh, stop button,
group-by-table log view, and run-single-table.
"""
from __future__ import annotations

import streamlit as st
import time

from metadata import config_manager
from metadata import connection_manager

# Brand tokens
TA_NAVY = "#0F1B2D"
TXT_PRIMARY = "#F0F4F8"
TXT_SECONDARY = "#A8B8CC"
TXT_LABEL = "#7E96B0"
ST_SUCCESS = "#34D058"
ST_FAILED = "#F85149"
ST_SKIPPED = "#F0A742"
ST_PENDING = "#58A6FF"
BORDER = "#263245"


def _group_log_by_table(text: str, known: set | None = None) -> dict:
    """Split finished run log into {table: [lines]} using [db.table] prefix."""
    groups = {}
    for raw in text.split("\n"):
        table = "· general ·"
        body = raw
        if raw.startswith("[") and "] " in raw:
            tag = raw[1:raw.index("] ")]
            is_table = (tag in known) if known is not None else (
                "." in tag and "/" not in tag and " " not in tag)
            if is_table:
                table = tag
                body = raw[raw.index("] ") + 2:]
        groups.setdefault(table, []).append(body)
    return groups


def render(conn):
    """Main render function for the Run page."""
    from utils import shared

    st.markdown('<div class="section-header">Pipeline Controls</div>',
                unsafe_allow_html=True)

    _running = shared.job_running()
    cur = conn.cursor()

    # Use sidebar-selected profile
    _sidebar_profile = st.session_state.get("selected_profile", "All Connections")
    profile_filter = None if _sidebar_profile == "All Connections" else _sidebar_profile

    # Empty state if no connection selected
    if not profile_filter:
        from utils.shared import empty_state
        empty_state("▶️", "Select a Source Connection",
                    "Choose a connection from the <b>Source Connection</b> dropdown "
                    "in the sidebar to run pipelines.")
        cur.close()
        return

    # -- Table selector (multi-select) --
    active_tables = config_manager.list_active(cur, connection_profile=profile_filter)
    table_names = [f"{t['SOURCE_DB']}.{t['SOURCE_TABLE']}" for t in active_tables]

    run_all = st.checkbox("Run All Active Tables", value=True, key="run_all_tables")
    if run_all:
        selected_tables = table_names
    else:
        selected_tables = st.multiselect(
            "Select Tables", table_names, default=[], key="run_table_multi",
            placeholder="Choose one or more tables...")

    # -- Action buttons (single row) --
    b1, b2, b3, b4 = st.columns(4)
    full_clicked = b1.button("▶️ Full Run", type="primary",
                             use_container_width=True, disabled=_running)
    extract_clicked = b2.button("📤 Extract", use_container_width=True,
                                disabled=_running)
    load_clicked = b3.button("📥 Load", use_container_width=True,
                             disabled=_running)
    resume_clicked = b4.button("🔁 Resume", use_container_width=True,
                               disabled=_running)

    # -- Launch logic --
    def _launch(mode_flag: str | None, label: str):
        if not selected_tables:
            st.warning("Select at least one table to run.")
            return
        args = ["core/orchestrator.py"]
        if profile_filter:
            args += ["--profile", profile_filter]
        if not run_all:
            for tbl in selected_tables:
                _, tbl_name = tbl.split(".", 1)
                args += ["--table", tbl_name]
        if mode_flag:
            args.append(mode_flag)
        shared.start_job(args, label)
        st.rerun()

    if full_clicked and not _running:
        _label = "Full Run · All" if run_all else f"Full Run · {len(selected_tables)} table(s)"
        _launch(None, _label)
    elif extract_clicked and not _running:
        _label = "Extract · All" if run_all else f"Extract · {len(selected_tables)} table(s)"
        _launch("--extract-only", _label)
    elif load_clicked and not _running:
        _label = "Load · All" if run_all else f"Load · {len(selected_tables)} table(s)"
        _launch("--load-only", _label)
    elif resume_clicked and not _running:
        _launch("--resume", "Resume failed")

    # -- Resume from dashboard retry button --
    if "run_table_id" in st.session_state:
        config_id = st.session_state.pop("run_table_id")
        run_mode = st.session_state.pop("run_mode", "full")
        tbl = config_manager.get_by_id(cur, config_id)
        if tbl and not _running:
            tbl_name = tbl.get("SOURCE_TABLE", "")
            args = ["core/orchestrator.py", "--table", tbl_name]
            if run_mode == "resume":
                args.append("--resume")
            shared.start_job(args, f"Resume · {tbl_name}")
            st.rerun()

    cur.close()

    # -- Live log --
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="section-header">Live Log</div>', unsafe_allow_html=True)

    if "last_log" not in st.session_state:
        st.session_state["last_log"] = "No runs yet. Click ▶️ Full Run to start."

    job = st.session_state.get("_job")

    if job and job.get("running"):
        # Running: show status message (left) + stop button (right), aligned
        msg_col, stop_col = st.columns([5, 1])
        msg_col.warning(f"⏳ {job.get('label','Job')} running… ({len(job['lines'])} log lines)")
        if stop_col.button("⏹️ Stop", type="secondary", use_container_width=True,
                           key="stop_running"):
            shared.stop_job()
            st.rerun()

        # Log output
        tail = job["lines"][-60:]
        colored = "<br>".join(shared.colorize_log(l) for l in tail) or "starting…"
        st.markdown(f'<div class="log-box">{colored}</div>', unsafe_allow_html=True)
        time.sleep(1.0)
        st.rerun()

    elif job and not job.get("consumed"):
        # Just finished: consume the result
        job["consumed"] = True
        if job.get("error"):
            st.session_state["last_log"] = job["error"]
            st.session_state["_toast"] = (1, job.get("label", "Load"))
        else:
            st.session_state["last_log"] = "\n".join(job["lines"]) or "(no output)"
            rc = -1 if job.get("stopped") else (job.get("rc") if job.get("rc") is not None else 1)
            st.session_state["_toast"] = (
                rc, job.get("label", "Load") + (" (stopped)" if job.get("stopped") else ""))
        st.rerun()

    else:
        # Show last log
        if job and job.get("stopped"):
            st.warning("⏹️ Last job was stopped by the user.")

        group = st.toggle("📑 Group by table", value=False,
                          help="Group output per table using [db.table] prefix from parallel runs.")

        if group:
            cur2 = conn.cursor()
            all_tables = config_manager.list_all(cur2)
            cur2.close()
            known = {f"{t.get('SOURCE_DB')}.{t.get('SOURCE_TABLE')}" for t in all_tables}
            groups = _group_log_by_table(st.session_state["last_log"], known)
            for table in sorted(groups, key=lambda t: (t == "· general ·", t)):
                body_lines = groups[table]
                blob = "<br>".join(shared.colorize_log(ln) for ln in body_lines if ln.strip())
                if not blob:
                    continue
                with st.expander(f"📄 {table}  ({len([l for l in body_lines if l.strip()])} lines)",
                                 expanded=(table != "· general ·")):
                    st.markdown(f'<div class="log-box">{blob}</div>', unsafe_allow_html=True)
        else:
            lines = st.session_state["last_log"].split("\n")
            colored = "<br>".join(shared.colorize_log(line) for line in lines[-60:])
            st.markdown(f'<div class="log-box">{colored}</div>', unsafe_allow_html=True)
