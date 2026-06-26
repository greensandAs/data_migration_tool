# Shared utilities for DMT v1 — job runner, log colorizer, password resolver.
# Co-authored with CoCo
"""shared.py — Shared functions used by app.py and view modules.

Extracted here to avoid circular imports (views cannot import app.py
because app.py contains st.set_page_config which can only run once).
"""
from __future__ import annotations

import os
import subprocess
import sys
import threading
from pathlib import Path

import streamlit as st

HERE = Path(__file__).parent


# ── Log colorizer ─────────────────────────────────────────────────────────────
def colorize_log(line: str) -> str:
    low = line.lower()
    if any(x in low for x in ["✅", "done", "success", "complete"]):
        return f'<span class="log-ok">{line}</span>'
    if any(x in low for x in ["❌", "failed", "error", "exception", "traceback"]):
        return f'<span class="log-err">{line}</span>'
    if any(x in low for x in ["⚠️", "warn", "skip", "no new", "drift"]):
        return f'<span class="log-warn">{line}</span>'
    if any(x in low for x in ["[full", "[incr", "batch", "====", "load"]):
        return f'<span class="log-info">{line}</span>'
    return line


# ── Background job runner (subprocess-based) ──────────────────────────────────
def start_job(args: list[str], label: str = "Load"):
    job = {"args": args, "label": label, "lines": [], "rc": None,
           "running": True, "proc": None, "error": None, "stopped": False,
           "consumed": False}
    try:
        proc = subprocess.Popen(
            [sys.executable, "-u", *args], cwd=str(HERE),
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1,
            env={**os.environ, "PYTHONUNBUFFERED": "1"})
    except Exception as ex:
        job.update(running=False, rc=-1,
                   error=f"Could not start the job ({sys.executable}): {ex}")
        st.session_state["_job"] = job
        return
    job["proc"] = proc

    def _reader():
        try:
            for raw in proc.stdout:
                job["lines"].append(raw.rstrip())
        except Exception:
            pass
        proc.wait()
        job["rc"] = proc.returncode
        job["running"] = False

    t = threading.Thread(target=_reader, daemon=True)
    t.start()
    st.session_state["_job"] = job


def stop_job():
    job = st.session_state.get("_job")
    if not (job and job.get("proc") and job.get("running")):
        return
    proc = job["proc"]
    try:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except Exception:
            proc.kill()
    except Exception:
        pass
    job["running"] = False
    job["stopped"] = True
    job["rc"] = proc.returncode if proc.returncode is not None else -15


def job_running() -> bool:
    job = st.session_state.get("_job")
    return bool(job and job.get("running"))


# ── Live log panel (fragment-based auto-refresh) ──────────────────────────────
_fragment = getattr(st, "fragment", None) or getattr(st, "experimental_fragment", None)


def _render_running_panel():
    job = st.session_state.get("_job")
    if not (job and job.get("running")):
        try:
            st.rerun(scope="app")
        except TypeError:
            st.rerun()
        return
    c1, c2 = st.columns([1, 4])
    if c1.button("⏹️ Stop", type="secondary", use_container_width=True, key="stop_job_btn"):
        stop_job()
        try:
            st.rerun(scope="app")
        except TypeError:
            st.rerun()
        return
    c2.warning(f"⏳ {job.get('label', 'Job')} running… ({len(job['lines'])} log lines)")
    tail = job["lines"][-60:]
    colored = "<br>".join(colorize_log(l) for l in tail) or "starting…"
    st.markdown(f'<div class="log-box">{colored}</div>', unsafe_allow_html=True)


live_running_panel = (_fragment(run_every=1.0)(_render_running_panel)
                      if _fragment else None)


# ── AI helpers (Cortex) ───────────────────────────────────────────────────────
def ai_enabled() -> bool:
    """Check if AI Assist is toggled on in the sidebar."""
    return bool(st.session_state.get("_ai_on", False))


def cortex_complete(prompt: str, model: str = None, conn=None) -> str:
    """Call Snowflake Cortex COMPLETE. Pass conn explicitly or it reads from session."""
    model = model or st.session_state.get("_ai_model", "llama3.1-70b")
    try:
        if conn is None:
            conn = st.session_state.get("_sf_conn_obj")
        if conn is None:
            return "(No Snowflake connection available for AI)"
        cur = conn.cursor()
        cur.execute("SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s)", (model, prompt))
        result = (cur.fetchone()[0] or "").strip()
        cur.close()
        return result
    except Exception as e:
        return f"(AI error: {e})"
TXT_PRIMARY = "#F0F4F8"
TXT_SECONDARY = "#A8B8CC"


def empty_state(icon: str, title: str, description: str = ""):
    """Render a consistent centered empty state with icon, title, and description.

    Usage:
        from shared import empty_state
        empty_state("📋", "No Tables Configured",
                    "Click <b>Generate Config</b> to auto-discover tables.")
    """
    desc_html = (f'<div style="font-size:.85rem;color:{TXT_SECONDARY};max-width:420px;'
                 f'margin:0 auto">{description}</div>') if description else ""
    st.markdown(
        f'<div style="text-align:center;padding:50px 20px;">'
        f'<div style="font-size:2.5rem;margin-bottom:12px">{icon}</div>'
        f'<div style="font-size:1.1rem;font-weight:700;color:{TXT_PRIMARY};margin-bottom:8px">'
        f'{title}</div>'
        f'{desc_html}'
        f'</div>', unsafe_allow_html=True)


# ── App settings helpers ──────────────────────────────────────────────────────

_SETTINGS_TABLE = "HISTLOAD_DB.META.DMT_SETTINGS"


def get_setting(cur, key: str, default: str = "") -> str:
    """Read a single setting value from DMT_SETTINGS."""
    try:
        cur.execute(
            f"SELECT SETTING_VALUE FROM {_SETTINGS_TABLE} WHERE SETTING_KEY = %s",
            (key,))
        row = cur.fetchone()
        return row[0] if row and row[0] else default
    except Exception:
        return default


def get_allowed_sources(cur) -> list[str]:
    """Return list of allowed source types from DMT_SETTINGS.

    If the setting is missing or empty, defaults to all implemented sources.
    """
    raw = get_setting(cur, "ALLOWED_SOURCES", "mysql,teradata")
    return [s.strip().lower() for s in raw.split(",") if s.strip()]

