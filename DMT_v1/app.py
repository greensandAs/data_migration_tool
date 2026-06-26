# Unified Streamlit app entry point for DMT v1 — multi-source data migration toolkit.
# Co-authored with CoCo
"""app.py — Main Streamlit entry point for DMT v1.

Modular page-based layout with Tiger Analytics branding. Each page is a
self-contained module in pages/. The app handles global state (Snowflake session),
sidebar navigation, custom CSS, connection pooling, and AI integration.
"""
from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import streamlit as st

from views.dashboard import render as render_dashboard
from views.connections import render as render_connections
from views.config import render as render_config
from views.run import render as render_run
from views.history import render as render_history
from views.monitoring import render as render_monitoring

try:
    from dotenv import load_dotenv
    # Explicitly load .env from the app directory (not CWD)
    _env_path = Path(__file__).parent / ".env"
    load_dotenv(_env_path)
except ImportError:
    pass

HERE = Path(__file__).parent
_LOGO_DIR = HERE / "assets" / "logos"
_FAVICON = _LOGO_DIR / "ta_favicon.png"

# ── Brand & semantic color tokens ─────────────────────────────────────────────
TA_ORANGE = "#F15A22"
TA_ORANGE_DARK = "#C94A18"
TA_NAVY = "#0F1B2D"
TA_NAVY_LIGHT = "#162032"

TXT_PRIMARY = "#F0F4F8"
TXT_SECONDARY = "#A8B8CC"
TXT_LABEL = "#7E96B0"

ST_SUCCESS = "#34D058"
ST_FAILED = "#F85149"
ST_SKIPPED = "#F0A742"
ST_PENDING = "#58A6FF"

BORDER = "#263245"

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="DMT v1 | Tiger Analytics",
    page_icon=str(_FAVICON) if _FAVICON.exists() else "🔄",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Global CSS ────────────────────────────────────────────────────────────────
st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Source+Sans+Pro:wght@400;600;700&family=JetBrains+Mono:wght@400;600&display=swap');
html, body, [class*="css"] {{ font-family: 'Source Sans Pro', 'Segoe UI', Arial, sans-serif; }}
#MainMenu, footer {{ visibility: hidden; }}
/* Hide header toolbar but keep the header container for toasts */
header[data-testid="stHeader"] {{ background: transparent !important; }}
div[data-testid="stToolbar"] {{ display: none !important; }}
div[data-testid="stDecoration"] {{ display: none !important; }}
.block-container {{ padding-top: 1rem; padding-bottom: 2rem; }}

/* Sidebar — static, non-collapsible, fixed width 240px */
section[data-testid="stSidebar"] {{ background: {TA_NAVY} !important; border-right: 3px solid {TA_ORANGE}; min-width: 240px !important; max-width: 240px !important; width: 240px !important; }}
section[data-testid="stSidebar"] > div {{ width: 240px !important; }}
section[data-testid="stSidebar"] > div > div {{ padding-top: 0 !important; margin-top: 0 !important; }}
section[data-testid="stSidebar"] > div > div > div:first-child {{ margin-top: 0 !important; padding-top: 0 !important; }}
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] .stMarkdown,
section[data-testid="stSidebar"] .stMarkdown * {{ color: {TXT_PRIMARY} !important; }}
section[data-testid="stSidebar"] .stCaption,
section[data-testid="stSidebar"] .stCaption * {{ color: {TXT_SECONDARY} !important; }}
/* Hide collapse button */
button[data-testid="stSidebarCollapseButton"],
button[data-testid="baseButton-headerNoPadding"] {{ display: none !important; }}
/* Sticky sidebar header */
.sidebar-header {{ position: sticky; top: 0; z-index: 999; background: {TA_NAVY}; padding: 6px 0 6px 0; margin-bottom: 8px; }}
/* Sticky main header */
.main-header {{ position: sticky; top: 0; z-index: 998; background: #0e1117; padding: 12px 0 12px 0; margin: -1rem 0 18px 0; border-bottom: 1px solid {BORDER}; }}
/* Connection card in sidebar */
.conn-card {{ background: {TA_NAVY_LIGHT}; border: 1px solid {BORDER}; border-radius: 8px; padding: 12px 14px; margin-top: 12px; }}
.conn-card .conn-row {{ display: flex; align-items: center; margin-bottom: 4px; }}
.conn-card .conn-dot {{ width: 10px; height: 10px; border-radius: 50%; margin-right: 8px; flex-shrink: 0; }}
.conn-card .conn-title {{ font-size: .78rem; font-weight: 700; color: {TXT_PRIMARY}; }}
.conn-card .conn-detail {{ font-family: 'JetBrains Mono', monospace; font-size: .68rem; color: {TXT_SECONDARY}; margin-left: 18px; line-height: 1.7; }}

/* Metric cards */
.metric-card {{ background: {TA_NAVY}; border: 1px solid {BORDER}; border-radius: 10px; padding: 18px 22px; margin-bottom: 12px; }}
.metric-card .label {{ font-size: .72rem; letter-spacing: 2px; text-transform: uppercase; color: {TXT_LABEL}; margin-bottom: 4px; font-weight: 600; }}
.metric-card .value {{ font-family: 'JetBrains Mono', monospace; font-size: 2rem; font-weight: 700; color: {TXT_PRIMARY}; line-height: 1.1; }}
.metric-card .sub {{ font-size: .72rem; color: {TXT_SECONDARY}; margin-top: 4px; }}

/* Table status cards */
.table-card {{ background: {TA_NAVY}; border: 1px solid {BORDER}; border-radius: 10px; padding: 16px 20px; margin-bottom: 10px; position: relative; overflow: hidden; }}
.table-card::before {{ content: ''; position: absolute; left: 0; top: 0; bottom: 0; width: 4px; }}
.table-card.success::before {{ background: {ST_SUCCESS}; }}
.table-card.failed::before {{ background: {ST_FAILED}; }}
.table-card.skipped::before {{ background: {ST_SKIPPED}; }}
.table-card.pending::before {{ background: {ST_PENDING}; }}
.table-card .tname {{ font-weight: 700; font-size: .95rem; color: {TXT_PRIMARY}; padding-right: 90px; }}
.table-card .tmeta {{ font-family: 'JetBrains Mono', monospace; font-size: .72rem; color: {TXT_SECONDARY}; margin-top: 4px; line-height: 1.6; }}
.table-card .tstatus {{ font-size: .68rem; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; padding: 3px 9px; border-radius: 4px; position: absolute; top: 16px; right: 20px; }}
.tstatus.success {{ background: #0d2e15; color: {ST_SUCCESS}; border: 1px solid {ST_SUCCESS}44; }}
.tstatus.failed {{ background: #2e0d0d; color: {ST_FAILED}; border: 1px solid {ST_FAILED}44; }}
.tstatus.skipped {{ background: #2e1e0d; color: {ST_SKIPPED}; border: 1px solid {ST_SKIPPED}44; }}
.tstatus.pending {{ background: #0d1e2e; color: {ST_PENDING}; border: 1px solid {ST_PENDING}44; }}

/* Log terminal */
.log-box {{ background: #0A0F17; border: 1px solid {BORDER}; border-radius: 8px; padding: 16px; font-family: 'JetBrains Mono', monospace; font-size: .75rem; color: #8BAFC8; height: 340px; overflow-y: auto; white-space: pre-wrap; line-height: 1.7; }}
.log-ok {{ color: {ST_SUCCESS}; }}
.log-err {{ color: {ST_FAILED}; }}
.log-warn {{ color: {ST_SKIPPED}; }}
.log-info {{ color: {ST_PENDING}; }}

/* Section header */
.section-header {{ font-size: .7rem; letter-spacing: 2px; text-transform: uppercase; color: {TA_ORANGE}; font-weight: 700; margin-bottom: 12px; padding-bottom: 6px; border-bottom: 2px solid {TA_ORANGE}33; }}

/* Pills */
.pill {{ display: inline-block; padding: 2px 9px; border-radius: 20px; font-size: .67rem; font-weight: 700; letter-spacing: .5px; text-transform: uppercase; margin-right: 4px; }}
.pill-full {{ background: #0d1e2e; color: {ST_PENDING}; border: 1px solid {ST_PENDING}55; }}
.pill-incr {{ background: #0d2e15; color: {ST_SUCCESS}; border: 1px solid {ST_SUCCESS}55; }}
.pill-source {{ background: #1a0d2e; color: #B07EFF; border: 1px solid #B07EFF55; }}

/* Connection dot */
.dot {{ display: inline-block; width: 9px; height: 9px; border-radius: 50%; margin-right: 6px; vertical-align: middle; }}

/* Primary button */
.stButton > button[kind="primary"] {{ background: {TA_ORANGE} !important; color: #fff !important; border: none !important; font-weight: 700 !important; letter-spacing: .3px; }}
.stButton > button[kind="primary"]:hover {{ background: {TA_ORANGE_DARK} !important; }}

/* Namespace info box */
.ns-box {{ background: {TA_NAVY}; border: 1px solid {BORDER}; border-radius: 6px; padding: 10px 14px; font-family: 'JetBrains Mono', monospace; font-size: .73rem; line-height: 1.8; margin-top: 8px; }}
.ns-box .ns-label {{ color: {TXT_LABEL}; }}
.ns-box .ns-value {{ color: {TXT_PRIMARY}; }}

/* Config summary cards */
.cfg-summary {{ display: flex; gap: 12px; margin-bottom: 16px; }}
.cfg-mini-card {{ flex: 1; background: {TA_NAVY}; border: 1px solid {BORDER}; border-radius: 8px; padding: 12px 16px; text-align: center; }}
.cfg-mini-card .mc-val {{ font-family: 'JetBrains Mono', monospace; font-size: 1.4rem; font-weight: 700; line-height: 1.2; }}
.cfg-mini-card .mc-lbl {{ font-size: .65rem; letter-spacing: 1.5px; text-transform: uppercase; color: {TXT_LABEL}; margin-top: 4px; font-weight: 600; }}

/* Schema group header */
.schema-group {{ background: {TA_NAVY}; border: 1px solid {BORDER}; border-radius: 8px; padding: 12px 18px; margin: 16px 0 8px 0; display: flex; align-items: center; justify-content: space-between; }}
.schema-group .sg-title {{ font-size: .82rem; font-weight: 700; color: {TXT_PRIMARY}; }}
.schema-group .sg-count {{ font-size: .68rem; color: {TXT_SECONDARY}; background: {TA_NAVY_LIGHT}; border: 1px solid {BORDER}; border-radius: 12px; padding: 2px 10px; margin-left: 10px; }}
.schema-group .sg-meta {{ font-size: .68rem; color: {TXT_LABEL}; }}
</style>
""", unsafe_allow_html=True)


# ── Session state init ────────────────────────────────────────────────────────
if "current_page" not in st.session_state:
    st.session_state.current_page = "Dashboard"


# ── Secure password resolution ────────────────────────────────────────────────
def resolve_password(auth_secret: str | None, sf_conn=None) -> str:
    """Resolve a source system password securely.

    Lookup order:
      1. Snowflake SECRET (if auth_secret looks like a FQN: DB.SCHEMA.NAME)
      2. Streamlit secrets (st.secrets)
      3. Environment variable (auth_secret is the env var name)
      4. Empty string (no password configured)

    The password NEVER touches disk or session_state.
    """
    if not auth_secret:
        return ""

    # 1. Snowflake SECRET (FQN contains dots: HISTLOAD_DB.META.MYSQL_PWD)
    if "." in auth_secret and sf_conn:
        try:
            cur = sf_conn.cursor()
            cur.execute(
                f"SELECT SECRET_STRING FROM TABLE("
                f"RESULT_SCAN(LAST_QUERY_ID())) -- not available; using workaround below"
            )
        except Exception:
            pass
        # Use SYSTEM$GET_GENERIC_SECRET or direct query
        try:
            cur = sf_conn.cursor()
            cur.execute(f"CALL SYSTEM$GET_GENERIC_SECRET('{auth_secret}')")
            pwd = cur.fetchone()[0]
            cur.close()
            if pwd:
                return pwd
        except Exception:
            pass

    # 2. Streamlit secrets
    try:
        parts = auth_secret.lower().split(".")
        sec = st.secrets
        for part in parts:
            sec = sec[part]
        if isinstance(sec, str):
            return sec
    except Exception:
        pass

    # 3. Environment variable
    val = os.getenv(auth_secret, "")
    if val:
        return val

    # 4. Fallback
    return ""


# ── Snowflake connection (pooled via cache_resource) ──────────────────────────
@st.cache_resource(show_spinner=False)
def _sf_conn():
    # Try Snowpark session first (running inside Snowsight/SiS)
    try:
        from snowflake.snowpark.context import get_active_session
        session = get_active_session()
        return session.connection
    except Exception:
        pass
    # Fallback: snowflake.connector with env vars
    import snowflake.connector
    return snowflake.connector.connect(
        account=os.getenv("SF_ACCOUNT"),
        user=os.getenv("SF_USER"),
        password=os.getenv("SF_PASSWORD"),
        role=os.getenv("SF_ROLE", "SYSADMIN"),
        warehouse=os.getenv("SF_WAREHOUSE", "COMPUTE_WH"),
        database=os.getenv("SF_DATABASE", "HISTLOAD_DB"),
        schema=os.getenv("SF_SCHEMA", "META"),
    )


def get_sf():
    """Cached Snowflake connection; reconnects if stale."""
    conn = _sf_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        return conn
    except Exception:
        _sf_conn.clear()
        return _sf_conn()


def check_snowflake() -> tuple[bool, dict]:
    """Returns (ok, info_dict) where info_dict has user, role, warehouse, account."""
    try:
        conn = get_sf()
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_ACCOUNT()")
        user, role, wh, acct = cur.fetchone()
        cur.close()
        return True, {"user": user, "role": role, "warehouse": wh, "account": acct}
    except Exception as e:
        has_acct = bool(os.getenv("SF_ACCOUNT"))
        has_user = bool(os.getenv("SF_USER"))
        has_pwd = bool(os.getenv("SF_PASSWORD"))
        return False, {"error": str(e)[:100],
                       "env": f"acct={'✓' if has_acct else '✗'} user={'✓' if has_user else '✗'} pwd={'✓' if has_pwd else '✗'}"}


# ── AI (Cortex) helpers ───────────────────────────────────────────────────────
AI_MODEL = "llama3.1-70b"
AI_MODELS = ["llama3.1-70b", "llama3.1-8b", "llama3.1-405b", "mistral-large2",
             "mixtral-8x7b", "snowflake-arctic", "claude-3-5-sonnet",
             "reka-flash", "gemma-7b"]


def ai_enabled() -> bool:
    return bool(st.session_state.get("_ai_on", False))


def cortex_complete(prompt: str, model: str = None) -> str:
    model = model or st.session_state.get("_ai_model", AI_MODEL)
    conn = get_sf()
    cur = conn.cursor()
    try:
        cur.execute("SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s)", (model, prompt))
        return (cur.fetchone()[0] or "").strip()
    finally:
        cur.close()


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


# ── UI helpers (shared across pages via import) ───────────────────────────────
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


def status_icon(s):
    return {"success": "✅", "failed": "❌", "skipped": "⏭️", None: "⏳"}.get(s, "⏳")


def load_type_pill(load_type: str) -> str:
    cls = "pill-incr" if load_type == "incremental" else "pill-full"
    return f'<span class="pill {cls}">{load_type}</span>'


def source_type_pill(source_type: str) -> str:
    return f'<span class="pill pill-source">{source_type}</span>'


def render_header():
    st.markdown(
        f'<div class="main-header">'
        f'<div style="background:{TA_NAVY};border-left:6px solid {TA_ORANGE};'
        f'border-radius:8px;padding:14px 22px;">'
        f'<div style="font-size:1.4rem;font-weight:700;color:#FFFFFF;">'
        f'DMT — Data Migration Toolkit</div>'
        f'<div style="font-size:.78rem;color:{TXT_SECONDARY};margin-top:2px;">'
        f'Tiger Analytics &middot; Multi-source &middot; Modular &middot; Resumable'
        f'</div></div></div>', unsafe_allow_html=True)


def render_footer():
    st.markdown("---")
    st.markdown(
        f'<p style="text-align:center;color:{TXT_SECONDARY};font-size:0.8rem;">'
        f'Powered by <span style="color:{TA_ORANGE};font-weight:700;">Tiger Analytics</span>'
        f' &middot; DMT v1.0</p>', unsafe_allow_html=True)


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


# ── Sidebar ───────────────────────────────────────────────────────────────────
PAGES = {
    "Dashboard": render_dashboard,
    "Connections": render_connections,
    "Config": render_config,
    "Run": render_run,
    "History": render_history,
    "Monitoring": render_monitoring,
}

with st.sidebar:
    # ── Sticky header with logo ───────────────────────────────────────────────
    import base64
    _logo_b64 = ""
    for logo_name in ("ta_logo_dark.svg", "ta_logo_light.svg"):
        logo_path = _LOGO_DIR / logo_name
        if logo_path.exists():
            _logo_b64 = base64.b64encode(logo_path.read_bytes()).decode()
            break

    if _logo_b64:
        st.markdown(
            f'<div class="sidebar-header">'
            f'<img src="data:image/svg+xml;base64,{_logo_b64}" '
            f'style="width:85%;max-width:180px;display:block;margin:0 auto">'
            f'</div>', unsafe_allow_html=True)
    else:
        st.markdown(
            f'<div class="sidebar-header">'
            f'<h4 style="color:{TXT_PRIMARY};text-align:center;margin:0">DMT v1</h4>'
            f'</div>', unsafe_allow_html=True)

    # ── Navigation buttons ────────────────────────────────────────────────────
    NAV_ICONS = {
        "Dashboard": "📊",
        "Config": "⚙️",
        "Run": "▶️",
        "History": "📜",
        "Monitoring": "🩺",
    }
    for page_name in NAV_ICONS.keys():
        icon = NAV_ICONS[page_name]
        is_active = st.session_state.current_page == page_name
        btn_type = "primary" if is_active else "secondary"
        if st.button(f"{icon} {page_name}", key=f"nav_{page_name}",
                     use_container_width=True, type=btn_type):
            st.session_state.current_page = page_name
            st.rerun()
    selected = st.session_state.current_page

    st.markdown(f"<hr style='border-color:{BORDER}'>", unsafe_allow_html=True)

    # ── Source Connection Selector ────────────────────────────────────────────
    st.markdown(
        f'<div style="font-size:.65rem;letter-spacing:2px;text-transform:uppercase;'
        f'color:{TXT_LABEL};font-weight:700;margin-bottom:4px">Source Connection</div>',
        unsafe_allow_html=True)

    # Fetch profiles (cached in session to avoid repeated queries)
    if "_profiles_list" not in st.session_state:
        try:
            _cur = _sf_conn().cursor()
            import connection_manager as _cm
            st.session_state["_profiles_list"] = _cm.list_profiles(_cur, active_only=True)
            _cur.close()
        except Exception:
            st.session_state["_profiles_list"] = []

    _profiles = st.session_state.get("_profiles_list", [])
    _profile_options = ["All Connections"] + [p["PROFILE_NAME"] for p in _profiles]

    if "selected_profile" not in st.session_state:
        st.session_state["selected_profile"] = "All Connections"

    sel_profile = st.selectbox(
        "source_conn",
        _profile_options,
        index=_profile_options.index(st.session_state["selected_profile"])
        if st.session_state["selected_profile"] in _profile_options else 0,
        label_visibility="collapsed",
        key="sidebar_profile_select",
    )
    st.session_state["selected_profile"] = sel_profile

    # Show selected profile info
    if sel_profile != "All Connections":
        _sel_p = next((p for p in _profiles if p["PROFILE_NAME"] == sel_profile), None)
        if _sel_p:
            st.markdown(
                f'<div style="font-family:monospace;font-size:.65rem;color:{TXT_SECONDARY};'
                f'margin-top:4px;line-height:1.6">'
                f'{_sel_p.get("SOURCE_TYPE","?")} · {_sel_p.get("HOST","?")}:{_sel_p.get("PORT","?")}'
                f'</div>', unsafe_allow_html=True)

    # Manage Connections button (replaces nav entry)
    if st.button("🔌 Manage Connections", key="nav_Connections",
                 use_container_width=True,
                 type="primary" if st.session_state.current_page == "Connections" else "secondary"):
        st.session_state.current_page = "Connections"
        selected = "Connections"
        st.rerun()

    st.markdown(f"<hr style='border-color:{BORDER}'>", unsafe_allow_html=True)
    st.session_state["_ai_on"] = st.toggle(
        "🤖 AI Assist", value=st.session_state.get("_ai_on", False),
        key="sidebar_ai_toggle",
        help="Enable Cortex-powered config recommendations and failure explanations.")
    if st.session_state["_ai_on"]:
        st.session_state["_ai_model"] = st.selectbox(
            "Cortex model", AI_MODELS,
            index=AI_MODELS.index(st.session_state.get("_ai_model", AI_MODEL))
            if st.session_state.get("_ai_model", AI_MODEL) in AI_MODELS else 0,
            key="sidebar_ai_model")

    st.markdown(f"<hr style='border-color:{BORDER}'>", unsafe_allow_html=True)

    # ── Connection status card (green/red dot + user + role) ──────────────────
    if "_conn_status" not in st.session_state:
        with st.spinner("Connecting…"):
            st.session_state["_conn_status"] = check_snowflake()

    ok, info = st.session_state["_conn_status"]
    dot_color = ST_SUCCESS if ok else ST_FAILED
    status_label = "Connected" if ok else "Disconnected"

    if ok:
        user = info.get("user", "?")
        role = info.get("role", "?")
        wh = info.get("warehouse", "?")
        st.markdown(
            f'<div class="conn-card">'
            f'<div class="conn-row">'
            f'<div class="conn-dot" style="background:{dot_color}"></div>'
            f'<span class="conn-title">{status_label}</span></div>'
            f'<div class="conn-detail">'
            f'👤 {user}<br>'
            f'🛡️ {role}<br>'
            f'🏭 {wh}'
            f'</div></div>', unsafe_allow_html=True)
    else:
        err = info.get("error", "Unknown error")
        env = info.get("env", "")
        st.markdown(
            f'<div class="conn-card" style="border-color:{ST_FAILED}44">'
            f'<div class="conn-row">'
            f'<div class="conn-dot" style="background:{dot_color}"></div>'
            f'<span class="conn-title" style="color:{ST_FAILED}">{status_label}</span></div>'
            f'<div class="conn-detail" style="color:{ST_FAILED}">'
            f'{err}<br><span style="color:{TXT_LABEL}">{env}</span>'
            f'</div></div>', unsafe_allow_html=True)

    st.markdown(f"<br>", unsafe_allow_html=True)
    st.caption("DMT v1.0.0 · Tiger Analytics")

# ── Main content ──────────────────────────────────────────────────────────────
render_header()

conn = None
ok, _ = st.session_state.get("_conn_status", (False, {}))
if ok:
    conn = get_sf()

# Status notification from previous run
_toast = st.session_state.pop("_toast", None)
if _toast:
    _rc, _label = _toast
    if _rc == 0:
        st.toast(f"{_label} completed successfully", icon="✅")
    else:
        st.toast(f"{_label} finished with failures (exit {_rc})", icon="❌")

if conn:
    PAGES[selected](conn)
else:
    st.warning("Cannot connect to Snowflake. Check environment variables or run inside Snowsight.")
    st.info("Required: `SF_ACCOUNT`, `SF_USER`, `SF_PASSWORD`, `SF_ROLE`, `SF_WAREHOUSE`")

render_footer()
