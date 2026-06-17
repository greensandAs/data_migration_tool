
"""app.py — MySQL → Snowflake Historical Load Control Panel (Tiger Analytics).

1:1 lift-and-shift into a single layer: <MYSQL_SCHEMA>.RAW.<table> (no SILVER/SCD2).
Start with:  streamlit run app.py
Connections come from the environment (.env): MYSQL_* and SF_*.

Contrast strategy: .streamlit/config.toml pins base=light so native widgets render
with dark text on white; custom HTML cards use explicit dark surfaces with light
text — guaranteed contrast regardless of theme detection.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import streamlit as st

import loader
import validator

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

HERE = Path(__file__).parent
CONFIG_PATH = HERE / "histload_config.json"
GUIDE_PATH = HERE / "USER_GUIDE.md"
_LOGO_DIR = HERE / "assets" / "logos"
# Fall back to the workspace design-skill logos when the app folder has none.
_SKILL_LOGO_DIR = (HERE.parent / ".snowflake" / "cortex" / "skills"
                   / "streamlit_frontend_design_skill" / "assets" / "logos")
_FAVICON = _LOGO_DIR / "ta_favicon.png"

# ── Brand & semantic color tokens (FIXED — not theme-adaptive) ────────────────
TA_ORANGE = "#F15A22"
TA_ORANGE_DARK = "#C94A18"
TA_NAVY = "#0F1B2D"        # card / surface background
TA_NAVY_LIGHT = "#162032"  # slightly lighter surface

TXT_PRIMARY = "#F0F4F8"    # near-white on dark
TXT_SECONDARY = "#A8B8CC"  # muted on dark
TXT_LABEL = "#7E96B0"      # uppercase labels on dark

ST_SUCCESS = "#34D058"
ST_FAILED = "#F85149"
ST_SKIPPED = "#F0A742"
ST_PENDING = "#58A6FF"

BORDER = "#263245"

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Historical Load | Tiger Analytics",
    page_icon=str(_FAVICON) if _FAVICON.exists() else "❄️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Global CSS ────────────────────────────────────────────────────────────────
st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Source+Sans+Pro:wght@400;600;700&family=JetBrains+Mono:wght@400;600&display=swap');
html, body, [class*="css"] {{ font-family: 'Source Sans Pro', 'Segoe UI', Arial, sans-serif; }}
#MainMenu, footer, header {{ visibility: hidden; }}
.block-container {{ padding-top: 1rem; padding-bottom: 2rem; }}

/* Sidebar */
section[data-testid="stSidebar"] {{ background: {TA_NAVY} !important; border-right: 3px solid {TA_ORANGE}; }}
/* Light text ONLY for labels and our own markdown — never the native input
   widgets (selectbox/value/dropdown), which keep the readable light theme
   (dark text on white) so they don't go light-on-light. */
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] .stMarkdown,
section[data-testid="stSidebar"] .stMarkdown * {{ color: {TXT_PRIMARY} !important; }}
section[data-testid="stSidebar"] .stCaption,
section[data-testid="stSidebar"] .stCaption * {{ color: {TXT_SECONDARY} !important; }}

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

/* Connection dot */
.dot {{ display: inline-block; width: 9px; height: 9px; border-radius: 50%; margin-right: 6px; vertical-align: middle; }}

/* Primary button (stable selector — avoids volatile data-testid) */
.stButton > button[kind="primary"] {{ background: {TA_ORANGE} !important; color: #fff !important; border: none !important; font-weight: 700 !important; letter-spacing: .3px; }}
.stButton > button[kind="primary"]:hover {{ background: {TA_ORANGE_DARK} !important; }}

/* Namespace info box */
.ns-box {{ background: {TA_NAVY}; border: 1px solid {BORDER}; border-radius: 6px; padding: 10px 14px; font-family: 'JetBrains Mono', monospace; font-size: .73rem; line-height: 1.8; margin-top: 8px; }}
.ns-box .ns-label {{ color: {TXT_LABEL}; }}
.ns-box .ns-value {{ color: {TXT_PRIMARY}; }}

/* Config tab — schema group header */
.schema-group {{ background: {TA_NAVY}; border: 1px solid {BORDER}; border-radius: 8px; padding: 12px 18px; margin: 16px 0 8px 0; display: flex; align-items: center; justify-content: space-between; }}
.schema-group .sg-title {{ font-size: .82rem; font-weight: 700; color: {TXT_PRIMARY}; }}
.schema-group .sg-count {{ font-size: .68rem; color: {TXT_SECONDARY}; background: {TA_NAVY_LIGHT}; border: 1px solid {BORDER}; border-radius: 12px; padding: 2px 10px; margin-left: 10px; }}
.schema-group .sg-meta {{ font-size: .68rem; color: {TXT_LABEL}; }}

/* Config tab — mini summary cards */
.cfg-summary {{ display: flex; gap: 12px; margin-bottom: 16px; }}
.cfg-mini-card {{ flex: 1; background: {TA_NAVY}; border: 1px solid {BORDER}; border-radius: 8px; padding: 12px 16px; text-align: center; }}
.cfg-mini-card .mc-val {{ font-family: 'JetBrains Mono', monospace; font-size: 1.4rem; font-weight: 700; line-height: 1.2; }}
.cfg-mini-card .mc-lbl {{ font-size: .65rem; letter-spacing: 1.5px; text-transform: uppercase; color: {TXT_LABEL}; margin-top: 4px; font-weight: 600; }}
</style>
""", unsafe_allow_html=True)


# ── Config helpers ────────────────────────────────────────────────────────────
def _default_config() -> dict:
    return {"export_dir": str(HERE / "export"), "tables": []}


def load_config() -> dict:
    try:
        with open(CONFIG_PATH) as f:
            d = json.load(f)
        if d:
            return d
    except Exception:  # noqa: BLE001
        pass
    return _default_config()


def save_config(cfg: dict) -> None:
    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)


def sf_conf() -> dict:
    out = {}
    for k, e in {"account": "SF_ACCOUNT", "user": "SF_USER", "password": "SF_PASSWORD",
                 "role": "SF_ROLE", "warehouse": "SF_WAREHOUSE",
                 "database": "SF_DATABASE", "schema": "SF_SCHEMA"}.items():
        if os.getenv(e):
            out[k] = os.getenv(e)
    out.setdefault("database", "HISTLOAD_DB")
    out.setdefault("schema", "META")
    return out


def my_conf() -> dict:
    return {
        "host": os.getenv("MYSQL_HOST", "localhost"),
        "port": int(os.getenv("MYSQL_PORT", "3306")),
        "user": os.getenv("MYSQL_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD", ""),
    }


# ── Connections (pooled via st.cache_resource — no per-rerun handshakes) ──────
@st.cache_resource(show_spinner=False)
def _sf_conn():
    return loader.get_sf_conn(sf_conf())


@st.cache_resource(show_spinner=False)
def _mysql_conn():
    import mysql.connector
    c = my_conf()
    return mysql.connector.connect(
        host=c["host"], port=int(c["port"]), user=c["user"], password=c["password"])


def get_sf():
    """Cached Snowflake connection; reconnects transparently if it expired.
    Do NOT .close() the returned connection (it is pooled and reused)."""
    conn = _sf_conn()
    try:
        cur = conn.cursor(); cur.execute("SELECT 1"); cur.fetchone(); cur.close()
        return conn
    except Exception:  # noqa: BLE001 — stale session, rebuild it
        _sf_conn.clear()
        return _sf_conn()


def get_mysql():
    """Cached MySQL connection; pings/reconnects if dropped. Do NOT .close() it."""
    conn = _mysql_conn()
    try:
        conn.ping(reconnect=True, attempts=1, delay=0)
        return conn
    except Exception:  # noqa: BLE001
        _mysql_conn.clear()
        return _mysql_conn()


def reset_connections():
    """Drop the pooled connections so the next get_* rebuilds them."""
    _sf_conn.clear()
    _mysql_conn.clear()


def check_connections() -> dict:
    out = {}
    try:
        con = get_mysql(); cur = con.cursor()
        cur.execute("SELECT VERSION()")
        ver = cur.fetchone()[0]; cur.close()
        out["MySQL"] = (True, str(ver))
    except Exception as e:  # noqa: BLE001
        out["MySQL"] = (False, str(e)[:80])
    try:
        con = get_sf(); cur = con.cursor()
        cur.execute("SELECT CURRENT_ACCOUNT(), CURRENT_WAREHOUSE()")
        acct, wh = cur.fetchone(); cur.close()
        out["Snowflake"] = (True, f"{acct} / {wh}")
    except Exception as e:  # noqa: BLE001
        out["Snowflake"] = (False, str(e)[:80])
    return out


# ── AI (Snowflake Cortex) helpers — gated by the sidebar toggle ───────────────
AI_MODEL = "llama3.1-70b"  # default Cortex model.
# Common Cortex models (availability varies by region) — selectable in the sidebar.
AI_MODELS = ["llama3.1-70b", "llama3.1-8b", "llama3.1-405b", "mistral-large2",
             "mixtral-8x7b", "snowflake-arctic", "claude-3-5-sonnet",
             "reka-flash", "gemma-7b"]


def ai_enabled() -> bool:
    """AI features run only when the user turns on the sidebar toggle (saves credits)."""
    return bool(st.session_state.get("_ai_on", False))


def cortex_complete(prompt: str, model: str = None) -> str:
    """Single Cortex COMPLETE call via the existing Snowflake connection."""
    model = model or st.session_state.get("_ai_model", AI_MODEL)
    con = get_sf(); cur = con.cursor()
    try:
        cur.execute("SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s)", (model, prompt))
        return (cur.fetchone()[0] or "").strip()
    finally:
        cur.close()  # pooled connection stays open


def _mysql_table_meta(source_db: str, source_table: str) -> dict:
    """Column types + primary key for a MySQL table (for the config recommender)."""
    con = get_mysql(); cur = con.cursor()
    try:
        cur.execute(
            "SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_KEY, EXTRA "
            "FROM information_schema.columns "
            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION",
            (source_db, source_table))
        cols = [{"name": r[0], "type": r[1], "key": r[2], "extra": r[3]}
                for r in cur.fetchall()]
        cur.execute(
            "SELECT COLUMN_NAME FROM information_schema.key_column_usage "
            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND CONSTRAINT_NAME='PRIMARY' "
            "ORDER BY ORDINAL_POSITION", (source_db, source_table))
        pk = [r[0] for r in cur.fetchall()]
    finally:
        cur.close()  # pooled connection stays open
    return {"columns": cols, "primary_key": pk}


def mysql_columns_cached(source_db: str, source_table: str) -> list:
    """Column names for a MySQL table, cached per (db, table) in session_state so
    we hit MySQL only once. Returns [] on any error (caller falls back to text)."""
    if not (source_db and source_table):
        return []
    key = f"_cols::{source_db}::{source_table}"
    if key not in st.session_state:
        try:
            st.session_state[key] = [c["name"]
                                     for c in _mysql_table_meta(source_db, source_table)["columns"]]
        except Exception:  # noqa: BLE001
            st.session_state[key] = []
    return st.session_state[key]


def ai_recommend_config(source_db: str, source_table: str) -> dict:
    """Ask Cortex to recommend load settings from the table's metadata.

    Returns {"json": dict|None, "raw": str}. json has keys: load_type,
    watermark_col, watermark_type, merge_keys, partition_col, rationale.
    """
    meta = _mysql_table_meta(source_db, source_table)
    cols_txt = "\n".join(
        f"- {c['name']} {c['type']} key={c['key']} extra={c['extra']}"
        for c in meta["columns"])
    prompt = (
        "You are a data migration assistant for a MySQL->Snowflake replication tool.\n"
        f"Source table: {source_db}.{source_table}\n"
        f"Primary key: {meta['primary_key'] or 'NONE'}\n"
        f"Columns:\n{cols_txt}\n\n"
        "Recommend load settings. Rules: use 'incremental' only if there is a "
        "reliable cursor — a timestamp column that updates on change (watermark_type "
        "'time') OR a monotonic AUTO_INCREMENT integer PK (watermark_type 'id', "
        "INSERTS ONLY). Otherwise 'full'. merge_keys = full uniqueness grain (the PK, "
        "composite if needed). partition_col = an integer column for parallel reads or null.\n"
        "Respond with ONLY a JSON object, no prose, with keys: load_type "
        "('full'|'incremental'), watermark_col (string|null), watermark_type "
        "('time'|'id'|null), merge_keys (array of column names), partition_col "
        "(string|null), rationale (one short sentence).")
    raw = cortex_complete(prompt)
    parsed = None
    try:
        txt = raw.strip()
        if "```" in txt:  # strip markdown fences if the model added them
            txt = txt.split("```")[1].lstrip("json").strip() if "```" in txt else txt
        start, end = txt.find("{"), txt.rfind("}")
        if start != -1 and end != -1:
            parsed = json.loads(txt[start:end + 1])
    except Exception:  # noqa: BLE001
        parsed = None
    return {"json": parsed, "raw": raw}


# ── Namespace helpers (mirror loader.py) ──────────────────────────────────────
def raw_fqn(tbl):
    return f"{loader.target_db(tbl['source_db'])}.RAW.{tbl['target_table']}"


# ── UI helpers ────────────────────────────────────────────────────────────────
def status_icon(s):
    return {"success": "✅", "failed": "❌", "skipped": "⏭️", None: "⏳"}.get(s, "⏳")


def load_type_pill(tbl):
    lt = tbl.get("load_type", "full")
    cls = "pill-incr" if lt == "incremental" else "pill-full"
    return f'<span class="pill {cls}">{lt}</span>'


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


def run_subprocess_stream(args: list[str]):
    log_area = st.empty()
    raw_lines = []  # store RAW lines; colorize only for display so the [db.table]
    # prefix stays parseable for the Group-by-table view.
    proc = subprocess.Popen(
        [sys.executable, "-u", *args], cwd=str(HERE),
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1,
        env={**os.environ, "PYTHONUNBUFFERED": "1"})
    for raw in proc.stdout:
        raw_lines.append(raw.rstrip())
        disp = "<br>".join(colorize_log(l) for l in raw_lines[-60:])
        log_area.markdown(f'<div class="log-box">{disp}</div>', unsafe_allow_html=True)
    proc.wait()
    return proc.returncode, "\n".join(raw_lines)


# ── Background job runner (non-blocking; supports a Stop button) ──────────────
# The orchestrator runs in a daemon thread that appends stdout lines to a shared
# list in session_state, so the UI stays responsive, can show a live tail, and
# can terminate the process. Spawn errors are captured (not raised) so hosted /
# containerized environments fail gracefully instead of crashing the app.
def start_job(args: list[str], label: str = "Load"):
    job = {"args": args, "label": label, "lines": [], "rc": None,
           "running": True, "proc": None, "error": None, "stopped": False,
           "consumed": False}
    try:
        proc = subprocess.Popen(
            [sys.executable, "-u", *args], cwd=str(HERE),
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1,
            env={**os.environ, "PYTHONUNBUFFERED": "1"})
    except Exception as ex:  # noqa: BLE001 — surface spawn failure in the UI
        job.update(running=False, rc=-1,
                   error=f"Could not start the job ({sys.executable}): {ex}")
        st.session_state["_job"] = job
        return
    job["proc"] = proc
    lines = job["lines"]

    def _reader():
        try:
            for raw in proc.stdout:
                lines.append(raw.rstrip())
        except Exception:  # noqa: BLE001
            pass
        proc.wait()
        job["rc"] = proc.returncode
        job["running"] = False

    t = threading.Thread(target=_reader, daemon=True)
    t.start()
    job["thread"] = t
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
        except Exception:  # noqa: BLE001
            proc.kill()
    except Exception:  # noqa: BLE001
        pass
    job["running"] = False
    job["stopped"] = True
    job["rc"] = proc.returncode if proc.returncode is not None else -15


def job_running() -> bool:
    job = st.session_state.get("_job")
    return bool(job and job.get("running"))


def _full_rerun():
    """Rerun the whole app (works whether or not st.rerun supports scope=)."""
    try:
        st.rerun(scope="app")
    except TypeError:
        st.rerun()


# Auto-refreshing live panel (fragment reruns just itself, so the Stop button
# stays painted and clickable). Falls back to sleep+rerun on older Streamlit.
_fragment = getattr(st, "fragment", None) or getattr(st, "experimental_fragment", None)


def _render_running_panel():
    job = st.session_state.get("_job")
    if not (job and job.get("running")):
        _full_rerun()  # finished -> full rerun so the main flow consumes the result
        return
    c1, c2 = st.columns([1, 4])
    if c1.button("⏹️ Stop", type="secondary", use_container_width=True, key="stop_job_btn"):
        stop_job()
        _full_rerun()
        return
    c2.warning(f"⏳ {job.get('label', 'Job')} running… ({len(job['lines'])} log lines)")
    tail = job["lines"][-60:]
    colored = "<br>".join(colorize_log(l) for l in tail) or "starting…"
    st.markdown(f'<div class="log-box">{colored}</div>', unsafe_allow_html=True)


live_running_panel = (_fragment(run_every=1.0)(_render_running_panel)
                      if _fragment else None)


def _render_autorefresh_tick():
    """Lightweight poller: full-rerun the app once ~60s after the last parity
    render. Non-blocking — the fragment re-runs only itself every few seconds."""
    last = st.session_state.get("_parity_rendered_at", 0)
    if last and (time.time() - last) >= 60:
        st.session_state["_parity_rendered_at"] = time.time()
        _full_rerun()


autorefresh_ticker = (_fragment(run_every=5.0)(_render_autorefresh_tick)
                      if _fragment else None)


def _group_log_by_table(text: str, known: set | None = None) -> dict:
    """Split a finished run log into {table: [lines]} using the [db.table] prefix
    added during runs. Lines whose prefix is a known table go to that table;
    everything else (run banners, untagged lines) goes under '· general ·'.
    """
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


def _logo_b64() -> str:
    import base64
    for d in (_LOGO_DIR, _SKILL_LOGO_DIR):
        for name in ("ta_logo_dark.svg", "ta_logo_light.svg"):
            p = d / name
            try:
                if p.exists():
                    return base64.b64encode(p.read_bytes()).decode()
            except Exception:  # noqa: BLE001
                continue
    return ""


def render_header():
    st.markdown(
        f'<div style="background:{TA_NAVY};border-left:6px solid {TA_ORANGE};'
        f'border-radius:8px;padding:16px 22px;margin-bottom:18px;">'
        f'<div style="font-size:1.5rem;font-weight:700;color:#FFFFFF;">'
        f'MySQL &#8594; Snowflake — Historical Load</div>'
        f'<div style="font-size:.82rem;color:{TXT_SECONDARY};margin-top:2px;">'
        f'Tiger Analytics &middot; 1:1 lift-and-shift &middot; &lt;schema&gt;.RAW.&lt;table&gt;'
        f'</div></div>', unsafe_allow_html=True)


def render_footer():
    st.markdown("---")
    st.markdown(
        f'<p style="text-align:center;color:{TXT_SECONDARY};font-size:0.8rem;">'
        f'Powered by <span style="color:{TA_ORANGE};font-weight:700;">Tiger Analytics</span>'
        , unsafe_allow_html=True)


# ── Load config ───────────────────────────────────────────────────────────────
cfg = load_config()

# Completion toast — set after a run, shown once on the following rerun.
_toast = st.session_state.pop("_toast", None)
if _toast:
    _rc, _label = _toast
    if _rc == 0:
        st.toast(f"✅ {_label} completed successfully", icon="✅")
    else:
        st.toast(f"❌ {_label} finished with failures (exit {_rc})", icon="⚠️")


def _add_table_form():
    """Body of the manual 'add table' popup. Appends to config + reruns."""
    st.caption("Add a single table entry to histload_config.json")
    c1, c2 = st.columns(2)
    source_db = c1.text_input("Source schema (MySQL db)", key="add_db")
    source_table = c2.text_input("Source table", key="add_tbl")
    target_table = st.text_input("Target table (Snowflake)", key="add_tgt",
                                 help="Defaults to UPPER(source table) if blank")
    c3, c4 = st.columns(2)
    primary_key = c3.text_input("Primary key", key="add_pk")
    # Watermark column: dropdown of the source table's columns when reachable,
    # else free text. Avoids typos / wrong type guesses.
    _wm_cols = mysql_columns_cached(source_db.strip(), source_table.strip())
    if _wm_cols:
        _opts = ["— none —"] + _wm_cols
        _sel = c4.selectbox("Watermark column", _opts, key="add_wm_sel",
                            help="Pick the column to track for incremental loads")
        watermark_col = "" if _sel == "— none —" else _sel
    else:
        watermark_col = c4.text_input(
            "Watermark column (optional)", key="add_wm",
            help="Fill source schema + table to pick from a dropdown")
    merge_keys_raw = st.text_input(
        "Merge keys (composite, comma-separated — blank = primary key)",
        key="add_mk",
        help="Full uniqueness grain used for MERGE/dedupe, e.g. EMP_NO, FROM_DATE")
    c5, c6 = st.columns(2)
    load_type = c5.selectbox("Load type", ["full", "incremental"], key="add_lt")
    wm_type = c6.selectbox(
        "Watermark type", ["auto", "time", "id"], key="add_wt",
        help="auto = detect from column type · time = timestamp (inserts+updates) · "
             "id = monotonic integer PK (inserts only)")
    c7, c8 = st.columns(2)
    reconcile = c7.checkbox("Reconcile deletes", key="add_rec")
    active = c8.checkbox("Active", value=True, key="add_act")

    if st.button("➕ Add table", type="primary", key="add_submit"):
        if not (source_db.strip() and source_table.strip()):
            st.error("Source schema and source table are required.")
            return
        tgt = (target_table.strip() or source_table.strip()).upper()
        pk_u = primary_key.strip().upper() or None
        wm_u = watermark_col.strip().upper() or None
        mk_u = [c.strip().upper() for c in merge_keys_raw.split(",") if c.strip()]
        entry = {
            "source_db": source_db.strip(), "source_table": source_table.strip(),
            "target_table": tgt, "primary_key": pk_u,
            "load_type": load_type, "watermark_col": wm_u,
            "last_loaded_at": None,
            "last_loaded_key": None,
            "partition_col": pk_u, "partition_num": 8,
            "reconcile": reconcile, "active": active, "last_run_status": None,
            "rows_per_file": 1000000,
        }
        if mk_u:
            entry["merge_keys"] = mk_u
        if wm_type in ("time", "id"):
            entry["watermark_type"] = wm_type
        live = load_config()
        live.setdefault("tables", [])
        live["tables"] = [t for t in live["tables"]
                          if not (t.get("source_db") == entry["source_db"]
                                  and t.get("source_table") == entry["source_table"])]
        live["tables"].append(entry)
        save_config(live)
        st.success(f"Added {entry['source_db']}.{entry['source_table']}")
        st.rerun()


# Wrap as a modal dialog when supported (Streamlit ≥1.31), else inline fallback.
if hasattr(st, "dialog"):
    add_table_dialog = st.dialog("➕ Add Table Manually")(_add_table_form)
else:
    add_table_dialog = None


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    b64 = _logo_b64()
    if b64:
        st.markdown(
            f'<img src="data:image/svg+xml;base64,{b64}" '
            f'style="width:80%;max-width:160px;margin:10px auto 18px;display:block">',
            unsafe_allow_html=True)
    else:
        st.markdown(f"<h3 style='color:{TXT_PRIMARY}'>❄️ Historical Load Hub</h3>",
                    unsafe_allow_html=True)
    st.markdown(f"<hr style='border-color:{BORDER}'>", unsafe_allow_html=True)

    # ── Top: active-config metrics + User Guide ──────────────────────────────
    active_tbls = [t for t in cfg.get("tables", []) if t.get("active", True)]
    full_n = sum(1 for t in active_tbls if t.get("load_type") == "full")
    incr_n = sum(1 for t in active_tbls if t.get("load_type") == "incremental")
    never_n = sum(1 for t in active_tbls if not t.get("last_loaded_at"))
    failed_n = sum(1 for t in active_tbls if t.get("last_run_status") == "failed")
    st.markdown(f"""
        <div class="metric-card">
          <div class="label">Active Tables</div>
          <div class="value">{len(active_tbls)}</div>
          <div class="sub">{full_n} full &nbsp;·&nbsp; {incr_n} incremental</div>
        </div>
        <div class="metric-card">
          <div class="label">Awaiting First Load</div>
          <div class="value">{never_n}</div>
          <div class="sub">last_loaded_at = null</div>
        </div>
    """, unsafe_allow_html=True)
    if failed_n:
        st.markdown(
            f'<div style="background:#2e0d0d;border:1px solid {ST_FAILED}44;border-radius:6px;'
            f'padding:8px 12px;font-size:.82rem;color:{ST_FAILED};margin-top:8px">'
            f'⚠️ {failed_n} table(s) failed last run</div>', unsafe_allow_html=True)

    if st.button("📖 User Guide", use_container_width=True):
        st.session_state["_view"] = "guide"
        st.rerun()

    # AI Assist toggle — features call Snowflake Cortex only when ON (saves credits).
    st.session_state["_ai_on"] = st.toggle(
        "🤖 AI Assist", value=st.session_state.get("_ai_on", False),
        help="Enable Cortex-powered failure explanations and config recommendations. "
             "Off by default to avoid consuming credits.")
    if st.session_state["_ai_on"]:
        st.session_state["_ai_model"] = st.selectbox(
            "Cortex model", AI_MODELS,
            index=AI_MODELS.index(st.session_state.get("_ai_model", AI_MODEL))
            if st.session_state.get("_ai_model", AI_MODEL) in AI_MODELS else 0,
            help="Pick a model available in your Snowflake region.")

    st.markdown(f"<hr style='border-color:{BORDER}'>", unsafe_allow_html=True)
    dbs = sorted({t["source_db"].upper() for t in cfg.get("tables", [])})
    if dbs:
        st.markdown(
            f'<div style="font-size:.68rem;letter-spacing:2px;text-transform:uppercase;'
            f'color:{TXT_LABEL};font-weight:700;margin-bottom:8px">Namespace Map</div>',
            unsafe_allow_html=True)
        for db in dbs[:6]:
            st.markdown(
                f'<div style="font-family:monospace;font-size:.7rem;'
                f'color:{TXT_SECONDARY};margin:3px 0">'
                f'<span style="color:{TA_ORANGE}">{db}</span> → {db}.RAW.*</div>',
                unsafe_allow_html=True)

    # ── Bottom: connection status + re-check ─────────────────────────────────
    st.markdown("<div style='height:18px'></div>", unsafe_allow_html=True)
    st.markdown(f"<hr style='border-color:{BORDER}'>", unsafe_allow_html=True)
    st.markdown(
        f'<div style="font-size:.68rem;letter-spacing:2px;text-transform:uppercase;'
        f'color:{TXT_LABEL};font-weight:700;margin-bottom:6px">Connections</div>',
        unsafe_allow_html=True)
    if "_conn" not in st.session_state:
        with st.spinner("Checking connections…"):
            st.session_state["_conn"] = check_connections()
    for name, (ok, detail) in st.session_state["_conn"].items():
        color = ST_SUCCESS if ok else ST_FAILED
        st.markdown(
            f'<div style="margin:5px 0;font-size:.82rem;color:{TXT_PRIMARY}">'
            f'<span class="dot" style="background:{color}"></span>'
            f'<b>{name}</b>: <span style="color:{TXT_SECONDARY}">{detail}</span></div>',
            unsafe_allow_html=True)
    if st.button("🔌 Re-check Connections", use_container_width=True):
        reset_connections()  # drop pooled conns so they're rebuilt fresh
        st.session_state["_conn"] = check_connections()
        st.rerun()

# ── Main ──────────────────────────────────────────────────────────────────────
if st.session_state.get("_view") == "guide":
    render_header()
    if st.button("⬅ Back to App"):
        st.session_state["_view"] = "app"
        st.rerun()
    try:
        st.markdown(GUIDE_PATH.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        st.error(f"Could not load USER_GUIDE.md: {e}")
    render_footer()
    st.stop()

render_header()

tab_dash, tab_run, tab_cfg_ed, tab_hist, tab_counts = st.tabs([
    "📊 Dashboard", "▶️ Run", "⚙️ Config", "📜 History", "🔢 Counts"])

# ── TAB 1 — DASHBOARD ─────────────────────────────────────────────────────────
with tab_dash:
    tables = cfg.get("tables", [])
    st.markdown('<div class="section-header">Run Summary</div>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    for col, label, val, color in [
        (c1, "SUCCESS", sum(1 for t in tables if t.get("last_run_status") == "success"), ST_SUCCESS),
        (c2, "FAILED", sum(1 for t in tables if t.get("last_run_status") == "failed"), ST_FAILED),
        (c3, "PENDING", sum(1 for t in tables if not t.get("last_run_status")), ST_PENDING),
        (c4, "SKIPPED", sum(1 for t in tables if t.get("last_run_status") == "skipped"), ST_SKIPPED),
    ]:
        col.markdown(f"""<div class="metric-card" style="border-left:4px solid {color}">
            <div class="label">{label}</div>
            <div class="value" style="color:{color}">{val}</div>
            <div class="sub">tables</div></div>""", unsafe_allow_html=True)

    if not tables:
        st.info("No tables configured yet. Use the **Config** tab to generate or add tables.")
    else:
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown('<div class="section-header">Per-Table Status</div>',
                    unsafe_allow_html=True)
        # Search + filter + view mode so the dashboard scales to 100s of tables.
        d1, d2, d3 = st.columns([3, 2, 2])
        dq = d1.text_input("🔍 Search tables", placeholder="Filter by name…",
                           label_visibility="collapsed", key="dash_search")
        dstatus = d2.selectbox("Status", ["All", "success", "failed", "pending", "skipped"],
                               label_visibility="collapsed", key="dash_status")
        dview = d3.selectbox("View", ["Table", "Cards"],
                             index=0 if len(tables) > 50 else 1,
                             label_visibility="collapsed", key="dash_view")

        def _dash_match(t):
            s = t.get("last_run_status") or "pending"
            if dstatus != "All" and s != dstatus:
                return False
            if dq and dq.strip().lower() not in (
                    f"{t.get('source_db','')}.{t.get('source_table','')} "
                    f"{t.get('target_table','')}").lower():
                return False
            return True

        flt = [t for t in tables if _dash_match(t)]
        st.caption(f"Showing {len(flt)} of {len(tables)} tables")

        if not flt:
            st.info("No tables match the current filters.")
        elif dview == "Table":
            import pandas as pd
            df = pd.DataFrame([{
                "Source": f"{t['source_db']}.{t['source_table']}",
                "Target": raw_fqn(t),
                "Load": t.get("load_type", "full"),
                "Status": (t.get("last_run_status") or "pending"),
                "Last Sync": str(t.get("last_loaded_at"))[:19] if t.get("last_loaded_at") else "Never",
                "PK": t.get("primary_key") or "—",
                "Active": "✅" if t.get("active", True) else "—",
                "Reconcile": "✅" if t.get("reconcile") else "",
            } for t in flt])
            st.dataframe(df, use_container_width=True, hide_index=True, height=480)
        else:
            PAGE = 20
            npages = max(1, (len(flt) + PAGE - 1) // PAGE)
            pg = st.number_input("Page", 1, npages, 1, key="dash_page") if npages > 1 else 1
            page_items = flt[(pg - 1) * PAGE: pg * PAGE]
            cols = st.columns(2)
            for i, tbl in enumerate(page_items):
                status = tbl.get("last_run_status") or "pending"
                wm = tbl.get("last_loaded_at")
                inactive = ("" if tbl.get("active", True)
                            else '<span class="pill" style="background:#1a1a1a;color:#666;border:1px solid #333">INACTIVE</span>')
                reconcile = ('<span class="pill" style="background:#0d1e2e;color:#58A6FF;border:1px solid #58A6FF44">RECONCILE</span>'
                             if tbl.get("reconcile") else "")
                with cols[i % 2]:
                    st.markdown(f"""
                        <div class="table-card {status}">
                          <span class="tstatus {status}">{status_icon(status)} {status.upper()}</span>
                          <div class="tname">{tbl['source_db']}.{tbl['source_table']}</div>
                          <div class="tmeta">→ {raw_fqn(tbl)}</div>
                          <div class="tmeta" style="margin-top:6px">
                            {load_type_pill(tbl)} {inactive} {reconcile}</div>
                          <div class="tmeta" style="margin-top:6px;color:{TXT_LABEL}">
                            🕐 {"Last sync: " + str(wm)[:19] if wm else "Never run"}
                            &nbsp;·&nbsp; PK: {tbl.get("primary_key", "—")}
                            &nbsp;·&nbsp; Parts: {tbl.get("partition_num", 8)}</div>
                        </div>
                    """, unsafe_allow_html=True)
            if npages > 1:
                st.caption(f"Page {pg} of {npages}")

# ── TAB 2 — RUN ───────────────────────────────────────────────────────────────
with tab_run:
    _running = job_running()
    st.markdown('<div class="section-header">Pipeline Controls</div>',
                unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    run_clicked = c1.button("▶️ Run Load", type="primary", use_container_width=True,
                            disabled=_running)
    rec_clicked = c2.button(
        "🗑️ Reconcile Deletes", use_container_width=True, disabled=True,
        help="Disabled for now. CLI: python orchestrator.py --reconcile")

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="section-header">Run Single Table</div>',
                unsafe_allow_html=True)
    g1, g2 = st.columns([5, 1])
    table_options = [f"{t['source_db']}.{t['source_table']}"
                     for t in cfg.get("tables", []) if t.get("active", True)]
    selected = g1.selectbox("Table", ["— all tables —"] + table_options,
                            label_visibility="collapsed")
    single_clicked = g2.button("▶️ Run", use_container_width=True,
                               disabled=_running or (selected == "— all tables —"))

    # Launch in the background (non-blocking) so the UI stays responsive.
    if run_clicked and not _running:
        start_job(["orchestrator.py"], "Load")
        st.rerun()
    elif single_clicked and not _running and selected != "— all tables —":
        _, tbl_name = selected.split(".", 1)
        start_job(["orchestrator.py", "--table", tbl_name], f"Load · {selected}")
        st.rerun()

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="section-header">Live Log</div>', unsafe_allow_html=True)
    if "last_log" not in st.session_state:
        st.session_state["last_log"] = "No runs yet. Click ▶️ Run Load to start."

    job = st.session_state.get("_job")

    if job and job.get("running"):
        if live_running_panel is not None:
            # Self-refreshing fragment: Stop button stays painted + clickable.
            live_running_panel()
        else:
            # Fallback for older Streamlit without st.fragment.
            sc1, sc2 = st.columns([1, 4])
            if sc1.button("⏹️ Stop", type="secondary", use_container_width=True):
                stop_job()
                st.rerun()
            sc2.warning(f"⏳ {job.get('label','Job')} running… ({len(job['lines'])} log lines)")
            tail = job["lines"][-60:]
            colored = "<br>".join(colorize_log(l) for l in tail) or "starting…"
            st.markdown(f'<div class="log-box">{colored}</div>', unsafe_allow_html=True)
            time.sleep(1.0)
            st.rerun()
    elif job and not job.get("consumed"):
        # Just finished (or failed to start / stopped): consume once.
        job["consumed"] = True
        if job.get("error"):
            st.session_state["last_log"] = job["error"]
            st.session_state["_toast"] = (1, job.get("label", "Load"))
        else:
            st.session_state["last_log"] = "\n".join(job["lines"]) or "(no output)"
            rc = -1 if job.get("stopped") else (job.get("rc") if job.get("rc") is not None else 1)
            st.session_state["_toast"] = (rc, job.get("label", "Load")
                                          + (" (stopped)" if job.get("stopped") else ""))
        st.rerun()
    else:
        if job and job.get("stopped"):
            st.warning("⏹️ Last job was stopped by the user.")
        group = st.toggle("📑 Group by table", value=False,
                          help="Group the last run's output per table (uses the "
                               "[db.table] prefix from parallel runs).")
        if group:
            known = {f"{t['source_db']}.{t['source_table']}"
                     for t in cfg.get("tables", [])}
            groups = _group_log_by_table(st.session_state["last_log"], known)
            for table in sorted(groups, key=lambda t: (t == "· general ·", t)):
                body_lines = groups[table]
                blob = "<br>".join(colorize_log(ln) for ln in body_lines if ln.strip())
                if not blob:
                    continue
                with st.expander(f"📄 {table}  ({len([l for l in body_lines if l.strip()])} lines)",
                                 expanded=(table != "· general ·")):
                    st.markdown(f'<div class="log-box">{blob}</div>',
                                unsafe_allow_html=True)
        else:
            lines = st.session_state["last_log"].split("\n")
            colored = "<br>".join(colorize_log(line) for line in lines[-60:])
            st.markdown(f'<div class="log-box">{colored}</div>', unsafe_allow_html=True)

# ── TAB 3 — CONFIG EDITOR ─────────────────────────────────────────────────────
with tab_cfg_ed:
    st.markdown('<div class="section-header">Build Configuration</div>',
                unsafe_allow_html=True)
    g1, g2, g3 = st.columns([3, 1, 1])
    schema_input = g1.text_input("MySQL schema", placeholder="e.g. mtest",
                                 label_visibility="collapsed", key="gen_schema")
    if g2.button("⚙️ Generate", use_container_width=True):
        if schema_input.strip():
            rc, _ = run_subprocess_stream(["config_generator.py", schema_input.strip()])
            if rc == 0:
                st.success(f"✅ Config updated for schema: {schema_input}")
                st.rerun()
            else:
                st.error("Generation failed — check log in Run tab")
        else:
            st.warning("Enter a MySQL schema name first.")
    if g3.button("➕ Add table", use_container_width=True):
        if add_table_dialog is not None:
            add_table_dialog()
        else:
            st.session_state["_show_add"] = True
    if add_table_dialog is None and st.session_state.get("_show_add"):
        with st.expander("➕ Add Table Manually", expanded=True):
            _add_table_form()

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="section-header">Table Configuration</div>',
                unsafe_allow_html=True)
    tables = cfg.get("tables", [])
    if not tables:
        st.info("No tables yet. Use **Generate** (from a MySQL schema) or **➕ Add table** above.")
    else:
        # ── Summary hero cards ────────────────────────────────────────────────
        n_active = sum(1 for t in tables if t.get("active", True))
        n_inactive = sum(1 for t in tables if not t.get("active", True))
        n_review = sum(1 for t in tables if t.get("_review"))
        n_total = len(tables)
        st.markdown(f"""<div class="cfg-summary">
            <div class="cfg-mini-card" style="border-left:3px solid {ST_SUCCESS}">
                <div class="mc-val" style="color:{ST_SUCCESS}">{n_active}</div>
                <div class="mc-lbl">Active</div></div>
            <div class="cfg-mini-card" style="border-left:3px solid {TXT_LABEL}">
                <div class="mc-val" style="color:{TXT_LABEL}">{n_inactive}</div>
                <div class="mc-lbl">Inactive</div></div>
            <div class="cfg-mini-card" style="border-left:3px solid {ST_SKIPPED}">
                <div class="mc-val" style="color:{ST_SKIPPED}">{n_review}</div>
                <div class="mc-lbl">Needs Review</div></div>
            <div class="cfg-mini-card" style="border-left:3px solid {ST_PENDING}">
                <div class="mc-val" style="color:{ST_PENDING}">{n_total}</div>
                <div class="mc-lbl">Total</div></div>
        </div>""", unsafe_allow_html=True)

        # ── Filter bar ────────────────────────────────────────────────────────
        schemas = sorted(set(t.get("source_db", "") for t in tables))
        fc1, fc2, fc3 = st.columns([3, 2, 2])
        search_q = fc1.text_input("🔍 Search tables", placeholder="Filter by name…",
                                  label_visibility="collapsed", key="cfg_search")
        schema_filter = fc2.selectbox("Schema", ["All schemas"] + schemas,
                                      label_visibility="collapsed", key="cfg_schema_f")
        status_filter = fc3.selectbox("Status", ["All", "Active", "Inactive", "Needs Review"],
                                       label_visibility="collapsed", key="cfg_status_f")

        # ── Apply filters ─────────────────────────────────────────────────────
        filtered_indices = []
        for i, tbl in enumerate(tables):
            if search_q:
                haystack = f"{tbl.get('source_table','')} {tbl.get('target_table','')}".lower()
                if search_q.strip().lower() not in haystack:
                    continue
            if schema_filter != "All schemas" and tbl.get("source_db") != schema_filter:
                continue
            if status_filter == "Active" and not tbl.get("active", True):
                continue
            if status_filter == "Inactive" and tbl.get("active", True):
                continue
            if status_filter == "Needs Review" and not tbl.get("_review"):
                continue
            filtered_indices.append(i)

        if not filtered_indices:
            st.info("No tables match the current filters.")
        else:
            # ── Pagination (avoid a 'wall of expanders' for large configs) ────
            PAGE = 25
            total_f = len(filtered_indices)
            npages = max(1, (total_f + PAGE - 1) // PAGE)
            if npages > 1:
                pp1, pp2 = st.columns([1, 4])
                cfg_pg = pp1.number_input("Page", 1, npages, 1, key="cfg_page")
                pp2.caption(f"Showing {((cfg_pg - 1) * PAGE) + 1}–"
                            f"{min(cfg_pg * PAGE, total_f)} of {total_f} tables")
                filtered_indices = filtered_indices[(cfg_pg - 1) * PAGE: cfg_pg * PAGE]
            else:
                st.caption(f"{total_f} table(s)")

            # ── Group by schema ───────────────────────────────────────────────
            from collections import OrderedDict
            schema_groups = OrderedDict()
            for i in filtered_indices:
                s = tables[i].get("source_db", "unknown")
                schema_groups.setdefault(s, []).append(i)

            changed = False
            for schema_name, indices in schema_groups.items():
                n_grp_active = sum(1 for i in indices if tables[i].get("active", True))
                n_grp_review = sum(1 for i in indices if tables[i].get("_review"))
                review_badge = (f' · <span style="color:{ST_SKIPPED}">⚠️ {n_grp_review} need review</span>'
                                if n_grp_review else "")
                st.markdown(f"""<div class="schema-group">
                    <div><span class="sg-title">📂 {schema_name}</span>
                    <span class="sg-count">{len(indices)} table(s)</span></div>
                    <div class="sg-meta">{n_grp_active} active{review_badge}</div>
                </div>""", unsafe_allow_html=True)

                # Bulk actions per schema group
                ba1, ba2, ba3 = st.columns([1, 1, 6])
                if ba1.button("✅ Activate All", key=f"ba_act_{schema_name}",
                              use_container_width=True):
                    for i in indices:
                        cfg["tables"][i]["active"] = True
                    changed = True
                if ba2.button("⚫ Deactivate All", key=f"ba_deact_{schema_name}",
                              use_container_width=True):
                    for i in indices:
                        cfg["tables"][i]["active"] = False
                    changed = True

                # ── Per-table expanders ───────────────────────────────────────
                for i in indices:
                    tbl = tables[i]
                    is_active = tbl.get("active", True)
                    status = tbl.get("last_run_status") or "pending"
                    has_review = bool(tbl.get("_review"))
                    # Enhanced label with status at a glance
                    status_badge = status_icon(status)
                    review_flag = " ⚠️" if has_review else ""
                    wm_short = tbl.get("last_loaded_at", "")
                    wm_hint = f" · 🕐 {str(wm_short)[:10]}" if wm_short else ""
                    label = (f"{'🟢' if is_active else '⚫'} "
                             f"{tbl['source_table']} → {tbl['target_table']} "
                             f"[{tbl.get('load_type', 'full')}] "
                             f"{status_badge} {status}{review_flag}{wm_hint}")
                    with st.expander(label, expanded=False):
                        # Review flag: a toggle to mark reviewed (or re-flag).
                        _note = tbl.get("_review") or tbl.get("_review_ack")
                        if _note:
                            reviewed_now = bool(tbl.get("_review_ack")) and not tbl.get("_review")
                            if reviewed_now:
                                st.caption(f"✅ Reviewed · was: {tbl.get('_review_ack')}")
                            else:
                                st.warning(f"⚠️ **Review Required:** {tbl.get('_review')}")
                            rv = st.toggle("Reviewed", value=reviewed_now, key=f"rev_{i}",
                                           help="On = reviewed (flag cleared). Toggle off to "
                                                "re-flag this table for review.")
                            if rv != reviewed_now:
                                live = load_config()
                                for t in live.get("tables", []):
                                    if (t.get("source_db") == tbl["source_db"]
                                            and t.get("source_table") == tbl["source_table"]):
                                        if rv:  # mark reviewed
                                            t["_review_ack"] = (t.pop("_review", None)
                                                                or t.get("_review_ack"))
                                        else:   # re-flag for review
                                            t["_review"] = (t.pop("_review_ack", None)
                                                            or t.get("_review"))
                                        break
                                save_config(live)
                                st.rerun()
                        col_a, col_b, col_c = st.columns(3)
                        new_load = col_a.selectbox(
                            "Load type", ["full", "incremental"],
                            index=["full", "incremental"].index(tbl.get("load_type", "full")),
                            key=f"lt_{i}")
                        new_wm = col_b.text_input("Watermark column",
                                                   value=tbl.get("watermark_col") or "", key=f"wm_{i}")
                        new_pk = col_c.text_input("Primary key",
                                                   value=tbl.get("primary_key") or "", key=f"pk_{i}")
                        new_mk_raw = st.text_input(
                            "Merge keys (composite, comma-separated — blank = primary key)",
                            value=", ".join(tbl.get("merge_keys", [])), key=f"mk_{i}",
                            help="Full uniqueness grain used for MERGE/dedupe, e.g. EMP_NO, FROM_DATE")
                        new_wt = st.selectbox(
                            "Watermark type", ["auto", "time", "id"],
                            index=["auto", "time", "id"].index(tbl.get("watermark_type", "auto")),
                            key=f"wt_{i}",
                            help="auto = detect from column type · time = timestamp "
                                 "(inserts+updates) · id = monotonic integer PK (inserts only)")
                        col_d, col_e, col_f, col_g = st.columns(4)
                        new_active = col_d.checkbox("Active", value=tbl.get("active", True),
                                                     key=f"act_{i}")
                        new_reconcile = col_e.checkbox("Reconcile deletes",
                                                        value=tbl.get("reconcile", False), key=f"rec_{i}")
                        new_parts = col_f.number_input("Partitions", min_value=1, max_value=32,
                                                        value=int(tbl.get("partition_num", 8)), key=f"pn_{i}")
                        new_rpf = col_g.number_input("Rows/file (0=off)", min_value=0, step=100000,
                                                      value=int(tbl.get("rows_per_file", 1000000) or 0),
                                                      key=f"rf_{i}")

                        wm_val = tbl.get("last_loaded_at")
                        st_status = tbl.get("last_run_status", "pending")
                        sc = (ST_SUCCESS if st_status == "success"
                              else ST_FAILED if st_status == "failed" else ST_SKIPPED)
                        st.markdown(f"""
                            <div class="ns-box">
                              <span class="ns-label">TARGET : </span>
                              <span class="ns-value">{raw_fqn(tbl)}</span><br>
                              <span class="ns-label">Watermark: </span>
                              <span class="ns-value">{wm_val or "Never run"}</span>
                              &nbsp;·&nbsp; <span style="color:{sc}">{status_icon(st_status)} {st_status}</span>
                            </div>
                        """, unsafe_allow_html=True)

                        # ── AI config recommender (Cortex) — only when AI is ON ──
                        if ai_enabled():
                            rkey = f"_ai_rec_{i}"
                            if st.button("🤖 Recommend settings", key=f"recbtn_{i}",
                                         help="Ask Cortex to suggest load_type / watermark / "
                                              "merge_keys from this table's MySQL metadata"):
                                with st.spinner("Analyzing table with Cortex…"):
                                    try:
                                        st.session_state[rkey] = ai_recommend_config(
                                            tbl["source_db"], tbl["source_table"])
                                    except Exception as ex:  # noqa: BLE001
                                        st.session_state[rkey] = {"json": None,
                                                                  "raw": f"AI error: {ex}"}
                            rec = st.session_state.get(rkey)
                            if rec:
                                j = rec.get("json")
                                if j:
                                    st.info(
                                        f"🤖 **Recommendation** — "
                                        f"load_type: `{j.get('load_type')}` · "
                                        f"watermark_col: `{j.get('watermark_col')}` · "
                                        f"watermark_type: `{j.get('watermark_type')}` · "
                                        f"merge_keys: `{j.get('merge_keys')}` · "
                                        f"partition_col: `{j.get('partition_col')}`\n\n"
                                        f"_{j.get('rationale', '')}_")
                                    if st.button("✅ Apply recommendation", key=f"recapply_{i}"):
                                        live = load_config()
                                        for t in live.get("tables", []):
                                            if (t.get("source_db") == tbl["source_db"]
                                                    and t.get("source_table") == tbl["source_table"]):
                                                t["load_type"] = j.get("load_type") or t.get("load_type")
                                                t["watermark_col"] = (
                                                    (j.get("watermark_col") or "").upper() or None)
                                                wt = j.get("watermark_type")
                                                if wt in ("time", "id"):
                                                    t["watermark_type"] = wt
                                                else:
                                                    t.pop("watermark_type", None)
                                                mk = [c.upper() for c in (j.get("merge_keys") or [])]
                                                if mk:
                                                    t["merge_keys"] = mk
                                                else:
                                                    t.pop("merge_keys", None)
                                                pc = j.get("partition_col")
                                                t["partition_col"] = pc.upper() if pc else None
                                                break
                                        save_config(live)
                                        st.session_state.pop(rkey, None)
                                        st.success("✅ Recommendation applied")
                                        time.sleep(0.4)
                                        st.rerun()
                                else:
                                    st.warning(f"🤖 Could not parse a recommendation:\n\n{rec.get('raw','')}")

                        updates = {
                            "load_type": new_load,
                            "watermark_col": (new_wm.strip().upper() or None),
                            "primary_key": (new_pk.strip().upper() or None),
                            "active": new_active,
                            "reconcile": new_reconcile,
                            "partition_num": int(new_parts),
                            "rows_per_file": int(new_rpf) or None,
                        }
                        for k, v in updates.items():
                            if v != tbl.get(k):
                                cfg["tables"][i][k] = v
                                changed = True
                        new_mk = [c.strip().upper() for c in new_mk_raw.split(",") if c.strip()]
                        if new_mk != tbl.get("merge_keys", []):
                            if new_mk:
                                cfg["tables"][i]["merge_keys"] = new_mk
                            else:
                                cfg["tables"][i].pop("merge_keys", None)
                            changed = True
                        # Watermark type: "auto" removes the key (let the engine detect).
                        if new_wt != tbl.get("watermark_type", "auto"):
                            if new_wt in ("time", "id"):
                                cfg["tables"][i]["watermark_type"] = new_wt
                            else:
                                cfg["tables"][i].pop("watermark_type", None)
                            changed = True

            if changed:
                if st.button("💾 Save Changes", type="primary"):
                    save_config(cfg)
                    st.success("✅ histload_config.json saved")
                    time.sleep(0.4)
                    st.rerun()
            else:
                st.caption("No unsaved changes.")
        with st.expander("📄 Raw JSON"):
            st.json(cfg)

# ── TAB 4 — HISTORY ───────────────────────────────────────────────────────────
with tab_hist:
    st.markdown('<div class="section-header">Run History — HISTLOAD_DB.META.RUN_LOG</div>',
                unsafe_allow_html=True)

    # ── Fetch data ────────────────────────────────────────────────────────────
    c1, c2 = st.columns([1, 4])
    if c1.button("🔄 Refresh", use_container_width=True, key="hist_refresh"):
        st.session_state.pop("_hist", None)
    limit = c2.slider("Max rows to fetch", 50, 2000, 500, key="hist_limit")
    if "_hist" not in st.session_state:
        try:
            con = get_sf(); cur = con.cursor()
            cur.execute(f"""
                SELECT INSERTED_AT, BATCH_ID, SOURCE_DB, SOURCE_TABLE, TARGET_DB,
                       LOAD_TYPE, ENGINE, STATUS, FAILED_STEP, DURATION_SEC,
                       ROW_DETAIL, WATERMARK_FROM, WATERMARK_TO, WATERMARK_TYPE,
                       ERROR_MESSAGE
                FROM HISTLOAD_DB.META.V_RUN_LOG
                LIMIT {int(limit)}""")
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            cur.close()  # pooled connection stays open
            import pandas as pd
            st.session_state["_hist"] = pd.DataFrame(rows, columns=cols)
        except Exception as e:  # noqa: BLE001
            st.error(f"Could not read RUN_LOG: {e}")
            st.session_state["_hist"] = None

    hist = st.session_state.get("_hist")
    if hist is not None and not hist.empty:
        import pandas as pd

        # ── Summary metrics ───────────────────────────────────────────────────
        total_runs = len(hist)
        n_success = len(hist[hist["STATUS"] == "success"])
        n_failed = len(hist[hist["STATUS"] == "failed"])
        success_pct = round(n_success / total_runs * 100, 1) if total_runs else 0
        avg_dur = round(hist["DURATION_SEC"].dropna().mean(), 1) if not hist["DURATION_SEC"].isna().all() else 0
        last_run = str(hist["INSERTED_AT"].iloc[0])[:19] if total_runs else "—"

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
        st.caption(f"Last run: {last_run}")

        # ── Filter bar ────────────────────────────────────────────────────────
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown('<div class="section-header">Filters</div>', unsafe_allow_html=True)
        hf1, hf2, hf3, hf4 = st.columns([2, 2, 2, 2])
        hf1.caption("Table")
        hf2.caption("Status")
        hf3.caption("Load Type")
        hf4.caption("Date Range")
        hist_tables = sorted(hist["SOURCE_TABLE"].dropna().unique().tolist())
        hist_table_filter = hf1.selectbox("Table", ["All"] + hist_tables,
                                           label_visibility="collapsed", key="hist_tbl_f")
        hist_status_opts = ["All"] + sorted(hist["STATUS"].dropna().unique().tolist())
        hist_status_filter = hf2.selectbox("Status", hist_status_opts,
                                            label_visibility="collapsed", key="hist_stat_f")
        hist_load_types = ["All"] + sorted(hist["LOAD_TYPE"].dropna().unique().tolist())
        hist_load_filter = hf3.selectbox("Load Type", hist_load_types,
                                          label_visibility="collapsed", key="hist_load_f")
        # Date range
        hist["_dt"] = pd.to_datetime(hist["INSERTED_AT"], errors="coerce")
        min_dt = hist["_dt"].min()
        max_dt = hist["_dt"].max()
        if pd.notna(min_dt) and pd.notna(max_dt):
            date_range = hf4.date_input("Date range", value=(min_dt.date(), max_dt.date()),
                                         label_visibility="collapsed", key="hist_date_f")
        else:
            date_range = None

        # Apply filters
        filtered = hist.copy()
        if hist_table_filter != "All":
            filtered = filtered[filtered["SOURCE_TABLE"] == hist_table_filter]
        if hist_status_filter != "All":
            filtered = filtered[filtered["STATUS"] == hist_status_filter]
        if hist_load_filter != "All":
            filtered = filtered[filtered["LOAD_TYPE"] == hist_load_filter]
        if date_range and len(date_range) == 2:
            d_start, d_end = date_range
            filtered = filtered[
                (filtered["_dt"].dt.date >= d_start) & (filtered["_dt"].dt.date <= d_end)]

        st.caption(f"Showing {len(filtered)} of {total_runs} rows")

        # ── Trend chart (daily runs by status) ────────────────────────────────
        if not filtered.empty and filtered["_dt"].notna().any():
            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown('<div class="section-header">Daily Run Trend</div>',
                        unsafe_allow_html=True)
            chart_df = filtered[["_dt", "STATUS"]].copy()
            chart_df["date"] = chart_df["_dt"].dt.date
            pivot = chart_df.groupby(["date", "STATUS"]).size().reset_index(name="count")
            pivot_wide = pivot.pivot(index="date", columns="STATUS", values="count").fillna(0)
            # Reorder columns for consistent color assignment
            col_order = [c for c in ["success", "failed", "skipped", "mismatch"]
                         if c in pivot_wide.columns]
            col_order += [c for c in pivot_wide.columns if c not in col_order]
            pivot_wide = pivot_wide[col_order]
            st.bar_chart(pivot_wide, color=[ST_SUCCESS, ST_FAILED, ST_SKIPPED, ST_PENDING][:len(col_order)])

        # ── Batch grouping ────────────────────────────────────────────────────
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown('<div class="section-header">Run Batches</div>',
                    unsafe_allow_html=True)
        display_cols = [c for c in filtered.columns if c != "_dt"]
        if not filtered.empty:
            batches = filtered.groupby("BATCH_ID", sort=False)
            for batch_id, batch_df in batches:
                batch_time = str(batch_df["INSERTED_AT"].iloc[0])[:19]
                n_batch = len(batch_df)
                n_ok = len(batch_df[batch_df["STATUS"] == "success"])
                n_err = len(batch_df[batch_df["STATUS"] == "failed"])
                batch_status_icon = "✅" if n_err == 0 else "❌"
                batch_dur = batch_df["DURATION_SEC"].dropna().sum()
                batch_label = (f"{batch_status_icon} Batch {batch_id} — "
                               f"{batch_time} — {n_batch} table(s) "
                               f"({n_ok} ok, {n_err} failed) — {batch_dur:.1f}s total")
                with st.expander(batch_label, expanded=(n_err > 0)):
                    def _style_status(val):
                        return {"success": f"color:{ST_SUCCESS};font-weight:700",
                                "failed": f"color:{ST_FAILED};font-weight:700",
                                "mismatch": f"color:{ST_SKIPPED};font-weight:700",
                                "skipped": f"color:{ST_SKIPPED};font-weight:700"}.get(
                            str(val).lower(), "")
                    show_cols = ["SOURCE_TABLE", "LOAD_TYPE", "STATUS", "DURATION_SEC",
                                 "ROW_DETAIL", "FAILED_STEP", "ERROR_MESSAGE"]
                    show_cols = [c for c in show_cols if c in batch_df.columns]
                    st.dataframe(
                        batch_df[show_cols].style.map(_style_status, subset=["STATUS"]),
                        use_container_width=True, hide_index=True)
                    # Show errors inline
                    errs = batch_df[batch_df["STATUS"] == "failed"]
                    if not errs.empty:
                        for ei, (_, row) in enumerate(errs.iterrows()):
                            st.error(f"**{row.get('SOURCE_TABLE', '?')}** — "
                                     f"step: {row.get('FAILED_STEP', '?')}")
                            st.code(row.get("ERROR_MESSAGE") or "No error message",
                                    language="text")
                            # AI failure explainer (Cortex) — only when AI Assist is ON.
                            if ai_enabled() and row.get("ERROR_MESSAGE"):
                                ekey = (f"_ai_err_{row.get('SOURCE_DB','')}."
                                        f"{row.get('SOURCE_TABLE','')}.{ei}")
                                if st.button("🤖 Explain this failure",
                                             key=f"btn{ekey}"):
                                    with st.spinner("Asking Cortex…"):
                                        try:
                                            prompt = (
                                                "You are a Snowflake/MySQL data-migration expert. "
                                                "Explain this load failure in 2-3 sentences and give a "
                                                "concrete fix.\n"
                                                f"Table: {row.get('SOURCE_DB','?')}."
                                                f"{row.get('SOURCE_TABLE','?')}\n"
                                                f"Load type: {row.get('LOAD_TYPE','?')}\n"
                                                f"Failed step: {row.get('FAILED_STEP','?')}\n"
                                                f"Error: {row.get('ERROR_MESSAGE','')}")
                                            st.session_state[ekey] = cortex_complete(prompt)
                                        except Exception as ex:  # noqa: BLE001
                                            st.session_state[ekey] = f"AI error: {ex}"
                                if st.session_state.get(ekey):
                                    st.info(f"🤖 **Cortex:** {st.session_state[ekey]}")
        else:
            st.info("No runs match the current filters.")

    elif hist is not None:
        st.info("No run history yet. Run a load first.")
    else:
        st.warning("Could not load history — check Snowflake connection.")

# ── TAB 5 — COUNTS & VALIDATION (UNIFIED) ────────────────────────────────────
with tab_counts:
    st.markdown('<div class="section-header">Data Parity — Source ↔ Snowflake RAW</div>',
                unsafe_allow_html=True)
    st.caption("Compares MySQL source rows against Snowflake RAW (live = excluding soft-deleted). "
               "Deep mode adds a row-hash fingerprint check.")

    # ── Controls ──────────────────────────────────────────────────────────────
    ctl1, ctl2, ctl3 = st.columns([1.5, 1, 1.5])
    run_parity = ctl1.button("🔍 Run Parity Check", type="primary", use_container_width=True)
    deep_validate = ctl2.toggle(
        "Deep (row hash)", value=False,
        help="Also compare an order-independent row-hash fingerprint. Slower; "
             "can false-alarm on float/exotic column types.")
    auto_refresh = ctl3.toggle("Auto-refresh every 60s", value=False)

    if run_parity or auto_refresh:
        import pandas as pd
        from datetime import datetime as _dt

        active = [t for t in cfg.get("tables", []) if t.get("active", True)]
        if not active:
            st.info("No active tables to validate.")
        else:
            # ── Progress feedback ─────────────────────────────────────────────
            progress_bar = st.progress(0, text="Connecting…")
            status_text = st.empty()
            vrows = []
            errors = []

            try:
                scon = get_sf(); scur = scon.cursor()
                mcon = get_mysql()

                # Get Snowflake row counts from INFORMATION_SCHEMA (instant)
                dbs = sorted({loader.target_db(t["source_db"]) for t in active})
                row_counts = {}
                for db in dbs:
                    try:
                        scur.execute(
                            f"SELECT TABLE_NAME, ROW_COUNT FROM {db}.INFORMATION_SCHEMA.TABLES "
                            "WHERE TABLE_SCHEMA = 'RAW' AND TABLE_TYPE = 'BASE TABLE'")
                        for tname, rc in scur.fetchall():
                            row_counts[(db, tname)] = rc
                    except Exception:  # noqa: BLE001
                        pass

                progress_bar.progress(10, text="Validating tables…")

                for idx, tbl in enumerate(active):
                    pct = 10 + int((idx + 1) / len(active) * 85)
                    tbl_name = f"{tbl['source_db']}.{tbl['source_table']}"
                    status_text.caption(f"Checking {tbl_name}… ({idx + 1}/{len(active)})")
                    progress_bar.progress(pct, text=f"Validating {tbl_name}")

                    db = loader.target_db(tbl["source_db"])
                    sf_total = row_counts.get((db, tbl["target_table"]))

                    # Live count (excludes soft-deleted)
                    if tbl.get("reconcile") and sf_total is not None:
                        try:
                            scur.execute(f'SELECT COUNT(*) FROM {raw_fqn(tbl)} '
                                         f'WHERE COALESCE("_IS_DELETED",FALSE)=FALSE')
                            sf_live = scur.fetchone()[0]
                        except Exception:  # noqa: BLE001
                            sf_live = sf_total
                    else:
                        sf_live = sf_total

                    # Full validation (MySQL count + watermark + optional hash)
                    try:
                        r = validator.validate_table(scur, mcon, tbl, deep=deep_validate)
                        wm_txt = ("—" if not r["has_wm"]
                                  else ("✅ " if r["wm_ok"] else "⚠️ ") + str(r["raw_wm"]))
                        hash_txt = ("—" if not r["deep"] else "✅" if r["hash_ok"] else "⚠️")
                        delta = r["delta"]
                        parity_ok = r["count_ok"]
                        sync_status = "in_sync" if r["ok"] else "out_of_sync"
                    except Exception as te:  # noqa: BLE001
                        wm_txt = f"ERR: {te}"
                        hash_txt = "—"
                        delta = None
                        parity_ok = None
                        sync_status = "error"
                        r = {"source": None, "raw_live": sf_live, "ok": False}
                        errors.append((tbl_name, str(te)))

                    # Freshness: time since last sync
                    last_sync = tbl.get("last_loaded_at")
                    if last_sync:
                        try:
                            since = _dt.now() - _dt.strptime(str(last_sync)[:19], "%Y-%m-%d %H:%M:%S")
                            hours = since.total_seconds() / 3600
                            if hours < 1:
                                freshness = f"{int(since.total_seconds() / 60)}m ago"
                            elif hours < 24:
                                freshness = f"{hours:.1f}h ago"
                            else:
                                freshness = f"{hours / 24:.1f}d ago"
                        except Exception:  # noqa: BLE001
                            freshness = str(last_sync)[:10]
                    else:
                        freshness = "Never"

                    vrows.append({
                        "_sync": sync_status,
                        "Source Table": tbl_name,
                        "Source (MySQL)": r.get("source"),
                        "RAW (total)": sf_total,
                        "RAW (live)": r.get("raw_live", sf_live),
                        "Δ": delta,
                        "Parity": "✅" if parity_ok else ("⚠️" if parity_ok is not None else "❌"),
                        "Watermark": wm_txt,
                        "Row Hash": hash_txt,
                        "Last Sync": freshness,
                    })

                scur.close()  # pooled connections stay open
                progress_bar.progress(100, text="Done")
                status_text.empty()

            except Exception as e:  # noqa: BLE001
                st.error(f"Connection error: {e}")
                progress_bar.empty()
                status_text.empty()

            if vrows:
                df = pd.DataFrame(vrows)

                # ── Summary metrics ───────────────────────────────────────────
                n_total = len(df)
                n_in_sync = len(df[df["_sync"] == "in_sync"])
                n_out_sync = len(df[df["_sync"] == "out_of_sync"])
                n_error = len(df[df["_sync"] == "error"])
                sync_pct = round(n_in_sync / n_total * 100, 1) if n_total else 0
                total_raw = df["RAW (live)"].dropna().sum()
                avg_delta = df["Δ"].dropna().abs().mean()
                avg_delta_str = f"{avg_delta:.0f}" if pd.notna(avg_delta) else "—"

                st.markdown(f"""<div class="cfg-summary">
                    <div class="cfg-mini-card" style="border-left:3px solid {ST_SUCCESS}">
                        <div class="mc-val" style="color:{ST_SUCCESS}">{sync_pct}%</div>
                        <div class="mc-lbl">In Sync</div></div>
                    <div class="cfg-mini-card" style="border-left:3px solid {ST_FAILED}">
                        <div class="mc-val" style="color:{ST_FAILED}">{n_out_sync}</div>
                        <div class="mc-lbl">Out of Sync</div></div>
                    <div class="cfg-mini-card" style="border-left:3px solid {ST_PENDING}">
                        <div class="mc-val" style="color:{ST_PENDING}">{total_raw:,.0f}</div>
                        <div class="mc-lbl">Total RAW Rows</div></div>
                    <div class="cfg-mini-card" style="border-left:3px solid {TXT_LABEL}">
                        <div class="mc-val" style="color:{TXT_PRIMARY}">{avg_delta_str}</div>
                        <div class="mc-lbl">Avg |Δ|</div></div>
                </div>""", unsafe_allow_html=True)

                # ── Filter bar ────────────────────────────────────────────────
                st.markdown("<br>", unsafe_allow_html=True)
                st.markdown('<div class="section-header">Filters</div>', unsafe_allow_html=True)
                pf1, pf2, pf3 = st.columns([2, 2, 3])
                pf1.caption("Sync Status")
                pf2.caption("Schema")
                pf3.caption("Search")
                parity_status_f = pf1.selectbox(
                    "Sync", ["All", "In Sync", "Out of Sync", "Error"],
                    label_visibility="collapsed", key="par_sync_f")
                parity_schemas = sorted(set(r.split(".")[0] for r in df["Source Table"]))
                parity_schema_f = pf2.selectbox(
                    "Schema", ["All"] + parity_schemas,
                    label_visibility="collapsed", key="par_schema_f")
                parity_search = pf3.text_input(
                    "Search", placeholder="Filter by table name…",
                    label_visibility="collapsed", key="par_search")

                # Apply filters
                show_df = df.copy()
                if parity_status_f == "In Sync":
                    show_df = show_df[show_df["_sync"] == "in_sync"]
                elif parity_status_f == "Out of Sync":
                    show_df = show_df[show_df["_sync"] == "out_of_sync"]
                elif parity_status_f == "Error":
                    show_df = show_df[show_df["_sync"] == "error"]
                if parity_schema_f != "All":
                    show_df = show_df[show_df["Source Table"].str.startswith(parity_schema_f + ".")]
                if parity_search.strip():
                    show_df = show_df[show_df["Source Table"].str.contains(
                        parity_search.strip(), case=False, na=False)]

                st.caption(f"Showing {len(show_df)} of {n_total} tables")

                # ── Results table with visual row indicators ──────────────────
                st.markdown("<br>", unsafe_allow_html=True)
                display_df = show_df.drop(columns=["_sync"])

                def _row_color(row):
                    if row["Parity"] == "✅":
                        return [f"background-color: #0d2e1544" for _ in row]
                    elif row["Parity"] == "⚠️":
                        return [f"background-color: #2e0d0d44" for _ in row]
                    else:
                        return [f"background-color: #1a1a1a44" for _ in row]

                styled = display_df.style.apply(_row_color, axis=1)
                st.dataframe(styled, use_container_width=True, hide_index=True)

                # ── Error details ─────────────────────────────────────────────
                if errors:
                    st.markdown("<br>", unsafe_allow_html=True)
                    st.markdown('<div class="section-header">❌ Validation Errors</div>',
                                unsafe_allow_html=True)
                    for tbl_name, err_msg in errors:
                        with st.expander(f"❌ {tbl_name}"):
                            st.code(err_msg, language="text")

        if auto_refresh:
            # Non-blocking: a fragment ticker triggers a full rerun ~every 60s.
            st.session_state["_parity_rendered_at"] = time.time()
            if autorefresh_ticker is not None:
                autorefresh_ticker()
            else:
                time.sleep(60)  # fallback for Streamlit without st.fragment
                st.rerun()

render_footer()
