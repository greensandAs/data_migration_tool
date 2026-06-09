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
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] span,
section[data-testid="stSidebar"] div,
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] .stMarkdown {{ color: {TXT_PRIMARY} !important; }}
section[data-testid="stSidebar"] .stCaption {{ color: {TXT_SECONDARY} !important; }}

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

/* Primary button */
[data-testid="baseButton-primary"] > button,
.stButton > button[kind="primary"] {{ background: {TA_ORANGE} !important; color: #fff !important; border: none !important; font-weight: 700 !important; letter-spacing: .3px; }}
[data-testid="baseButton-primary"] > button:hover {{ background: {TA_ORANGE_DARK} !important; }}

/* Namespace info box */
.ns-box {{ background: {TA_NAVY}; border: 1px solid {BORDER}; border-radius: 6px; padding: 10px 14px; font-family: 'JetBrains Mono', monospace; font-size: .73rem; line-height: 1.8; margin-top: 8px; }}
.ns-box .ns-label {{ color: {TXT_LABEL}; }}
.ns-box .ns-value {{ color: {TXT_PRIMARY}; }}
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


# ── Connections ───────────────────────────────────────────────────────────────
def get_sf():
    return loader.get_sf_conn(sf_conf())


def get_mysql():
    import mysql.connector
    c = my_conf()
    return mysql.connector.connect(
        host=c["host"], port=int(c["port"]), user=c["user"], password=c["password"])


def check_connections() -> dict:
    out = {}
    try:
        con = get_mysql(); cur = con.cursor()
        cur.execute("SELECT VERSION()")
        ver = cur.fetchone()[0]; cur.close(); con.close()
        out["MySQL"] = (True, str(ver))
    except Exception as e:  # noqa: BLE001
        out["MySQL"] = (False, str(e)[:80])
    try:
        con = get_sf(); cur = con.cursor()
        cur.execute("SELECT CURRENT_ACCOUNT(), CURRENT_WAREHOUSE()")
        acct, wh = cur.fetchone(); cur.close(); con.close()
        out["Snowflake"] = (True, f"{acct} / {wh}")
    except Exception as e:  # noqa: BLE001
        out["Snowflake"] = (False, str(e)[:80])
    return out


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
    lines = []
    # -u + PYTHONUNBUFFERED flush each line immediately so the log streams live.
    proc = subprocess.Popen(
        [sys.executable, "-u", *args], cwd=str(HERE),
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1,
        env={**os.environ, "PYTHONUNBUFFERED": "1"})
    for raw in proc.stdout:
        lines.append(colorize_log(raw.rstrip()))
        log_area.markdown(
            f'<div class="log-box">{"<br>".join(lines[-60:])}</div>',
            unsafe_allow_html=True)
    proc.wait()
    return proc.returncode, "\n".join(lines)


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
        f' &middot; Built on Snowflake</p>', unsafe_allow_html=True)


# ── Load config ───────────────────────────────────────────────────────────────
cfg = load_config()


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
    watermark_col = c4.text_input("Watermark column (optional)", key="add_wm")
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

    if "_conn" not in st.session_state:
        with st.spinner("Checking connections…"):
            st.session_state["_conn"] = check_connections()
    if st.button("🔌 Re-check Connections", use_container_width=True):
        st.session_state["_conn"] = check_connections()
    for name, (ok, detail) in st.session_state["_conn"].items():
        color = ST_SUCCESS if ok else ST_FAILED
        st.markdown(
            f'<div style="margin:5px 0;font-size:.82rem;color:{TXT_PRIMARY}">'
            f'<span class="dot" style="background:{color}"></span>'
            f'<b>{name}</b>: <span style="color:{TXT_SECONDARY}">{detail}</span></div>',
            unsafe_allow_html=True)

    st.markdown(f"<hr style='border-color:{BORDER}'>", unsafe_allow_html=True)
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

    st.markdown(f"<hr style='border-color:{BORDER}'>", unsafe_allow_html=True)
    st.markdown(
        f'<div style="font-size:.7rem;color:{TXT_LABEL};line-height:1.8">'
        f'Account: <code style="color:{TXT_SECONDARY}">{sf_conf().get("account","—")}</code><br>'
        f'Control: <code style="color:{TXT_SECONDARY};font-size:.65rem">HISTLOAD_DB.META</code>'
        f'</div>', unsafe_allow_html=True)

    st.markdown(f"<hr style='border-color:{BORDER}'>", unsafe_allow_html=True)
    if st.button("📖 User Guide", use_container_width=True):
        st.session_state["_view"] = "guide"
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
        cols = st.columns(2)
        for i, tbl in enumerate(tables):
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

# ── TAB 2 — RUN ───────────────────────────────────────────────────────────────
with tab_run:
    st.markdown('<div class="section-header">Pipeline Controls</div>',
                unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    run_clicked = c1.button("▶️ Run Load", type="primary", use_container_width=True)
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
                               disabled=(selected == "— all tables —"))

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="section-header">Live Log</div>', unsafe_allow_html=True)
    if "last_log" not in st.session_state:
        st.session_state["last_log"] = "No runs yet. Click ▶️ Run Load to start."

    if run_clicked:
        st.info("⚡ Load starting…")
        rc, log = run_subprocess_stream(["orchestrator.py"])
        st.session_state["last_log"] = log
        (st.success if rc == 0 else st.error)(
            f"{'✅ Completed' if rc == 0 else '❌ Finished with failures'} (exit {rc})")
        st.rerun()
    elif single_clicked and selected != "— all tables —":
        _, tbl_name = selected.split(".", 1)
        st.info(f"▶️ Running {selected}…")
        rc, log = run_subprocess_stream(["orchestrator.py", "--table", tbl_name])
        st.session_state["last_log"] = log
        st.rerun()
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
        changed = False
        for i, tbl in enumerate(tables):
            label = (f"{'🟢' if tbl.get('active', True) else '⚫'} "
                     f"{tbl['source_db']}.{tbl['source_table']} → {tbl['target_table']} "
                     f"[{tbl.get('load_type', 'full')}]")
            with st.expander(label, expanded=False):
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
                new_atomic = st.checkbox(
                    "Atomic full reload (load aside + SWAP — no empty window)",
                    value=tbl.get("atomic_full", False), key=f"at_{i}")

                wm_val = tbl.get("last_loaded_at")
                status = tbl.get("last_run_status", "pending")
                sc = (ST_SUCCESS if status == "success"
                      else ST_FAILED if status == "failed" else ST_SKIPPED)
                st.markdown(f"""
                    <div class="ns-box">
                      <span class="ns-label">TARGET : </span>
                      <span class="ns-value">{raw_fqn(tbl)}</span><br>
                      <span class="ns-label">Watermark: </span>
                      <span class="ns-value">{wm_val or "Never run"}</span>
                      &nbsp;·&nbsp; <span style="color:{sc}">{status_icon(status)} {status}</span>
                    </div>
                """, unsafe_allow_html=True)

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
                if bool(new_atomic) != bool(tbl.get("atomic_full", False)):
                    if new_atomic:
                        cfg["tables"][i]["atomic_full"] = True
                    else:
                        cfg["tables"][i].pop("atomic_full", None)
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
    c1, c2 = st.columns([1, 4])
    if c1.button("🔄 Refresh", use_container_width=True):
        st.session_state.pop("_hist", None)
    limit = c2.slider("Rows to show", 20, 500, 100)
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
            cur.close(); con.close()
            import pandas as pd
            st.session_state["_hist"] = pd.DataFrame(rows, columns=cols)
        except Exception as e:  # noqa: BLE001
            st.error(f"Could not read RUN_LOG: {e}")
            st.session_state["_hist"] = None
    hist = st.session_state.get("_hist")
    if hist is not None and not hist.empty:
        def _style(val):
            return {"success": f"color:{ST_SUCCESS};font-weight:700",
                    "failed": f"color:{ST_FAILED};font-weight:700",
                    "mismatch": f"color:{ST_SKIPPED};font-weight:700",
                    "skipped": f"color:{ST_SKIPPED};font-weight:700"}.get(str(val).lower(), "")
        st.dataframe(hist.style.map(_style, subset=["STATUS"]),
                     use_container_width=True, hide_index=True)
        failed = hist[hist["STATUS"] == "failed"]
        if not failed.empty:
            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown('<div class="section-header">❌ Error Detail</div>',
                        unsafe_allow_html=True)
            for _, row in failed.iterrows():
                step = row.get("FAILED_STEP") or "?"
                with st.expander(f"❌ {row.get('SOURCE_DB', '?')}.{row.get('SOURCE_TABLE', '?')} "
                                 f"— step: {step} — {str(row.get('INSERTED_AT', ''))[:19]}"):
                    st.code(row.get("ERROR_MESSAGE") or "No error message", language="text")
    elif hist is not None:
        st.info("No run history yet. Run a load first.")
    else:
        st.warning("Could not load history — check Snowflake connection.")

# ── TAB 5 — COUNTS ────────────────────────────────────────────────────────────
with tab_counts:
    st.markdown('<div class="section-header">Source ↔ Snowflake RAW</div>',
                unsafe_allow_html=True)
    st.caption("RAW = <MYSQL_SCHEMA>.RAW.<table> · live = excluding soft-deleted rows.")
    c1, c2, c3, c4 = st.columns([1.2, 1.2, 1, 1.2])
    compute = c1.button("🔢 Compute Counts", type="primary", use_container_width=True)
    validate = c2.button("🔍 Validate vs MySQL", use_container_width=True,
                         help="Compare MySQL row counts against Snowflake RAW (live)")
    deep_validate = c3.toggle(
        "Deep (row hash)", value=False,
        help="Also compare an order-independent row-hash fingerprint. Slower; "
             "can false-alarm on float/exotic column types.")
    auto_refresh = c4.toggle("Auto-refresh every 30s", value=False)

    if compute or auto_refresh:
        import pandas as pd
        rows_data = []
        try:
            con = get_sf(); cur = con.cursor()
            for tbl in cfg.get("tables", []):
                if not tbl.get("active", True):
                    continue

                def _count(where=""):
                    try:
                        q = f"SELECT COUNT(*) FROM {raw_fqn(tbl)}"
                        if where:
                            q += f" WHERE {where}"
                        cur.execute(q)
                        return cur.fetchone()[0]
                    except Exception:  # noqa: BLE001
                        return None
                total = _count()
                live = _count('COALESCE("_IS_DELETED",FALSE)=FALSE')
                wm = tbl.get("last_loaded_at")
                rows_data.append({
                    "Source Table": f"{tbl['source_db']}.{tbl['source_table']}",
                    "Target": raw_fqn(tbl),
                    "RAW (total)": total, "RAW (live)": live,
                    "Last Sync": str(wm)[:19] if wm else "Never",
                    "Status": tbl.get("last_run_status", "—"),
                })
            cur.close(); con.close()
        except Exception as e:  # noqa: BLE001
            st.error(f"Snowflake error: {e}")
        if rows_data:
            total_raw = sum(r["RAW (total)"] or 0 for r in rows_data)
            m1, m2 = st.columns(2)
            m1.markdown(f"""<div class="metric-card"><div class="label">Total RAW Rows</div>
                <div class="value">{total_raw:,}</div></div>""", unsafe_allow_html=True)
            m2.markdown(f"""<div class="metric-card"><div class="label">Active Tables</div>
                <div class="value">{len(rows_data)}</div></div>""", unsafe_allow_html=True)
            st.markdown("<br>", unsafe_allow_html=True)
            st.dataframe(pd.DataFrame(rows_data), use_container_width=True, hide_index=True)
        if auto_refresh:
            time.sleep(30)
            st.rerun()

    if validate:
        import pandas as pd
        st.markdown('<div class="section-header">Source ↔ RAW Parity</div>',
                    unsafe_allow_html=True)
        st.caption("MySQL COUNT(*) vs RAW live rows. Watermark parity always checked; "
                   "row-hash compared when Deep is on.")
        vrows = []
        try:
            scon = get_sf(); scur = scon.cursor()
            mcon = get_mysql()
            for tbl in cfg.get("tables", []):
                if not tbl.get("active", True):
                    continue
                try:
                    r = validator.validate_table(scur, mcon, tbl, deep=deep_validate)
                except Exception as te:  # noqa: BLE001
                    vrows.append({"Source Table": f"{tbl['source_db']}.{tbl['source_table']}",
                                  "Source (MySQL)": None, "RAW (live)": None, "Δ": None,
                                  "Parity": "—", "Watermark": f"ERR: {te}", "Row Hash": "—"})
                    continue
                wm_txt = ("—" if not r["has_wm"]
                          else ("✅ " if r["wm_ok"] else "⚠️ ") + str(r["raw_wm"]))
                hash_txt = ("—" if not r["deep"] else "✅" if r["hash_ok"] else "⚠️")
                vrows.append({
                    "Source Table": f"{tbl['source_db']}.{tbl['source_table']}",
                    "Source (MySQL)": r["source"], "RAW (live)": r["raw_live"],
                    "Δ": r["delta"], "Parity": "✅" if r["count_ok"] else "⚠️",
                    "Watermark": wm_txt, "Row Hash": hash_txt,
                })
            scur.close(); scon.close(); mcon.close()
        except Exception as e:  # noqa: BLE001
            st.error(f"Validation error: {e}")
        if vrows:
            in_sync = sum(1 for r in vrows if r["Parity"] == "✅")
            out_sync = sum(1 for r in vrows if r["Parity"] == "⚠️")
            v1, v2 = st.columns(2)
            v1.markdown(f"""<div class="metric-card" style="border-left:4px solid {ST_SUCCESS}">
                <div class="label">In Sync</div>
                <div class="value" style="color:{ST_SUCCESS}">{in_sync}</div>
                <div class="sub">source = RAW live</div></div>""", unsafe_allow_html=True)
            v2.markdown(f"""<div class="metric-card" style="border-left:4px solid {ST_FAILED}">
                <div class="label">Out of Sync</div>
                <div class="value" style="color:{ST_FAILED}">{out_sync}</div>
                <div class="sub">needs re-run / reconcile</div></div>""", unsafe_allow_html=True)
            st.markdown("<br>", unsafe_allow_html=True)
            st.dataframe(pd.DataFrame(vrows), use_container_width=True, hide_index=True)

render_footer()
