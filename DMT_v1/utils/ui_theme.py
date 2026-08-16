# Shared UI theme — Tiger Analytics branding, CSS, and reusable HTML helpers.
# Co-authored with CoCo
"""ui_theme.py — Centralized brand tokens, CSS injection, and HTML helpers.

All pages import from here to get consistent styling without duplicating
150+ lines of CSS in each module.
"""
from __future__ import annotations

import streamlit as st

# ── Brand & semantic color tokens (FIXED — not theme-adaptive) ────────────────
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


def inject_css():
    """Inject the full global CSS into the Streamlit page (call once in app.py)."""
    st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Source+Sans+Pro:wght@400;600;700&family=JetBrains+Mono:wght@400;600&display=swap');
html, body, [class*="css"] {{ font-family: 'Source Sans Pro', 'Segoe UI', Arial, sans-serif; }}
#MainMenu, footer, header {{ visibility: hidden; }}
.block-container {{ padding-top: 1rem; padding-bottom: 2rem; }}

/* Sidebar */
section[data-testid="stSidebar"] {{ background: {TA_NAVY} !important; border-right: 3px solid {TA_ORANGE}; }}
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
.pill-storage {{ background: #1a1a2e; color: #a78bfa; border: 1px solid #a78bfa55; }}
.pill-source {{ background: #2e1e0d; color: {ST_SKIPPED}; border: 1px solid {ST_SKIPPED}55; }}

/* Connection dot */
.dot {{ display: inline-block; width: 9px; height: 9px; border-radius: 50%; margin-right: 6px; vertical-align: middle; }}

/* Primary button */
.stButton > button[kind="primary"] {{ background: {TA_ORANGE} !important; color: #fff !important; border: none !important; font-weight: 700 !important; letter-spacing: .3px; }}
.stButton > button[kind="primary"]:hover {{ background: {TA_ORANGE_DARK} !important; }}

/* Namespace info box */
.ns-box {{ background: {TA_NAVY}; border: 1px solid {BORDER}; border-radius: 6px; padding: 10px 14px; font-family: 'JetBrains Mono', monospace; font-size: .73rem; line-height: 1.8; margin-top: 8px; }}
.ns-box .ns-label {{ color: {TXT_LABEL}; }}
.ns-box .ns-value {{ color: {TXT_PRIMARY}; }}

/* Config — schema group header */
.schema-group {{ background: {TA_NAVY}; border: 1px solid {BORDER}; border-radius: 8px; padding: 12px 18px; margin: 16px 0 8px 0; display: flex; align-items: center; justify-content: space-between; }}
.schema-group .sg-title {{ font-size: .82rem; font-weight: 700; color: {TXT_PRIMARY}; }}
.schema-group .sg-count {{ font-size: .68rem; color: {TXT_SECONDARY}; background: {TA_NAVY_LIGHT}; border: 1px solid {BORDER}; border-radius: 12px; padding: 2px 10px; margin-left: 10px; }}
.schema-group .sg-meta {{ font-size: .68rem; color: {TXT_LABEL}; }}

/* Config — mini summary cards */
.cfg-summary {{ display: flex; gap: 12px; margin-bottom: 16px; }}
.cfg-mini-card {{ flex: 1; background: {TA_NAVY}; border: 1px solid {BORDER}; border-radius: 8px; padding: 12px 16px; text-align: center; }}
.cfg-mini-card .mc-val {{ font-family: 'JetBrains Mono', monospace; font-size: 1.4rem; font-weight: 700; line-height: 1.2; }}
.cfg-mini-card .mc-lbl {{ font-size: .65rem; letter-spacing: 1.5px; text-transform: uppercase; color: {TXT_LABEL}; margin-top: 4px; font-weight: 600; }}
</style>
""", unsafe_allow_html=True)


# ── Reusable HTML helpers ─────────────────────────────────────────────────────

def section_header(text: str):
    st.markdown(f'<div class="section-header">{text}</div>', unsafe_allow_html=True)


def metric_card(label: str, value, sub: str = "", color: str = TXT_PRIMARY,
                border_color: str = ""):
    border = f"border-left:4px solid {border_color}" if border_color else ""
    return (
        f'<div class="metric-card" style="{border}">'
        f'<div class="label">{label}</div>'
        f'<div class="value" style="color:{color}">{value}</div>'
        f'<div class="sub">{sub}</div></div>'
    )


def mini_card(label: str, value, color: str = TXT_PRIMARY,
              border_color: str = BORDER) -> str:
    return (
        f'<div class="cfg-mini-card" style="border-left:3px solid {border_color}">'
        f'<div class="mc-val" style="color:{color}">{value}</div>'
        f'<div class="mc-lbl">{label}</div></div>'
    )


def mini_cards_row(cards: list[str]):
    st.markdown(
        '<div class="cfg-summary">' + "".join(cards) + '</div>',
        unsafe_allow_html=True,
    )


def status_icon(s: str | None) -> str:
    return {"success": "✅", "failed": "❌", "skipped": "⏭️", None: "⏳"}.get(s, "⏳")


def load_type_pill(load_type: str) -> str:
    cls = "pill-incr" if load_type == "incremental" else "pill-full"
    return f'<span class="pill {cls}">{load_type}</span>'


def source_type_pill(source_type: str) -> str:
    return f'<span class="pill pill-source">{source_type}</span>'


def storage_pill(storage_type: str) -> str:
    return f'<span class="pill pill-storage">{storage_type}</span>'


def table_card(name: str, target: str, status: str, load_type: str,
               storage_type: str = "", source_type: str = "",
               wm_info: str = "", extra: str = "") -> str:
    s = (status or "pending").lower()
    icon = status_icon(status)
    pills = load_type_pill(load_type)
    if source_type:
        pills += " " + source_type_pill(source_type)
    if storage_type:
        pills += " " + storage_pill(storage_type)
    return (
        f'<div class="table-card {s}">'
        f'<span class="tstatus {s}">{icon} {s.upper()}</span>'
        f'<div class="tname">{name}</div>'
        f'<div class="tmeta">→ {target}</div>'
        f'<div class="tmeta" style="margin-top:6px">{pills}</div>'
        f'<div class="tmeta" style="margin-top:6px;color:{TXT_LABEL}">{wm_info}{extra}</div>'
        f'</div>'
    )


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


def render_header():
    st.markdown(
        f'<div style="background:{TA_NAVY};border-left:6px solid {TA_ORANGE};'
        f'border-radius:8px;padding:16px 22px;margin-bottom:18px;">'
        f'<div style="font-size:1.5rem;font-weight:700;color:#FFFFFF;">'
        f'DMT — Data Migration Toolkit</div>'
        f'<div style="font-size:.82rem;color:{TXT_SECONDARY};margin-top:2px;">'
        f'Tiger Analytics &middot; Multi-source to Snowflake &middot; '
        f'Modular &middot; Resumable</div></div>', unsafe_allow_html=True)


def render_footer():
    st.markdown("---")
    st.markdown(
        f'<p style="text-align:center;color:{TXT_SECONDARY};font-size:0.8rem;">'
        f'Powered by <span style="color:{TA_ORANGE};font-weight:700;">Tiger Analytics</span>'
        f'</p>', unsafe_allow_html=True)
