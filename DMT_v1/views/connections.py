# Connection profile management page — branded CRUD for source system connections.
# Co-authored with CoCo
"""pages/connections.py — Manage source connection profiles.

Provides forms to create, edit, test, and delete connection profiles stored
in HISTLOAD_DB.META.CONNECTION_PROFILES. Styled with Tiger Analytics theme.
"""
from __future__ import annotations

import os

import streamlit as st

import connection_manager


class theme:
    @staticmethod
    def section_header(text: str):
        st.markdown(f'<div class="section-header">{text}</div>', unsafe_allow_html=True)

    @staticmethod
    def source_type_pill(source_type: str) -> str:
        return f'<span class="pill pill-source">{source_type or "unknown"}</span>'


def _test_mysql(host: str, port: int, user: str, password: str) -> tuple[bool, str]:
    try:
        import mysql.connector
        conn = mysql.connector.connect(
            host=str(host), port=int(port), user=str(user),
            password=str(password) if password else "",
            connect_timeout=10)
        cur = conn.cursor()
        cur.execute("SELECT VERSION()")
        version = cur.fetchone()[0]
        cur.close()
        conn.close()
        return True, f"MySQL {version}"
    except Exception as e:
        return False, str(e)[:100]


def _test_teradata(host: str, port: int, user: str, password: str) -> tuple[bool, str]:
    try:
        import teradatasql
        conn = teradatasql.connect(host=host, user=user, password=password)
        cur = conn.cursor()
        cur.execute("SELECT DATABASE")
        db = cur.fetchone()[0]
        cur.close()
        conn.close()
        return True, f"Default DB: {db}"
    except Exception as e:
        return False, str(e)[:100]


def render(conn):
    """Main render function for the Connections page."""
    cur = conn.cursor()

    # ── Create form ───────────────────────────────────────────────────────────
    theme.section_header("New Connection Profile")

    with st.form("create_profile", clear_on_submit=True):
        # Get allowed sources from settings
        from shared import get_allowed_sources
        allowed_sources = get_allowed_sources(cur)

        c1, c2, c3 = st.columns(3)
        with c1:
            profile_name = st.text_input("Profile Name", placeholder="mysql_prod")
            source_type = st.selectbox("Source Type", allowed_sources)
        with c2:
            host = st.text_input("Host", placeholder="10.0.0.1 or hostname")
            port = st.number_input("Port", value=3306, min_value=1, max_value=65535,
                                   help="MySQL: 3306, Teradata: 1025 (not used by teradatasql)")
        with c3:
            username = st.text_input("Username", placeholder="etl_user")
            password = st.text_input("Password", type="password",
                                     placeholder="Source DB password")
            logmech = st.selectbox("Auth Method", ["TD2", "LDAP"],
                                   help="Teradata only: TD2=native, LDAP=enterprise")

        submitted = st.form_submit_button("➕ Create Profile", type="primary",
                                          use_container_width=True)
        if submitted:
            if not profile_name or not host or not username:
                st.error("Profile name, host, and username are required.")
            elif not password:
                st.error("Password is required.")
            else:
                try:
                    connection_manager.create_profile(
                        cur, profile_name=profile_name.strip(),
                        source_type=source_type,
                        host=host.strip(), port=int(port),
                        username=username.strip(),
                        password=password,
                        logmech=logmech if source_type == "teradata" else None,
                    )
                    st.success(f"Profile `{profile_name}` created.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed: {e}")

    # ── Existing profiles ─────────────────────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    theme.section_header("Existing Profiles")

    profiles = connection_manager.list_profiles(cur, active_only=False)
    if not profiles:
        from shared import empty_state
        empty_state("🔌", "No Connection Profiles",
                    "Create a new source connection profile using the form above.")
        cur.close()
        return

    for p in profiles:
        active_badge = "🟢" if p.get("IS_ACTIVE", True) else "🔴"
        src = p.get("SOURCE_TYPE", "?")
        label = (f"{active_badge} **{p['PROFILE_NAME']}** — "
                 f"{theme.source_type_pill(src)} `{p['HOST']}:{p['PORT']}`")

        with st.expander(f"{active_badge} {p['PROFILE_NAME']} — {src} @ {p['HOST']}:{p['PORT']}"):
            # Info row
            st.markdown(
                f'<div class="ns-box">'
                f'<span class="ns-label">USERNAME : </span>'
                f'<span class="ns-value">{p.get("USERNAME", "?")}</span><br>'
                f'<span class="ns-label">AUTH     : </span>'
                f'<span class="ns-value">{p.get("AUTH_SECRET") or "N/A"}</span><br>'
                f'<span class="ns-label">STATUS   : </span>'
                f'<span class="ns-value">{"Active" if p.get("IS_ACTIVE", True) else "Inactive"}</span>'
                f'</div>', unsafe_allow_html=True)

            # Action buttons
            col1, col2, col3 = st.columns(3)
            with col1:
                if st.button("🧪 Test Connection", key=f"test_{p['PROFILE_NAME']}",
                             use_container_width=True):
                    # Password: directly from table, fallback to env var
                    pwd = p.get("PASSWORD") or os.getenv(p.get("AUTH_SECRET") or "", "") or ""
                    if not pwd:
                        st.warning("No password found in profile or environment.")
                    elif src == "mysql":
                        ok, msg = _test_mysql(p["HOST"], int(p["PORT"]), p["USERNAME"], pwd)
                    elif src == "teradata":
                        ok, msg = _test_teradata(p["HOST"], int(p["PORT"]), p["USERNAME"], pwd)
                    else:
                        ok, msg = False, f"Unknown: {src}"
                    if pwd:
                        if ok:
                            st.success(f"✅ Connected — {msg}")
                        else:
                            st.error(f"❌ {msg}")

            with col2:
                if p.get("IS_ACTIVE", True):
                    if st.button("⏸️ Deactivate", key=f"deact_{p['PROFILE_NAME']}",
                                 use_container_width=True):
                        connection_manager.deactivate_profile(cur, p["PROFILE_NAME"])
                        st.rerun()
                else:
                    if st.button("▶️ Activate", key=f"act_{p['PROFILE_NAME']}",
                                 use_container_width=True):
                        connection_manager.update_profile(cur, p["PROFILE_NAME"], is_active=True)
                        st.rerun()

            with col3:
                if st.button("🗑️ Delete", key=f"del_{p['PROFILE_NAME']}",
                             use_container_width=True):
                    connection_manager.delete_profile(cur, p["PROFILE_NAME"])
                    st.rerun()

    cur.close()
