# Connection profile management page — branded CRUD for source system connections.
# Co-authored with CoCo
"""pages/connections.py — Manage source connection profiles.

Provides forms to create, edit, test, and delete connection profiles stored
in HISTLOAD_DB.META.CONNECTION_PROFILES. Field sets are driven by
metadata.source_specs so each source type only asks for what it needs.

POC scope: username + password authentication only.
"""
from __future__ import annotations

import json
import os

import streamlit as st

from metadata import connection_manager
from metadata import source_specs


class theme:
    @staticmethod
    def section_header(text: str):
        st.markdown(f'<div class="section-header">{text}</div>', unsafe_allow_html=True)

    @staticmethod
    def source_type_pill(source_type: str) -> str:
        return f'<span class="pill pill-source">{source_type or "unknown"}</span>'


def _as_dict(value) -> dict:
    """Normalize a Snowflake VARIANT column (dict or JSON string) to a dict."""
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except ValueError:
            return {}
    return {}


def _connection_errors(source_type: str, host: str, username: str,
                       password: str, extras: dict) -> list[str]:
    """Validate only the fields needed to OPEN a connection.

    Deliberately excludes profile name — a credential probe does not need one.
    """
    errors = []
    if not (host or "").strip():
        errors.append("Host is required.")
    if not (username or "").strip():
        errors.append("Username is required.")
    if not password:
        errors.append("Password is required.")
    errors += source_specs.validate_extras(source_type, extras)
    return errors


def _probe_profile(source_type: str, host: str, port, username: str,
                   password: str, extras: dict) -> dict:
    """Build an unsaved, profile-shaped dict for connection_manager.test_connection."""
    return {
        "SOURCE_TYPE": source_type,
        "HOST": (host or "").strip(),
        "PORT": int(port) if port else source_specs.default_port(source_type),
        "USERNAME": (username or "").strip(),
        "PASSWORD": password,
        "LOGMECH": "TD2" if source_type == "teradata" else None,
        "EXTRA_PARAMS": extras or {},
    }


def _show_test_result(ok: bool, msg: str):
    if ok:
        st.success(f"✅ Connected — {msg}")
    else:
        st.error(f"❌ {msg}")


def _render_extra_fields(source_type: str, key_prefix: str,
                         existing: dict | None = None) -> dict:
    """Render the source-specific extra inputs. Returns {key: value}."""
    existing = existing or {}
    values = {}
    for field in source_specs.extra_fields(source_type):
        fkey = field["key"]
        widget_key = f"{key_prefix}_{fkey}"
        label = field["label"] + (" *" if field.get("required") else "")
        current = existing.get(fkey) or field.get("default", "")

        if field.get("widget") == "select":
            options = field.get("options", [])
            idx = options.index(current) if current in options else 0
            values[fkey] = st.selectbox(label, options, index=idx,
                                        key=widget_key, help=field.get("help"))
        else:
            values[fkey] = st.text_input(label, value=current,
                                         placeholder=field.get("placeholder", ""),
                                         key=widget_key, help=field.get("help"))
    return values


def render(conn):
    """Main render function for the Connections page."""
    cur = conn.cursor()

    from utils.shared import get_allowed_sources
    allowed_sources = get_allowed_sources(cur)

    # ── Create form ───────────────────────────────────────────────────────────
    theme.section_header("New Connection Profile")

    # Source type lives OUTSIDE the form so changing it reruns and swaps fields.
    sel_col, note_col = st.columns([1, 2])
    with sel_col:
        source_type = st.selectbox(
            "Source Type", allowed_sources,
            format_func=source_specs.source_label,
            key="new_profile_source_type")
    with note_col:
        if not source_specs.extractor_ready(source_type):
            st.info(f"{source_specs.source_label(source_type)} connections can be "
                    f"saved and tested, but migrations are not supported yet "
                    f"(no extractor).", icon="ℹ️")

    spec = source_specs.get_spec(source_type)

    # clear_on_submit must stay False: the Test button is also a submit, and
    # clearing would wipe the form every time you probe credentials. Fields are
    # cleared explicitly (via their keys) only after a successful create.
    with st.form("create_profile", clear_on_submit=False):
        c1, c2, c3 = st.columns(3)
        with c1:
            profile_name = st.text_input(
                "Profile Name *", placeholder=f"{source_type}_prod",
                key="np_profile_name")
            username = st.text_input("Username *", placeholder="etl_user",
                                     key="np_username")
        with c2:
            host = st.text_input("Host *", placeholder="10.0.0.1 or hostname",
                                 key="np_host")
            if source_specs.uses_port(source_type):
                port = st.number_input(
                    "Port *", value=source_specs.default_port(source_type),
                    min_value=1, max_value=65535, key="np_port")
            else:
                port = source_specs.default_port(source_type)
                st.caption(f"Port not used — {spec.get('port_note', '')}")
        with c3:
            password = st.text_input("Password *", type="password",
                                     placeholder="Source DB password",
                                     key="np_password")
            extras = _render_extra_fields(source_type, "new")

        b1, b2 = st.columns([1, 1])
        tested = b1.form_submit_button("🧪 Test Credentials",
                                      use_container_width=True)
        submitted = b2.form_submit_button("➕ Create Profile", type="primary",
                                          use_container_width=True)

        # ── Test without saving ───────────────────────────────────────────────
        if tested:
            errors = _connection_errors(source_type, host, username,
                                        password, extras)
            if errors:
                for err in errors:
                    st.error(err)
            else:
                probe = _probe_profile(source_type, host, port, username,
                                       password, extras)
                with st.spinner("Connecting…"):
                    ok, msg = connection_manager.test_connection(probe)
                _show_test_result(ok, msg)
                if ok:
                    st.caption("Credentials valid — click **Create Profile** to save.")

        # ── Create ────────────────────────────────────────────────────────────
        if submitted:
            errors = []
            if not profile_name.strip():
                errors.append("Profile name is required.")
            errors += _connection_errors(source_type, host, username,
                                         password, extras)

            if errors:
                for err in errors:
                    st.error(err)
            else:
                try:
                    connection_manager.create_profile(
                        cur, profile_name=profile_name.strip(),
                        source_type=source_type,
                        host=host.strip(), port=int(port),
                        username=username.strip(),
                        password=password,
                        # POC: password auth only. TD2 is Teradata's native mech.
                        logmech="TD2" if source_type == "teradata" else None,
                        extra_params=extras or None,
                    )
                    st.session_state.pop("_profiles_list", None)  # refresh sidebar
                    # Clear the form now that it succeeded (clear_on_submit=False).
                    for k in ("np_profile_name", "np_username", "np_host",
                              "np_port", "np_password", "new_driver",
                              "new_service_name"):
                        st.session_state.pop(k, None)
                    st.success(f"Profile `{profile_name}` created.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed: {e}")

    # ── Existing profiles ─────────────────────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    theme.section_header("Existing Profiles")

    profiles = connection_manager.list_profiles(cur, active_only=False)
    if not profiles:
        from utils.shared import empty_state
        empty_state("🔌", "No Connection Profiles",
                    "Create a new source connection profile using the form above.")
        cur.close()
        return

    for p in profiles:
        pname = p["PROFILE_NAME"]
        src = (p.get("SOURCE_TYPE") or "?").lower()
        active_badge = "🟢" if p.get("IS_ACTIVE", True) else "🔴"
        p_extras = _as_dict(p.get("EXTRA_PARAMS"))
        port_display = p.get("PORT") if source_specs.uses_port(src) else "n/a"

        with st.expander(f"{active_badge} {pname} — {src} @ {p.get('HOST','?')}:{port_display}"):
            # ── Detail block (all stored fields) ──────────────────────────────
            extra_rows = "".join(
                f'<span class="ns-label">{k.upper():<9}: </span>'
                f'<span class="ns-value">{v}</span><br>'
                for k, v in p_extras.items())
            st.markdown(
                f'<div class="ns-box">'
                f'<span class="ns-label">SOURCE   : </span>'
                f'<span class="ns-value">{source_specs.source_label(src)}</span><br>'
                f'<span class="ns-label">HOST     : </span>'
                f'<span class="ns-value">{p.get("HOST", "?")}</span><br>'
                f'<span class="ns-label">PORT     : </span>'
                f'<span class="ns-value">{port_display}</span><br>'
                f'<span class="ns-label">USERNAME : </span>'
                f'<span class="ns-value">{p.get("USERNAME", "?")}</span><br>'
                f'<span class="ns-label">PASSWORD : </span>'
                f'<span class="ns-value">{"•••••• (stored)" if p.get("PASSWORD") else "not set"}</span><br>'
                f'<span class="ns-label">ENV VAR  : </span>'
                f'<span class="ns-value">{p.get("AUTH_SECRET") or "N/A"}</span><br>'
                f'{extra_rows}'
                f'<span class="ns-label">STATUS   : </span>'
                f'<span class="ns-value">{"Active" if p.get("IS_ACTIVE", True) else "Inactive"}</span>'
                f'</div>', unsafe_allow_html=True)

            if not source_specs.extractor_ready(src):
                st.warning(f"{source_specs.source_label(src)} has no extractor yet — "
                           f"pipelines using this profile will fail.", icon="⚠️")

            # ── Edit form ─────────────────────────────────────────────────────
            with st.form(f"edit_{pname}"):
                st.caption("Edit profile — leave password blank to keep the current one.")
                e1, e2, e3 = st.columns(3)
                with e1:
                    new_host = st.text_input("Host", value=p.get("HOST", ""),
                                             key=f"eh_{pname}")
                    new_username = st.text_input("Username",
                                                 value=p.get("USERNAME", ""),
                                                 key=f"eu_{pname}")
                with e2:
                    if source_specs.uses_port(src):
                        new_port = st.number_input(
                            "Port", value=int(p.get("PORT") or source_specs.default_port(src)),
                            min_value=1, max_value=65535, key=f"ep_{pname}")
                    else:
                        new_port = int(p.get("PORT") or source_specs.default_port(src))
                        st.caption("Port not used for this source.")
                    new_password = st.text_input("New Password", type="password",
                                                 placeholder="(unchanged)",
                                                 key=f"epw_{pname}")
                with e3:
                    new_extras = _render_extra_fields(src, f"e_{pname}", p_extras)

                eb1, eb2 = st.columns([1, 1])
                edit_tested = eb1.form_submit_button("🧪 Test These Values",
                                                     use_container_width=True)
                edit_saved = eb2.form_submit_button("💾 Save Changes",
                                                    type="primary",
                                                    use_container_width=True)

                # Probe the edited values without saving. A blank password
                # means "unchanged", so fall back to the stored credential.
                if edit_tested:
                    probe_pwd = (new_password
                                 or p.get("PASSWORD")
                                 or os.getenv(p.get("AUTH_SECRET") or "", "")
                                 or "")
                    errors = _connection_errors(src, new_host, new_username,
                                                probe_pwd, new_extras)
                    if errors:
                        for err in errors:
                            st.error(err)
                    else:
                        probe = _probe_profile(src, new_host, new_port,
                                               new_username, probe_pwd,
                                               new_extras)
                        with st.spinner("Connecting…"):
                            ok, msg = connection_manager.test_connection(probe)
                        _show_test_result(ok, msg)
                        if ok:
                            st.caption("These values work — click **Save Changes** "
                                       "to persist them.")

                if edit_saved:
                    errors = []
                    if not new_host.strip():
                        errors.append("Host is required.")
                    if not new_username.strip():
                        errors.append("Username is required.")
                    errors += source_specs.validate_extras(src, new_extras)

                    if errors:
                        for err in errors:
                            st.error(err)
                    else:
                        updates = {
                            "host": new_host.strip(),
                            "port": int(new_port),
                            "username": new_username.strip(),
                            "extra_params": new_extras or None,
                        }
                        if new_password:
                            updates["password"] = new_password
                        try:
                            connection_manager.update_profile(cur, pname, **updates)
                            st.session_state.pop("_profiles_list", None)
                            st.success("Profile updated.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Update failed: {e}")

            # ── Action buttons ────────────────────────────────────────────────
            col1, col2, col3 = st.columns(3)
            with col1:
                if st.button("🧪 Test Saved Profile", key=f"test_{pname}",
                             use_container_width=True,
                             help="Tests the credentials currently stored in "
                                  "Snowflake, ignoring any unsaved edits above."):
                    # Password: directly from table, fallback to env var
                    pwd = p.get("PASSWORD") or os.getenv(p.get("AUTH_SECRET") or "", "") or ""
                    if not pwd:
                        st.warning("No password found in profile or environment.")
                    else:
                        test_profile = {**p, "PASSWORD": pwd,
                                        "EXTRA_PARAMS": p_extras}
                        with st.spinner("Connecting…"):
                            ok, msg = connection_manager.test_connection(test_profile)
                        _show_test_result(ok, msg)

            with col2:
                if p.get("IS_ACTIVE", True):
                    if st.button("⏸️ Deactivate", key=f"deact_{pname}",
                                 use_container_width=True):
                        connection_manager.deactivate_profile(cur, pname)
                        st.session_state.pop("_profiles_list", None)
                        st.rerun()
                else:
                    if st.button("▶️ Activate", key=f"act_{pname}",
                                 use_container_width=True):
                        connection_manager.update_profile(cur, pname, is_active=True)
                        st.session_state.pop("_profiles_list", None)
                        st.rerun()

            with col3:
                if st.button("🗑️ Delete", key=f"del_{pname}",
                             use_container_width=True):
                    connection_manager.delete_profile(cur, pname)
                    st.session_state.pop("_profiles_list", None)
                    st.rerun()

    cur.close()
