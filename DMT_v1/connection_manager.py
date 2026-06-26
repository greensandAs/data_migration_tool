# CRUD operations for CONNECTION_PROFILES table in Snowflake.
"""connection_manager.py — Manage source connection profiles in Snowflake.

Each profile defines a source system (MySQL, Teradata, etc.) with host/port/user.
Passwords are never stored here — they come from Snowflake SECRETs or env vars.
"""
from __future__ import annotations


_TABLE = "HISTLOAD_DB.META.CONNECTION_PROFILES"


def list_profiles(cur, source_type: str | None = None,
                  active_only: bool = True) -> list[dict]:
    """Return connection profiles, optionally filtered by source type."""
    q = f"SELECT * FROM {_TABLE}"
    conditions = []
    params = []
    if active_only:
        conditions.append("IS_ACTIVE = TRUE")
    if source_type:
        conditions.append("SOURCE_TYPE = %s")
        params.append(source_type.lower())
    if conditions:
        q += " WHERE " + " AND ".join(conditions)
    q += " ORDER BY PROFILE_NAME"
    cur.execute(q, params)
    cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_profile(cur, profile_name: str) -> dict | None:
    """Fetch a single profile by name."""
    cur.execute(
        f"SELECT * FROM {_TABLE} WHERE PROFILE_NAME = %s", (profile_name,))
    cols = [desc[0] for desc in cur.description]
    row = cur.fetchone()
    return dict(zip(cols, row)) if row else None


def create_profile(cur, *, profile_name: str, source_type: str,
                   host: str, port: int, username: str,
                   password: str | None = None,
                   auth_secret: str | None = None,
                   logmech: str | None = None,
                   extra_params: dict | None = None) -> str:
    """Insert a new connection profile. Returns the profile name."""
    import json
    extra_json = json.dumps(extra_params) if extra_params else None
    cols = ["PROFILE_NAME", "SOURCE_TYPE", "HOST", "PORT", "USERNAME",
            "PASSWORD", "AUTH_SECRET"]
    vals = [profile_name, source_type.lower(), host, port, username,
            password, auth_secret]
    placeholders = ["%s"] * len(vals)

    if logmech:
        cols.append("LOGMECH")
        vals.append(logmech)
        placeholders.append("%s")

    if extra_json:
        cols.append("EXTRA_PARAMS")
        vals.append(extra_json)
        placeholders.append("PARSE_JSON(%s)")

    col_str = ", ".join(cols)
    ph_str = ", ".join(placeholders)
    cur.execute(
        f"INSERT INTO {_TABLE} ({col_str}) VALUES ({ph_str})", vals)
    return profile_name


def update_profile(cur, profile_name: str, **kwargs) -> str:
    """Update fields on an existing profile. Only provided kwargs are updated."""
    import json
    allowed = {"source_type", "host", "port", "username", "password",
               "auth_secret", "extra_params", "is_active"}
    sets = []
    vals = []
    for key, val in kwargs.items():
        if key not in allowed:
            continue
        col = key.upper()
        if key == "extra_params":
            sets.append(f"{col} = PARSE_JSON(%s)")
            vals.append(json.dumps(val) if val else None)
        else:
            sets.append(f"{col} = %s")
            vals.append(val)

    if not sets:
        return profile_name

    sets.append("UPDATED_AT = CURRENT_TIMESTAMP()")
    vals.append(profile_name)
    cur.execute(
        f"UPDATE {_TABLE} SET {', '.join(sets)} WHERE PROFILE_NAME = %s", vals)
    return profile_name


def delete_profile(cur, profile_name: str):
    """Hard-delete a connection profile."""
    cur.execute(f"DELETE FROM {_TABLE} WHERE PROFILE_NAME = %s", (profile_name,))


def deactivate_profile(cur, profile_name: str):
    """Soft-disable a profile (tables using it won't run)."""
    cur.execute(
        f"UPDATE {_TABLE} SET IS_ACTIVE = FALSE, UPDATED_AT = CURRENT_TIMESTAMP() "
        "WHERE PROFILE_NAME = %s", (profile_name,))


def get_source_type(cur, profile_name: str) -> str | None:
    """Quick lookup of the source_type for routing to correct extractor."""
    cur.execute(
        f"SELECT SOURCE_TYPE FROM {_TABLE} WHERE PROFILE_NAME = %s",
        (profile_name,))
    row = cur.fetchone()
    return row[0] if row else None
