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
    selects = ["%s"] * len(vals)

    if logmech:
        cols.append("LOGMECH")
        vals.append(logmech)
        selects.append("%s")

    if extra_json:
        cols.append("EXTRA_PARAMS")
        vals.append(extra_json)
        selects.append("PARSE_JSON(%s)")

    col_str = ", ".join(cols)
    sel_str = ", ".join(selects)
    cur.execute(
        f"INSERT INTO {_TABLE} ({col_str}) SELECT {sel_str}", vals)
    return profile_name


def update_profile(cur, profile_name: str, **kwargs) -> str:
    """Update fields on an existing profile. Only provided kwargs are updated."""
    import json
    allowed = {"source_type", "host", "port", "username", "password",
               "auth_secret", "logmech", "extra_params", "is_active"}
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


def test_connection(profile: dict) -> tuple[bool, str]:
    """Test connectivity to source database. Returns (success, message)."""
    source_type = (profile.get("SOURCE_TYPE") or "").lower()
    host = profile.get("HOST", "")
    port = int(profile.get("PORT") or 0)
    user = profile.get("USERNAME", "")
    password = profile.get("PASSWORD", "")
    extra = profile.get("EXTRA_PARAMS") or {}
    if isinstance(extra, str):
        import json
        try:
            extra = json.loads(extra)
        except ValueError:
            extra = {}

    try:
        if source_type == "mysql":
            import mysql.connector
            conn = mysql.connector.connect(
                host=host, port=port or 3306, user=user, password=password,
                connect_timeout=10)
            cur = conn.cursor()
            cur.execute("SELECT VERSION()")
            version = cur.fetchone()[0]
            cur.close()
            conn.close()
            return True, f"MySQL {version}"

        elif source_type == "teradata":
            import teradatasql
            logmech = profile.get("LOGMECH") or "TD2"
            conn = teradatasql.connect(host=host, user=user, password=password,
                                       logmech=logmech)
            cur = conn.cursor()
            cur.execute("SELECT DATABASE")
            db = cur.fetchone()[0]
            cur.close()
            conn.close()
            return True, f"Teradata · default DB: {str(db).strip()}"

        elif source_type == "mssql":
            import pyodbc
            driver = extra.get("driver", "ODBC Driver 17 for SQL Server")
            conn_str = (
                f"DRIVER={{{driver}}};SERVER={host},{port or 1433};"
                f"UID={user};PWD={password};"
                "Encrypt=yes;TrustServerCertificate=yes;"
                "Connection Timeout=10;"
            )
            conn = pyodbc.connect(conn_str)
            cur = conn.cursor()
            cur.execute("SELECT @@VERSION")
            version = (cur.fetchone()[0] or "").split("\n")[0].strip()
            cur.close()
            conn.close()
            return True, version[:120] or "MSSQL connected"

        elif source_type == "oracle":
            import oracledb
            service = extra.get("service_name") or ""
            if not service:
                return False, "Service Name is required for Oracle connections."
            dsn = f"{host}:{port or 1521}/{service}"
            conn = oracledb.connect(user=user, password=password, dsn=dsn)
            cur = conn.cursor()
            cur.execute("SELECT banner FROM v$version WHERE ROWNUM = 1")
            row = cur.fetchone()
            banner = (row[0] if row else "") or "Oracle connected"
            cur.close()
            conn.close()
            return True, str(banner)[:120]

        else:
            return False, f"Unknown source type: {source_type}"

    except Exception as e:
        return False, str(e)[:500]
