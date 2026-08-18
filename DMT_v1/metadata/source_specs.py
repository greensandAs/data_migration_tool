# Per-source connection field specifications — single source of truth.
"""metadata.source_specs — Declarative per-source connection requirements.

Defines what input fields each source type needs so the UI form, the
connection builder, and the test-connection helper all agree. Adding a new
source means adding one entry here plus an extractor/ddl_generator.

POC scope: username + password authentication only. Advanced auth
(Kerberos, LDAP, JWT, wallets, managed identity) is intentionally excluded.
"""
from __future__ import annotations


# ODBC drivers commonly installed for SQL Server. Driver 18 defaults
# Encrypt=yes, driver 17 defaults Encrypt=no — the choice matters.
MSSQL_DRIVERS = [
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
    "SQL Server Native Client 11.0",
    "FreeTDS",
]


SOURCE_SPECS: dict[str, dict] = {
    "mysql": {
        "label": "MySQL",
        "default_port": 3306,
        "uses_port": True,
        "extractor_ready": True,
        "extra_fields": [],
    },
    "mssql": {
        "label": "SQL Server (MSSQL)",
        "default_port": 1433,
        "uses_port": True,
        "extractor_ready": True,
        "extra_fields": [
            {
                "key": "driver",
                "label": "ODBC Driver",
                "widget": "select",
                "options": MSSQL_DRIVERS,
                "default": MSSQL_DRIVERS[0],
                "required": True,
                "help": "Must match a driver installed on the host running DMT. "
                        "Driver 18 enables encryption by default; driver 17 does not.",
            },
        ],
    },
    "teradata": {
        "label": "Teradata",
        "default_port": 1025,
        "uses_port": False,   # teradatasql resolves the port itself
        "extractor_ready": True,
        "extra_fields": [],
        "port_note": "Teradata connections ignore the port — teradatasql resolves it.",
    },
    "oracle": {
        "label": "Oracle",
        "default_port": 1521,
        "uses_port": True,
        "extractor_ready": True,
        "extra_fields": [
            {
                "key": "service_name",
                "label": "Service Name",
                "widget": "text",
                "default": "",
                "required": True,
                "placeholder": "FREEPDB1 / ORCLPDB1 / XEPDB1",
                "help": "Oracle DSN is host:port/service_name. Find it with "
                        "SELECT value FROM v$parameter WHERE name = 'service_names'.",
            },
        ],
    },
}


def get_spec(source_type: str) -> dict:
    """Return the spec for a source type, falling back to a MySQL-like default."""
    return SOURCE_SPECS.get((source_type or "").lower(), SOURCE_SPECS["mysql"])


def default_port(source_type: str) -> int:
    return int(get_spec(source_type).get("default_port", 3306))


def uses_port(source_type: str) -> bool:
    return bool(get_spec(source_type).get("uses_port", True))


def extra_fields(source_type: str) -> list[dict]:
    """Extra (source-specific) input fields, stored in EXTRA_PARAMS."""
    return list(get_spec(source_type).get("extra_fields", []))


def extractor_ready(source_type: str) -> bool:
    """False when a source can be configured/tested but has no extractor yet."""
    return bool(get_spec(source_type).get("extractor_ready", False))


def source_label(source_type: str) -> str:
    return get_spec(source_type).get("label", (source_type or "").upper())


def validate_extras(source_type: str, extras: dict | None) -> list[str]:
    """Return a list of human-readable validation errors for extra params."""
    extras = extras or {}
    errors = []
    for field in extra_fields(source_type):
        if field.get("required") and not str(extras.get(field["key"], "") or "").strip():
            errors.append(f"{field['label']} is required for "
                          f"{source_label(source_type)}.")
    return errors


# ── Extraction output contract ────────────────────────────────────────────────
# What each source's extractors actually write to disk, and which engine does it.
# Keyed (source_type, is_full). These MUST stay in sync with the ExtractionResult
# returned by extractors/<source>_{full,incremental}.py — the loader picks its
# Snowflake FILE FORMAT and glob PATTERN from this, and a mismatch makes
# COPY INTO match zero files and silently load nothing.
_OUTPUT: dict[str, tuple[str, str]] = {
    # source:      (full_format, incremental_format)
    "mysql":       ("tsv_zstd", "parquet"),
    "teradata":    ("csv",      "parquet"),
    "mssql":       ("csv_gzip", "csv_gzip"),
    "oracle":      ("parquet",  "parquet"),
}

_ENGINE: dict[str, tuple[str, str]] = {
    # source:      (full_engine, incremental_engine)
    "mysql":       ("mysqlsh", "connectorx"),
    "teradata":    ("tpt",     "teradatasql"),
    "mssql":       ("bcp",     "bcp"),
    "oracle":      ("oracledb", "oracledb"),
}


def output_format(source_type: str, is_full: bool) -> str:
    """File format this source's extractor produces.

    Needed for LOAD_ONLY runs, where the extract step never ran so there is no
    ExtractionResult to read file_format from.
    """
    full_fmt, incr_fmt = _OUTPUT.get((source_type or "").lower(),
                                     _OUTPUT["mysql"])
    return full_fmt if is_full else incr_fmt


def engine_name(source_type: str, is_full: bool) -> str:
    """Engine label recorded in RUN_LOG.ENGINE."""
    full_eng, incr_eng = _ENGINE.get((source_type or "").lower(),
                                     _ENGINE["mysql"])
    return full_eng if is_full else incr_eng
