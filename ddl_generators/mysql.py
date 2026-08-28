# MySQL-specific DDL generation (re-exports from package __init__).
"""ddl_generators.mysql — MySQL-specific entry point.

All MySQL DDL logic lives in ddl_generators/__init__.py for simplicity.
This module re-exports for explicit imports like:
    from ddl_generators.mysql import generate_and_apply
"""
from ddl_generators import (
    RAW_SCHEMA,
    AUDIT_COLS,
    SCD2_COLS,
    target_db,
    map_mysql_type,
    get_mysql_columns,
    build_table_ddl,
    generate_and_apply,
)

__all__ = [
    "RAW_SCHEMA", "AUDIT_COLS", "SCD2_COLS", "target_db", "map_mysql_type",
    "get_mysql_columns", "build_table_ddl", "generate_and_apply",
]
