# Shared Oracle extraction helpers — connection tuning, size estimation, parallelism.
# Co-authored with CoCo
"""extractors.oracle_common — Shared utilities for Oracle extractors.

Key optimisations over the legacy cx_Oracle approach:
  - arraysize = 10 000 (100× fewer network round-trips vs default 100)
  - prefetchrows = arraysize + 1 (eliminates initial probe round-trip)
  - streaming fetchmany with bounded batch size (constant memory)
  - Parquet output via PyArrow (columnar, compressed, faster COPY INTO)
"""
from __future__ import annotations

import math
from datetime import datetime

# ── Constants ──────────────────────────────────────────────────────────────────

DEFAULT_ARRAYSIZE = 10_000
DEFAULT_PREFETCH = DEFAULT_ARRAYSIZE + 1
DEFAULT_BATCH_ROWS = 500_000
PARALLEL_THRESHOLD_BYTES = 500 * 1024 * 1024  # 500 MB — use parallel above this
PARALLEL_DEGREE = 8


def tuned_cursor(conn, arraysize: int = DEFAULT_ARRAYSIZE):
    """Create a cursor with optimised fetch settings.

    arraysize controls how many rows Oracle sends per network round-trip.
    prefetchrows eliminates the initial "describe" round-trip for the first batch.
    """
    cur = conn.cursor()
    cur.arraysize = arraysize
    cur.prefetchrows = arraysize + 1
    return cur


def estimate_table_bytes(conn, schema: str, table: str) -> int:
    """Estimate table size in bytes from Oracle segment metadata.

    Falls back to 0 if the user lacks access to USER_SEGMENTS/ALL_SEGMENTS.
    """
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT NVL(SUM(bytes), 0) FROM all_segments "
            "WHERE owner = :1 AND segment_name = :2",
            (schema.upper(), table.upper()))
        row = cur.fetchone()
        return int(row[0]) if row else 0
    except Exception:
        return 0
    finally:
        cur.close()


def estimate_row_count(conn, schema: str, table: str) -> int:
    """Fast approximate row count from Oracle statistics (no full scan)."""
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT NVL(num_rows, 0) FROM all_tables "
            "WHERE owner = :1 AND table_name = :2",
            (schema.upper(), table.upper()))
        row = cur.fetchone()
        return int(row[0]) if row else 0
    except Exception:
        return 0
    finally:
        cur.close()


def find_numeric_pk(conn, schema: str, table: str) -> str | None:
    """Find a single numeric primary key column suitable for range partitioning.

    Returns the column name or None if no suitable PK exists.
    """
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT cc.column_name
            FROM all_constraints c
            JOIN all_cons_columns cc
              ON c.constraint_name = cc.constraint_name
             AND c.owner = cc.owner
            JOIN all_tab_columns tc
              ON tc.owner = cc.owner
             AND tc.table_name = cc.table_name
             AND tc.column_name = cc.column_name
            WHERE c.owner = :1
              AND c.table_name = :2
              AND c.constraint_type = 'P'
              AND tc.data_type = 'NUMBER'
            ORDER BY cc.position
        """, (schema.upper(), table.upper()))
        row = cur.fetchone()
        return row[0] if row else None
    except Exception:
        return None
    finally:
        cur.close()


def build_parallel_ranges(conn, schema: str, table: str,
                          pk_col: str, n_parts: int) -> list[str]:
    """Build N range-based WHERE clauses for parallel extraction.

    Returns a list of conditions like:
      ["ID >= 1 AND ID < 1000001", "ID >= 1000001 AND ID < 2000001", ...]

    The last range uses <= max to capture the final row.
    """
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT MIN({pk_col}), MAX({pk_col}) "
            f"FROM {schema}.{table}")
        row = cur.fetchone()
        if not row or row[0] is None:
            return []
        min_val, max_val = int(row[0]), int(row[1])
    except Exception:
        return []
    finally:
        cur.close()

    if min_val >= max_val:
        return [f"{pk_col} >= {min_val}"]

    range_size = math.ceil((max_val - min_val + 1) / n_parts)
    ranges = []
    for i in range(n_parts):
        lo = min_val + i * range_size
        hi = lo + range_size
        if i == n_parts - 1:
            ranges.append(f"{pk_col} >= {lo} AND {pk_col} <= {max_val}")
        else:
            ranges.append(f"{pk_col} >= {lo} AND {pk_col} < {hi}")
    return ranges


def parallel_hint(degree: int = PARALLEL_DEGREE) -> str:
    """Oracle optimizer hint for parallel query execution."""
    return f"/*+ parallel({degree}) */"


def oracle_dsn(src_cfg: dict) -> str:
    """Build Oracle Easy Connect string from src_cfg dict."""
    host = src_cfg.get("host", "localhost")
    port = src_cfg.get("port", 1521)
    service = src_cfg.get("service_name", "")
    return f"{host}:{port}/{service}"


def oracle_connect(src_cfg: dict):
    """Create a tuned Oracle connection using oracledb."""
    import oracledb
    dsn = oracle_dsn(src_cfg)
    return oracledb.connect(
        user=src_cfg["user"],
        password=src_cfg.get("password", ""),
        dsn=dsn)


def rows_to_arrow(rows: list, col_names: list[str], col_types: list):
    """Convert fetched rows to a PyArrow Table.

    Handles Oracle-specific types (datetime, Decimal, LOB) gracefully.
    """
    import pyarrow as pa
    from decimal import Decimal

    if not rows:
        return pa.table({name: pa.array([], type=pa.string()) for name in col_names})

    # Transpose row-major → column-major
    n_cols = len(col_names)
    col_data = [[] for _ in range(n_cols)]
    for row in rows:
        for i in range(n_cols):
            val = row[i]
            # Handle Oracle LOB objects (CLOB/BLOB)
            if hasattr(val, 'read'):
                val = val.read()
            col_data[i].append(val)

    arrays = []
    for i, data in enumerate(col_data):
        try:
            arrays.append(pa.array(data))
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            # Fallback: convert to strings
            arrays.append(pa.array([str(v) if v is not None else None for v in data]))

    return pa.table(dict(zip(col_names, arrays)))
