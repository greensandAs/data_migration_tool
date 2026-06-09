"""reconciler.py — Delete reconciliation (soft-delete).

Compares the set of (composite) keys in MySQL against those in <DB>.RAW.<table>.
Keys present in Snowflake but absent in MySQL were deleted at the source; they
are soft-deleted in RAW (_IS_DELETED=TRUE, _DELETED_AT=now).

Invoked via `python orchestrator.py --reconcile`. Run on its own cadence — it
scans full key sets, so it's heavier than an incremental run. Tables without a
key are skipped.
"""
from __future__ import annotations

import loader

DELETE_BATCH = 5000


def _norm(r) -> tuple:
    """Normalize a key row to a tuple of strings (None preserved) so a MySQL
    Decimal/int/date matches a Snowflake str (e.g. an overflow DECIMAL stored as
    VARCHAR)."""
    return tuple(str(v) if v is not None else None for v in r)


def _mysql_pks(mysql_conn, tbl: dict, keys: list) -> set:
    cur = mysql_conn.cursor()
    col_list = ", ".join(f"`{k}`" for k in keys)
    cur.execute(
        f"SELECT {col_list} FROM `{tbl['source_db']}`.`{tbl['source_table']}`")
    pks = {_norm(r) for r in cur.fetchall()}
    cur.close()
    return pks


def _snowflake_live_pks(cur, tbl: dict, keys: list) -> set:
    col_list = ", ".join(f'"{k}"' for k in keys)
    cur.execute(
        f'SELECT {col_list} FROM {loader.raw_table(tbl)} '
        f'WHERE COALESCE("_IS_DELETED", FALSE) = FALSE')
    return {_norm(r) for r in cur.fetchall()}


def _sql_literal(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def reconcile_table(cur, mysql_conn, tbl: dict) -> dict:
    """Soft-delete RAW rows whose (composite) key no longer exists in MySQL."""
    keys = loader.merge_keys(tbl)
    if not keys:
        return {"deleted": 0, "skipped": "no primary key"}

    mysql_pks = _mysql_pks(mysql_conn, tbl, keys)
    sf_pks = _snowflake_live_pks(cur, tbl, keys)
    deleted_pks = list(sf_pks - mysql_pks)
    if not deleted_pks:
        return {"deleted": 0, "skipped": None}

    target = loader.raw_table(tbl)
    key_cols = "(" + ", ".join(f'"{k}"' for k in keys) + ")"
    total = 0
    for i in range(0, len(deleted_pks), DELETE_BATCH):
        chunk = deleted_pks[i:i + DELETE_BATCH]
        tuples = ", ".join(
            "(" + ", ".join(_sql_literal(v) for v in key_tuple) + ")"
            for key_tuple in chunk)
        cur.execute(
            f'UPDATE {target} '
            f'SET "_IS_DELETED" = TRUE, "_DELETED_AT" = CURRENT_TIMESTAMP() '
            f'WHERE {key_cols} IN ({tuples}) '
            f'AND COALESCE("_IS_DELETED", FALSE) = FALSE')
        total += cur.rowcount or 0

    return {"deleted": total, "skipped": None}
