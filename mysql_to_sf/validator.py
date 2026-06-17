"""validator.py — source (MySQL) vs target (Snowflake RAW) parity checks.

Levels of parity (cheapest first):
  1. Row count : MySQL COUNT(*) vs RAW *live* rows (excluding soft-deleted).
  2. Watermark : MAX(watermark_col) MySQL vs RAW (second precision).
  3. Deep hash : (opt-in) order-independent fingerprint = XOR of a 60-bit slice
                 of each row's MD5 over the business columns. Brittle by nature:
                 per-column text must match byte-for-byte across engines.
"""
from __future__ import annotations

import loader

_NULL = "~N~"
_SEP = "~|~"
_HEX = 15  # 60-bit slice — safe for BIT_XOR / BITXOR_AGG on both engines.


def _mysql_count(mysql_conn, tbl: dict) -> int:
    cur = mysql_conn.cursor()
    cur.execute(
        f"SELECT COUNT(*) FROM `{tbl['source_db']}`.`{tbl['source_table']}`")
    n = cur.fetchone()[0]
    cur.close()
    return int(n)


def _sf_count(cur, fqn: str, where: str = "") -> int:
    q = f"SELECT COUNT(*) FROM {fqn}"
    if where:
        q += f" WHERE {where}"
    cur.execute(q)
    return int(cur.fetchone()[0])


def _mysql_max_wm(mysql_conn, tbl: dict, wm: str):
    cur = mysql_conn.cursor()
    cur.execute(
        f"SELECT DATE_FORMAT(MAX(`{wm}`), '%Y-%m-%d %H:%i:%s') "
        f"FROM `{tbl['source_db']}`.`{tbl['source_table']}`")
    v = cur.fetchone()[0]
    cur.close()
    return str(v) if v is not None else None


def _sf_max_wm(cur, tbl: dict, wm: str):
    cur.execute(
        f"SELECT TO_CHAR(MAX(\"{wm}\"), 'YYYY-MM-DD HH24:MI:SS') "
        f"FROM {loader.raw_table(tbl)} "
        f'WHERE COALESCE("_IS_DELETED", FALSE) = FALSE')
    v = cur.fetchone()[0]
    return str(v) if v is not None else None


def _mysql_business_cols(mysql_conn, tbl: dict):
    cur = mysql_conn.cursor()
    cur.execute(
        "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.columns "
        "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION",
        (tbl["source_db"], tbl["source_table"]),
    )
    cols = [(r[0], str(r[1]).lower()) for r in cur.fetchall()]
    cur.close()
    return cols


def _mysql_col_expr(name: str, dtype: str) -> str:
    if dtype in ("datetime", "timestamp"):
        e = f"DATE_FORMAT(`{name}`, '%Y-%m-%d %H:%i:%s')"
    elif dtype == "date":
        e = f"DATE_FORMAT(`{name}`, '%Y-%m-%d')"
    elif dtype == "time":
        e = f"DATE_FORMAT(`{name}`, '%H:%i:%s')"
    else:
        e = f"CAST(`{name}` AS CHAR)"
    return f"COALESCE({e}, '{_NULL}')"


def _sf_col_expr(name: str, dtype: str) -> str:
    col = f'"{name.upper()}"'
    if dtype in ("datetime", "timestamp"):
        e = f"TO_CHAR({col}, 'YYYY-MM-DD HH24:MI:SS')"
    elif dtype == "date":
        e = f"TO_CHAR({col}, 'YYYY-MM-DD')"
    elif dtype == "time":
        e = f"TO_CHAR({col}, 'HH24:MI:SS')"
    else:
        e = f"TO_VARCHAR({col})"
    return f"COALESCE({e}, '{_NULL}')"


def _mysql_hash(mysql_conn, tbl: dict, cols) -> str:
    row_str = f"CONCAT_WS('{_SEP}', " + ", ".join(
        _mysql_col_expr(n, t) for n, t in cols) + ")"
    cur = mysql_conn.cursor()
    cur.execute(
        f"SELECT BIT_XOR(CAST(CONV(SUBSTR(MD5({row_str}), 1, {_HEX}), 16, 10) "
        f"AS UNSIGNED)) FROM `{tbl['source_db']}`.`{tbl['source_table']}`")
    v = cur.fetchone()[0]
    cur.close()
    return str(int(v)) if v is not None else "0"


def _sf_hash(cur, tbl: dict, cols) -> str:
    row_str = (" || '" + _SEP + "' || ").join(
        _sf_col_expr(n, t) for n, t in cols)
    cur.execute(
        f"SELECT BITXOR_AGG(TO_DECIMAL(SUBSTR(MD5({row_str}), 1, {_HEX}), "
        f"'{'X' * _HEX}')) FROM {loader.raw_table(tbl)} "
        f'WHERE COALESCE("_IS_DELETED", FALSE) = FALSE')
    v = cur.fetchone()[0]
    return str(int(v)) if v is not None else "0"


def validate_table(sf_cur, mysql_conn, tbl: dict, deep: bool = False) -> dict:
    """Return parity counts/flags for one table (source vs RAW live)."""
    source = _mysql_count(mysql_conn, tbl)
    raw_live = _sf_count(sf_cur, loader.raw_table(tbl),
                         'COALESCE("_IS_DELETED", FALSE) = FALSE')
    count_ok = source == raw_live

    wm_col = tbl.get("watermark_col")
    has_wm = bool(wm_col)
    source_wm = raw_wm = None
    wm_ok = True
    if has_wm:
        source_wm = _mysql_max_wm(mysql_conn, tbl, wm_col)
        raw_wm = _sf_max_wm(sf_cur, tbl, wm_col)
        wm_ok = source_wm == raw_wm

    source_hash = raw_hash = None
    hash_ok = True
    if deep:
        cols = _mysql_business_cols(mysql_conn, tbl)
        source_hash = _mysql_hash(mysql_conn, tbl, cols)
        raw_hash = _sf_hash(sf_cur, tbl, cols)
        hash_ok = source_hash == raw_hash

    return {
        "source": source,
        "raw_live": raw_live,
        "count_ok": count_ok,
        "delta": source - raw_live,
        "has_wm": has_wm,
        "source_wm": source_wm,
        "raw_wm": raw_wm,
        "wm_ok": wm_ok,
        "deep": deep,
        "source_hash": source_hash,
        "raw_hash": raw_hash,
        "hash_ok": hash_ok,
        "ok": count_ok and wm_ok and hash_ok,
    }
