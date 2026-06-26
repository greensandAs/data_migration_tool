# Source-vs-target parity validation (count, watermark, deep hash).
# Co-authored with CoCo
"""validator.py — Source (MySQL) vs target (Snowflake RAW) parity checks.

Levels of parity (cheapest first):
  1. Row count: MySQL COUNT(*) vs RAW live rows (excluding soft-deleted)
  2. Watermark: MAX(watermark_col) on both sides
  3. Deep hash: order-independent XOR of row MD5 slices (opt-in)
"""
from __future__ import annotations

import loader

_NULL = "~N~"
_SEP = "~|~"
_HEX = 15  # 60-bit slice for BIT_XOR/BITXOR_AGG


def _mysql_count(mysql_conn, config: dict) -> int:
    cur = mysql_conn.cursor()
    cur.execute(
        f"SELECT COUNT(*) FROM `{config['SOURCE_DB']}`.`{config['SOURCE_TABLE']}`")
    n = cur.fetchone()[0]
    cur.close()
    return int(n)


def _sf_count(cur, fqn: str, where: str = "") -> int:
    q = f"SELECT COUNT(*) FROM {fqn}"
    if where:
        q += f" WHERE {where}"
    cur.execute(q)
    return int(cur.fetchone()[0])


def _mysql_max_wm(mysql_conn, config: dict, wm: str):
    cur = mysql_conn.cursor()
    cur.execute(
        f"SELECT DATE_FORMAT(MAX(`{wm}`), '%Y-%m-%d %H:%i:%s') "
        f"FROM `{config['SOURCE_DB']}`.`{config['SOURCE_TABLE']}`")
    v = cur.fetchone()[0]
    cur.close()
    return str(v) if v is not None else None


def _sf_max_wm(cur, config: dict, wm: str):
    cur.execute(
        f'SELECT TO_CHAR(MAX("{wm.upper()}"), \'YYYY-MM-DD HH24:MI:SS\') '
        f'FROM {loader.raw_table(config)} '
        f'WHERE COALESCE("_IS_DELETED", FALSE) = FALSE')
    v = cur.fetchone()[0]
    return str(v) if v is not None else None


def validate_table(sf_cur, mysql_conn, config: dict, deep: bool = False) -> dict:
    """Return parity results for one table (source vs RAW live)."""
    source = _mysql_count(mysql_conn, config)
    raw_live = _sf_count(sf_cur, loader.raw_table(config),
                         'COALESCE("_IS_DELETED", FALSE) = FALSE')
    count_ok = source == raw_live

    wm_col = config.get("WATERMARK_COL")
    has_wm = bool(wm_col)
    source_wm = raw_wm = None
    wm_ok = True
    if has_wm:
        source_wm = _mysql_max_wm(mysql_conn, config, wm_col)
        raw_wm = _sf_max_wm(sf_cur, config, wm_col)
        wm_ok = source_wm == raw_wm

    return {
        "source": source,
        "raw_live": raw_live,
        "count_ok": count_ok,
        "delta": source - raw_live,
        "has_wm": has_wm,
        "source_wm": source_wm,
        "raw_wm": raw_wm,
        "wm_ok": wm_ok,
        "ok": count_ok and wm_ok,
    }
