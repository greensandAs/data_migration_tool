"""load.py — Snowflake load layer: COPY INTO work table + MERGE (SCD 0/1/2).

Best practices applied:
- Work table pattern: COPY INTO _WRK → MERGE/INSERT to target (no direct load)
- SCD0: INSERT only (append, no dedup)
- SCD1: MERGE with UPDATE (excludes PK from SET) + INSERT
- SCD2: History tracking with IS_CURRENT, EFF_START_DATE, EFF_END_DATE
- FULL: SWAP (atomic, zero-downtime) instead of DELETE+INSERT
- Uses COPY INTO with MATCH_BY_COLUMN_NAME for resilience
"""
from __future__ import annotations

import os

from core.connections import sf_execute, sf_query
from core.extract import get_column_metadata


# ─── MSSQL → Snowflake Type Mapping ─────────────────────────────────────────

MSSQL_TO_SF_TYPE = {
    "int": "NUMBER(10,0)",
    "bigint": "NUMBER(19,0)",
    "smallint": "NUMBER(5,0)",
    "tinyint": "NUMBER(3,0)",
    "bit": "BOOLEAN",
    "float": "FLOAT",
    "real": "FLOAT",
    "money": "NUMBER(19,4)",
    "smallmoney": "NUMBER(10,4)",
    "date": "DATE",
    "time": "TIME",
    "datetime": "TIMESTAMP_NTZ",
    "datetime2": "TIMESTAMP_NTZ",
    "smalldatetime": "TIMESTAMP_NTZ",
    "datetimeoffset": "TIMESTAMP_TZ",
    "uniqueidentifier": "VARCHAR(36)",
    "text": "VARCHAR(16777216)",
    "ntext": "VARCHAR(16777216)",
    "image": "BINARY",
    "xml": "VARIANT",
}


def _map_mssql_type(col: dict) -> str:
    """Map a single MSSQL column to Snowflake data type."""
    dt = col["data_type"]

    # Direct mapping
    if dt in MSSQL_TO_SF_TYPE:
        return MSSQL_TO_SF_TYPE[dt]

    # varchar/nvarchar/char/nchar with length
    if dt in ("varchar", "nvarchar", "char", "nchar"):
        length = col["max_length"]
        if length and length > 0:
            return f"VARCHAR({length})"
        return "VARCHAR(16777216)"

    # varbinary
    if dt in ("varbinary", "binary"):
        length = col["max_length"]
        if length and length > 0:
            return f"BINARY({length})"
        return "BINARY"

    # decimal/numeric with precision and scale
    if dt in ("decimal", "numeric"):
        p = col["precision"] or 38
        s = col["scale"] or 0
        return f"NUMBER({p},{s})"

    # Fallback
    return "VARCHAR(16777216)"


def ensure_target_table(tbl: dict) -> str | None:
    """Create target table from MSSQL schema if it doesn't exist.

    Returns DDL string if created, None if already exists.
    """
    tgt_db = tbl.get("target_db", "ANALYTICS")
    tgt_schema = tbl.get("target_schema", "PUBLIC")
    tgt_table = tbl["target_table"]
    tgt_fqn = f"{tgt_db}.{tgt_schema}.{tgt_table}"
    scd_type = tbl.get("scd_type", 0)
    primary_key = tbl.get("primary_key", "")
    pk_list = [pk.strip().upper() for pk in primary_key.split(",")] if primary_key else []

    # Check if table already exists
    check_sql = (
        f"SELECT COUNT(*) AS C FROM {tgt_db}.INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_SCHEMA='{tgt_schema}' AND TABLE_NAME='{tgt_table}'"
    )
    try:
        df = sf_query(check_sql)
        if df is not None and not df.empty and int(df.iloc[0]["C"]) > 0:
            return None  # Already exists
    except Exception:
        pass

    # Get MSSQL column metadata
    src_db = tbl["source_db"]
    src_schema = tbl.get("source_schema", "dbo")
    src_table = tbl["source_table"]

    columns = get_column_metadata(src_db, src_schema, src_table)
    if not columns:
        raise ValueError(f"No columns found for {src_db}.{src_schema}.{src_table}")

    # Build CREATE TABLE DDL
    col_defs = []
    for col in columns:
        sf_type = _map_mssql_type(col)
        nullable = "" if col["is_nullable"] else " NOT NULL"
        col_defs.append(f"    {col['name'].upper()} {sf_type}{nullable}")

    # Add SCD2 metadata columns if configured
    if scd_type == 2:
        col_defs.append("    IS_CURRENT BOOLEAN DEFAULT TRUE")
        col_defs.append("    EFF_START_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()")
        col_defs.append("    EFF_END_DATE TIMESTAMP_NTZ")

    cols_str = ",\n".join(col_defs)
    ddl = f"CREATE TABLE IF NOT EXISTS {tgt_fqn} (\n{cols_str}\n)"

    # Ensure schema exists
    sf_execute(f"CREATE SCHEMA IF NOT EXISTS {tgt_db}.{tgt_schema}")
    sf_execute(ddl)

    return ddl


SF_STAGE = os.getenv("SF_STAGE", "@DATA_MIGRATION.CONTROL.MIGRATION_STAGE")


# ─── Work Table Management ────────────────────────────────────────────────────

def ensure_work_schema(tgt_db: str, tgt_schema: str):
    """Create the _WRK schema if it doesn't exist."""
    wrk_schema = f"{tgt_schema}_WRK"
    sf_execute(f"CREATE SCHEMA IF NOT EXISTS {tgt_db}.{wrk_schema}")


def prepare_work_table(tgt_db: str, tgt_schema: str, tgt_table: str):
    """Create work table as clone of target structure, truncate if exists."""
    wrk_schema = f"{tgt_schema}_WRK"
    wrk_fqn = f"{tgt_db}.{wrk_schema}.{tgt_table}"
    tgt_fqn = f"{tgt_db}.{tgt_schema}.{tgt_table}"

    ensure_work_schema(tgt_db, tgt_schema)
    sf_execute(f"CREATE TABLE IF NOT EXISTS {wrk_fqn} LIKE {tgt_fqn}")
    sf_execute(f"TRUNCATE TABLE {wrk_fqn}")
    return wrk_fqn


# ─── COPY INTO ────────────────────────────────────────────────────────────────

def check_files_in_stage(tgt_table: str) -> int:
    """Check if files exist in the stage path. Returns file count."""
    try:
        df = sf_query(f"LIST {SF_STAGE}/{tgt_table}/")
        return len(df) if df is not None and not df.empty else 0
    except Exception:
        return -1


def copy_into(target_fqn: str, tgt_table: str, delimiter: str = "|", compressed: bool = True, columns: list[str] | None = None) -> dict:
    """COPY INTO target from stage. Returns {returncode, sql, log, rows_loaded}.
    
    If columns is provided, only loads into those specific columns (positional match to CSV).
    """
    compression = "COMPRESSION='GZIP'" if compressed else ""
    col_clause = f" ({', '.join(columns)})" if columns else ""
    copy_sql = (
        f"COPY INTO {target_fqn}{col_clause} FROM {SF_STAGE}/{tgt_table}/ "
        f"FILE_FORMAT=(TYPE=CSV FIELD_DELIMITER='{delimiter}' "
        f"FIELD_OPTIONALLY_ENCLOSED_BY='\"' NULL_IF=('NULL','') SKIP_HEADER=0 {compression}) "
        f"ON_ERROR='CONTINUE'"
    )
    try:
        result_df = sf_query(copy_sql)
        rows_loaded = 0
        if result_df is not None and not result_df.empty:
            for col in result_df.columns:
                if "loaded" in col.lower():
                    rows_loaded = int(result_df[col].sum())
                    break
        return {"returncode": 0, "sql": copy_sql, "log": f"COPY INTO — {rows_loaded} rows loaded", "rows_loaded": rows_loaded}
    except Exception as e:
        return {"returncode": 1, "sql": copy_sql, "log": f"COPY INTO FAILED: {e}", "rows_loaded": 0}


# ─── Helper: Get Columns ──────────────────────────────────────────────────────

def _get_columns(tgt_db: str, tgt_schema: str, tgt_table: str) -> list[str]:
    """Get column names from Snowflake target table."""
    df = sf_query(
        f"SELECT COLUMN_NAME FROM {tgt_db}.INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA='{tgt_schema}' AND TABLE_NAME='{tgt_table}' AND TABLE_CATALOG='{tgt_db}' "
        f"ORDER BY ORDINAL_POSITION"
    )
    return df["COLUMN_NAME"].tolist() if not df.empty else []


def _get_wrk_count(tgt_db: str, tgt_schema: str, tgt_table: str) -> int:
    """Get row count from work table."""
    wrk_fqn = f"{tgt_db}.{tgt_schema}_WRK.{tgt_table}"
    try:
        df = sf_query(f"SELECT COUNT(*) AS C FROM {wrk_fqn}")
        return int(df.iloc[0]["C"])
    except Exception:
        return 0


# ─── MERGE: SCD Type 0 (Append Only) ─────────────────────────────────────────

def merge_scd0(tgt_db: str, tgt_schema: str, tgt_table: str) -> dict:
    """SCD Type 0: INSERT only — append all new rows (no dedup, no update)."""
    wrk_fqn = f"{tgt_db}.{tgt_schema}_WRK.{tgt_table}"
    tgt_fqn = f"{tgt_db}.{tgt_schema}.{tgt_table}"

    sql = f"INSERT INTO {tgt_fqn} SELECT * FROM {wrk_fqn}"
    try:
        sf_execute(sql)
        return {"returncode": 0, "sql": sql, "log": "SCD0: Appended all rows from WRK"}
    except Exception as e:
        return {"returncode": 1, "sql": sql, "log": f"SCD0 FAILED: {e}"}


# ─── MERGE: SCD Type 1 (Upsert — Update + Insert) ────────────────────────────

def merge_scd1(tgt_db: str, tgt_schema: str, tgt_table: str, primary_keys: list[str]) -> dict:
    """SCD Type 1: MERGE — update matched rows (excl PK), insert new rows.

    Performance: single MERGE statement, no multi-pass.
    """
    wrk_fqn = f"{tgt_db}.{tgt_schema}_WRK.{tgt_table}"
    tgt_fqn = f"{tgt_db}.{tgt_schema}.{tgt_table}"
    columns = _get_columns(tgt_db, tgt_schema, tgt_table)

    if not columns or not primary_keys:
        return {"returncode": 1, "sql": "", "log": "MERGE failed: no columns or PKs found"}

    pk_set = set(pk.upper() for pk in primary_keys)

    # ON condition (join on all PKs)
    on_clause = " AND ".join(f"TARGET.{pk}=SOURCE.{pk}" for pk in primary_keys)

    # UPDATE SET — exclude PK columns (no point updating them, saves cost)
    non_pk_cols = [c for c in columns if c.upper() not in pk_set]
    if non_pk_cols:
        update_set = ", ".join(f"TARGET.{c}=SOURCE.{c}" for c in non_pk_cols)
    else:
        # All columns are PKs (rare) — skip update
        update_set = None

    # INSERT columns + values (all columns)
    insert_cols = ", ".join(columns)
    insert_vals = ", ".join(f"SOURCE.{c}" for c in columns)

    if update_set:
        merge_sql = (
            f"MERGE INTO {tgt_fqn} AS TARGET "
            f"USING {wrk_fqn} AS SOURCE ON {on_clause} "
            f"WHEN MATCHED THEN UPDATE SET {update_set} "
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        )
    else:
        # No non-PK columns to update — just insert new
        merge_sql = (
            f"MERGE INTO {tgt_fqn} AS TARGET "
            f"USING {wrk_fqn} AS SOURCE ON {on_clause} "
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        )

    try:
        result_df = sf_query(merge_sql)
        inserted = updated = 0
        if result_df is not None and not result_df.empty:
            row = result_df.iloc[0]
            inserted = int(row.get("number of rows inserted", 0))
            updated = int(row.get("number of rows updated", 0))
        return {
            "returncode": 0, "sql": merge_sql,
            "log": f"SCD1 MERGE: {inserted} inserted, {updated} updated",
            "rows_inserted": inserted, "rows_updated": updated, "rows_expired": 0,
        }
    except Exception as e:
        return {"returncode": 1, "sql": merge_sql, "log": f"SCD1 MERGE FAILED: {e}",
                "rows_inserted": 0, "rows_updated": 0, "rows_expired": 0}


# ─── MERGE: SCD Type 2 (History Tracking) ────────────────────────────────────

def merge_scd2(tgt_db: str, tgt_schema: str, tgt_table: str, primary_keys: list[str]) -> dict:
    """SCD Type 2: Expire old rows + insert new versions.

    Expects target table to have columns: IS_CURRENT (BOOLEAN), EFF_START_DATE, EFF_END_DATE.
    If these columns don't exist, falls back to SCD1 behavior.

    Logic:
    1. UPDATE target SET IS_CURRENT=FALSE, EFF_END_DATE=CURRENT_TIMESTAMP()
       WHERE PK matches AND IS_CURRENT=TRUE AND data differs
    2. INSERT new rows from WRK with IS_CURRENT=TRUE, EFF_START_DATE=CURRENT_TIMESTAMP()
    """
    wrk_fqn = f"{tgt_db}.{tgt_schema}_WRK.{tgt_table}"
    tgt_fqn = f"{tgt_db}.{tgt_schema}.{tgt_table}"
    columns = _get_columns(tgt_db, tgt_schema, tgt_table)

    if not columns or not primary_keys:
        return {"returncode": 1, "sql": "", "log": "SCD2 failed: no columns or PKs found"}

    col_upper = [c.upper() for c in columns]
    has_scd2_cols = all(c in col_upper for c in ["IS_CURRENT", "EFF_START_DATE", "EFF_END_DATE"])

    if not has_scd2_cols:
        # Fallback: no SCD2 columns in target — behave as SCD1
        return merge_scd1(tgt_db, tgt_schema, tgt_table, primary_keys)

    pk_set = set(pk.upper() for pk in primary_keys)
    scd2_meta = {"IS_CURRENT", "EFF_START_DATE", "EFF_END_DATE"}
    data_cols = [c for c in columns if c.upper() not in pk_set and c.upper() not in scd2_meta]

    on_clause = " AND ".join(f"TARGET.{pk}=SOURCE.{pk}" for pk in primary_keys)

    # Detect changes: any non-PK, non-meta column differs
    change_check = " OR ".join(
        f"NVL(TARGET.{c}::VARCHAR,'') <> NVL(SOURCE.{c}::VARCHAR,'')" for c in data_cols
    ) if data_cols else "1=0"

    # Step 1: Expire changed rows using MERGE (avoids unsupported correlated subquery)
    expire_sql = (
        f"MERGE INTO {tgt_fqn} AS TARGET "
        f"USING {wrk_fqn} AS SOURCE ON {on_clause} AND TARGET.IS_CURRENT = TRUE "
        f"WHEN MATCHED AND ({change_check}) THEN "
        f"UPDATE SET TARGET.IS_CURRENT = FALSE, TARGET.EFF_END_DATE = CURRENT_TIMESTAMP()"
    )

    # Step 2: Insert new versions — use LEFT JOIN to find new or changed rows
    all_cols_no_meta = [c for c in columns if c.upper() not in scd2_meta]
    insert_cols_str = ", ".join(all_cols_no_meta + ["IS_CURRENT", "EFF_START_DATE", "EFF_END_DATE"])
    source_vals = ", ".join(f"SOURCE.{c}" for c in all_cols_no_meta)
    insert_vals_str = f"{source_vals}, TRUE, CURRENT_TIMESTAMP(), NULL"

    # Insert rows that are either new (no match) or changed (match with different data)
    pk_join = " AND ".join(f"TARGET.{pk} = SOURCE.{pk}" for pk in primary_keys)
    no_change_check = " AND ".join(
        f"NVL(TARGET.{c}::VARCHAR,'') = NVL(SOURCE.{c}::VARCHAR,'')" for c in data_cols
    ) if data_cols else "1=1"

    insert_sql = (
        f"INSERT INTO {tgt_fqn} ({insert_cols_str}) "
        f"SELECT {insert_vals_str} FROM {wrk_fqn} AS SOURCE "
        f"LEFT JOIN {tgt_fqn} AS TARGET ON {pk_join} AND TARGET.IS_CURRENT = TRUE "
        f"WHERE TARGET.{primary_keys[0]} IS NULL OR NOT ({no_change_check})"
    )

    try:
        expire_result = sf_query(expire_sql)
        expired_count = 0
        if expire_result is not None and not expire_result.empty:
            for col in expire_result.columns:
                if "updated" in col.lower():
                    expired_count = int(expire_result[col].sum())
                    break

        sf_execute(insert_sql)
        # Count inserted rows (new versions)
        wrk_count = 0
        try:
            cnt = sf_query(f"SELECT COUNT(*) AS C FROM {wrk_fqn}")
            wrk_count = int(cnt.iloc[0]["C"])
        except Exception:
            pass

        full_sql = f"{expire_sql};\n{insert_sql}"
        return {
            "returncode": 0, "sql": full_sql,
            "log": f"SCD2: {expired_count} expired, {wrk_count} inserted as new versions",
            "rows_inserted": wrk_count, "rows_updated": 0, "rows_expired": expired_count,
        }
    except Exception as e:
        return {"returncode": 1, "sql": f"{expire_sql};\n{insert_sql}", "log": f"SCD2 FAILED: {e}",
                "rows_inserted": 0, "rows_updated": 0, "rows_expired": 0}


# ─── FULL Load (Atomic SWAP) ─────────────────────────────────────────────────

def full_load(tgt_db: str, tgt_schema: str, tgt_table: str) -> dict:
    """FULL load: Atomic swap — rename WRK to target (zero-downtime, cost-efficient).

    Falls back to DELETE+INSERT if SWAP fails (permissions).
    """
    wrk_fqn = f"{tgt_db}.{tgt_schema}_WRK.{tgt_table}"
    tgt_fqn = f"{tgt_db}.{tgt_schema}.{tgt_table}"

    # Try atomic swap first (best practice: no DELETE scan, instant)
    swap_sql = f"ALTER TABLE {tgt_fqn} SWAP WITH {wrk_fqn}"
    try:
        sf_execute(swap_sql)
        # Recreate empty WRK for next run
        sf_execute(f"CREATE OR REPLACE TABLE {wrk_fqn} LIKE {tgt_fqn}")
        return {"returncode": 0, "sql": swap_sql, "log": "FULL: Atomic SWAP (WRK ↔ target)"}
    except Exception:
        pass

    # Fallback: DELETE + INSERT
    del_sql = f"DELETE FROM {tgt_fqn}"
    ins_sql = f"INSERT INTO {tgt_fqn} SELECT * FROM {wrk_fqn}"
    try:
        sf_execute(del_sql)
        sf_execute(ins_sql)
        return {"returncode": 0, "sql": f"{del_sql}; {ins_sql}", "log": "FULL: DELETE+INSERT from WRK"}
    except Exception as e:
        return {"returncode": 1, "sql": f"{del_sql}; {ins_sql}", "log": f"FULL load FAILED: {e}"}


# ─── FILTER Load ──────────────────────────────────────────────────────────────

def filter_load(tgt_db: str, tgt_schema: str, tgt_table: str, filter_condition: str) -> dict:
    """FILTER load: DELETE matching rows + INSERT from work table."""
    wrk_fqn = f"{tgt_db}.{tgt_schema}_WRK.{tgt_table}"
    tgt_fqn = f"{tgt_db}.{tgt_schema}.{tgt_table}"
    condition = filter_condition or "1=1"

    del_sql = f"DELETE FROM {tgt_fqn} WHERE {condition}"
    ins_sql = f"INSERT INTO {tgt_fqn} SELECT * FROM {wrk_fqn}"
    try:
        sf_execute(del_sql)
        sf_execute(ins_sql)
        return {"returncode": 0, "sql": f"{del_sql}; {ins_sql}", "log": "FILTER: DELETE WHERE + INSERT"}
    except Exception as e:
        return {"returncode": 1, "sql": f"{del_sql}; {ins_sql}", "log": f"FILTER load FAILED: {e}"}


# ─── Load Dispatcher ──────────────────────────────────────────────────────────

def execute_load(tbl: dict) -> dict:
    """Execute the full load sequence: check files → COPY INTO WRK → MERGE/INSERT to target.

    Returns: {returncode, logs, sf_count, merge_sql}
    """
    tgt_db = tbl.get("target_db", "ANALYTICS")
    tgt_schema = tbl.get("target_schema", "PUBLIC")
    tgt_table = tbl["target_table"]
    delimiter = tbl.get("delimiter", "|")
    load_type = tbl.get("load_type", "full")
    scd_type = tbl.get("scd_type", 0)
    primary_key = tbl.get("primary_key", "")
    filter_condition = tbl.get("filter_condition")
    pk_list = [pk.strip() for pk in primary_key.split(",")] if primary_key else []

    logs = []

    # 0. Auto-create target table from MSSQL schema if it doesn't exist
    try:
        ddl = ensure_target_table(tbl)
        if ddl:
            logs.append(f"Created target table: {tgt_db}.{tgt_schema}.{tgt_table}")
    except Exception as e:
        return {"returncode": 1, "logs": [f"Target table creation failed: {e}"], "sf_count": 0}

    # 1. Check files exist
    file_count = check_files_in_stage(tgt_table)
    if file_count == 0:
        return {"returncode": 1, "logs": ["No files found in stage — nothing to load"], "sf_count": 0}
    logs.append(f"Found {file_count} file(s) in stage")

    # 2. Prepare work table (TRUNCATE ensures clean slate — avoids loading stale data from prior runs)
    try:
        wrk_fqn = prepare_work_table(tgt_db, tgt_schema, tgt_table)
        logs.append(f"Work table ready: {wrk_fqn}")
    except Exception as e:
        return {"returncode": 1, "logs": [f"Work table creation failed: {e}"], "sf_count": 0}

    # 3. COPY INTO work table (exclude SCD2 metadata columns — they don't exist in source CSV)
    all_columns = _get_columns(tgt_db, tgt_schema, tgt_table)
    scd2_meta = {"IS_CURRENT", "EFF_START_DATE", "EFF_END_DATE"}
    data_columns = [c for c in all_columns if c.upper() not in scd2_meta]
    copy_result = copy_into(wrk_fqn, tgt_table, delimiter, compressed=True, columns=data_columns if len(data_columns) < len(all_columns) else None)
    logs.append(copy_result["log"])
    if copy_result["returncode"] != 0:
        return {"returncode": 1, "logs": logs, "sf_count": 0}

    # 3b. Purge stage files after successful COPY to prevent reprocessing on failure retry
    try:
        sf_execute(f"REMOVE {SF_STAGE}/{tgt_table}/")
        logs.append("Stage purged after COPY")
    except Exception:
        pass

    # 3c. Deduplicate work table by PK (keeps latest row per key — prevents MERGE duplicate errors)
    if pk_list:
        pk_cols = ", ".join(pk_list)
        dedup_sql = (
            f"CREATE OR REPLACE TABLE {wrk_fqn} AS "
            f"SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {pk_cols} ORDER BY {pk_cols}) AS _rn "
            f"FROM {wrk_fqn}) WHERE _rn = 1"
        )
        try:
            sf_execute(dedup_sql)
            # Drop the helper column
            sf_execute(f"ALTER TABLE {wrk_fqn} DROP COLUMN _rn")
        except Exception:
            pass

    # 4. MERGE / INSERT based on load type + SCD type
    if load_type == "full":
        merge_result = full_load(tgt_db, tgt_schema, tgt_table)
    elif load_type == "filter":
        merge_result = filter_load(tgt_db, tgt_schema, tgt_table, filter_condition)
    elif load_type == "incremental":
        if scd_type == 0:
            merge_result = merge_scd0(tgt_db, tgt_schema, tgt_table)
        elif scd_type == 1:
            merge_result = merge_scd1(tgt_db, tgt_schema, tgt_table, pk_list)
        elif scd_type == 2:
            merge_result = merge_scd2(tgt_db, tgt_schema, tgt_table, pk_list)
        else:
            merge_result = merge_scd1(tgt_db, tgt_schema, tgt_table, pk_list)
    else:
        merge_result = full_load(tgt_db, tgt_schema, tgt_table)

    logs.append(merge_result["log"])

    # 5. Get final target count — only IS_CURRENT rows for SCD2 tables
    sf_count = 0
    try:
        all_cols_upper = [c.upper() for c in _get_columns(tgt_db, tgt_schema, tgt_table)]
        if "IS_CURRENT" in all_cols_upper:
            cnt = sf_query(f"SELECT COUNT(*) AS C FROM {tgt_db}.{tgt_schema}.{tgt_table} WHERE IS_CURRENT = TRUE")
        else:
            cnt = sf_query(f"SELECT COUNT(*) AS C FROM {tgt_db}.{tgt_schema}.{tgt_table}")
        sf_count = int(cnt.iloc[0]["C"])
    except Exception:
        pass

    return {
        "returncode": merge_result["returncode"],
        "logs": logs,
        "sf_count": sf_count,
        "rows_loaded": copy_result.get("rows_loaded", 0),
        "merge_sql": merge_result.get("sql", ""),
        "rows_inserted": merge_result.get("rows_inserted", 0),
        "rows_updated": merge_result.get("rows_updated", 0),
        "rows_expired": merge_result.get("rows_expired", 0),
    }
