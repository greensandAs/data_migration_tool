# MySQL → Snowflake Historical Load — User Guide

A 1:1 "lift-and-shift" pipeline: every MySQL table is replicated into a single
Snowflake layer with the **same columns and types**. No SILVER/SCD2 transforms.

---

## 1. What it does

| MySQL | → | Snowflake |
|-------|---|-----------|
| `mtest.huge_decimals` | → | `MTEST.RAW.HUGE_DECIMALS` |
| `employees.salaries`  | → | `EMPLOYEES.RAW.SALARIES` |

- **Database** = the MySQL schema name (uppercased).
- **Schema**   = literally `RAW`.
- **Table**    = the source table name (uppercased), with **business columns 1:1**
  plus a few audit columns (`_LOAD_TS`, `_SRC_FILE`, `_BATCH_ID`, `_IS_DELETED`,
  `_DELETED_AT`).
- **Control objects** (internal stage, file formats, `RUN_LOG`) live in
  `HISTLOAD_DB.META`.

---

## 2. One-time setup

1. **Credentials** — copy `.env.example` → `.env` and fill in `MYSQL_*` and `SF_*`.
   Credentials live **only** in the environment, never in the config file.
2. **Snowflake objects** — run `setup.sql` once (creates `HISTLOAD_DB.META`:
   stage, file formats, `RUN_LOG`, `V_RUN_LOG`). Re-running is safe (idempotent).
3. **Build the config** — generate from a MySQL schema:
   ```
   python config_generator.py <mysql_schema>
   ```
   or add tables in the app's **⚙️ Config** tab.

Requirements: `pip install -r requirements.txt`, plus **MySQL Shell (`mysqlsh`)**
on PATH for full loads.

---

## 3. Load modes

| Mode | Engine | When | How it loads |
|------|--------|------|--------------|
| **Full** | `mysqlsh dumpTables` | first run, or `load_type: "full"` | `TRUNCATE + COPY` (or atomic SWAP) |
| **Incremental** | `connectorx` | `load_type: "incremental"` after first load | `COPY → temp → MERGE` on the key |

The **first run of any table is always a full backfill**; subsequent runs go
incremental if `load_type` is `incremental`.

### Watermark types (incremental)

| `watermark_type` | Cursor column | Captures | Notes |
|------------------|---------------|----------|-------|
| `time` | a timestamp column (e.g. `UPDATED_AT`) | inserts **and** updates | the column must move on every update (`ON UPDATE CURRENT_TIMESTAMP`) |
| `id`   | a monotonic integer PK (e.g. `ID`)      | **inserts only**          | updates don't change the PK, so they're not re-pulled |
| `auto` (default) | — | — | detects: integer column → `id`, else `time` |

Window: `WHERE wm > <last> AND wm <= <ceiling>`
- `time`: ceiling = source `NOW() - 5min` (lag skips in-flight rows).
- `id`:   ceiling = current `MAX(pk)`.

### Where the cursor is stored
- `time` mode → `last_loaded_at` (timestamp cursor).
- `id` mode   → `last_loaded_key` (numeric PK cursor); `last_loaded_at` becomes a
  *tracking* timestamp of the last run.

Snowflake `MAX(watermark_col)` from RAW is the source of truth; the config value
is just the cached kickoff for the next run.

---

## 4. Config reference (`histload_config.json`)

```json
{
  "source_db": "mtest",
  "source_table": "huge_decimals",
  "target_table": "HUGE_DECIMALS",
  "primary_key": "MASSIVE_PK",
  "merge_keys": ["EMP_NO", "FROM_DATE"],   // optional composite key
  "load_type": "incremental",               // full | incremental
  "watermark_col": "UPDATED_AT",
  "watermark_type": "time",                 // time | id | (omit = auto)
  "last_loaded_at": "2026-06-09 02:10:00",
  "last_loaded_key": "100450",              // id mode only
  "partition_col": "ID",                     // integer col for parallel reads
  "partition_num": 8,
  "reconcile": false,                        // soft-delete missing keys
  "atomic_full": false,                      // SWAP-based full reload (no gap)
  "rows_per_file": 1000000,
  "active": true
}
```

| Field | Purpose |
|-------|---------|
| `primary_key` / `merge_keys` | MERGE/dedupe key. Use `merge_keys` for composite grain. |
| `load_type` | `full` (replace) or `incremental` (delta MERGE). |
| `watermark_col` / `watermark_type` | incremental cursor + how to interpret it. |
| `partition_col` / `partition_num` | parallel read; only used when `partition_col` is an **integer** column. |
| `reconcile` | enable delete reconciliation for this table. |
| `atomic_full` | full reload via side table + `SWAP` (no empty window for readers). |
| `rows_per_file` | split large deltas into N-row parquet files for parallel COPY. |

---

## 5. Special handling (built in)

- **Big decimals** — `DECIMAL` with precision > 38 (Snowflake's max) is stored as
  `VARCHAR` (lossless) and read as text, so PKs/values are never truncated.
- **Timezones** — both extractors keep `TIMESTAMP` values in the **source session
  timezone** (`mysqlsh tzUtc:false`, connectorx `DATE_FORMAT`), so the watermark
  loop never drifts by a UTC offset.
- **Composite keys** — `merge_keys` drives the MERGE `ON` and dedupe so multi-column
  grains (e.g. `emp_no, from_date`) don't collapse or error.
- **Schema drift** — new MySQL columns are auto-added to the RAW table; dropped
  columns are warned (data preserved).

---

## 6. Reconcile (deletes)

Incremental loads never see deletes. `--reconcile` (or `"reconcile": true`)
diffs the key set MySQL vs RAW and **soft-deletes** rows gone from the source
(`_IS_DELETED=TRUE`, `_DELETED_AT=now`). Run it on its own cadence — it scans the
full key set. (The Reconcile button in the app is disabled by default; use the CLI.)

---

## 7. Validate

`--validate` compares **source vs RAW live rows**:
1. **Row count** parity.
2. **Watermark** parity (`MAX` on both sides).
3. **Deep hash** (`--validate --deep`, or the *Deep* toggle) — an order-independent
   row-hash fingerprint; catches content drift counts miss. Can false-alarm on
   `FLOAT`/exotic types.

---

## 8. Running

**App** (`streamlit run app.py`):
- **📊 Dashboard** — per-table status cards.
- **▶️ Run** — Run Load / single-table; live streaming log.
- **⚙️ Config** — Generate from schema, ➕ Add table (popup), per-table editor.
- **📜 History** — `V_RUN_LOG` with error detail.
- **🔢 Counts** — RAW counts + Validate vs MySQL (with Deep toggle).

**CLI** (`python orchestrator.py ...`):
| Command | Action |
|---------|--------|
| `(no args)` | full/incremental per active table |
| `--full [--table T]` | force a full reload |
| `--reconcile [--table T]` | soft-delete missing keys |
| `--validate [--deep] [--table T]` | parity check |
| `--table T` | limit to one source table |

---

## 9. Monitoring (`HISTLOAD_DB.META.RUN_LOG`)

One row per table per run. Key columns: `LOAD_TYPE`, `WATERMARK_FROM/TO`,
`WATERMARK_TYPE` (time/id), `ROWS_EXTRACTED`, `ROWS_LOADED`, `STATUS`,
`FAILED_STEP`, `ERROR_MESSAGE`, `DURATION_SEC`. See `monitoring.sql` for
ready-made health queries (failed runs, stale tables, error frequency).

---

## 10. Troubleshooting

| Symptom | Likely cause / fix |
|---------|--------------------|
| Table always full-loads | no `watermark_col` → no delta possible (expected). Add a watermark or use `id` mode with a PK. |
| Updates not captured | `id` mode captures inserts only; switch to a `time` watermark that moves on update. |
| Watermark looks shifted by hours | timezone mismatch — ensure you're on the latest extractors (`tzUtc:false` / `DATE_FORMAT`) and reload once. |
| `Partition can only be done on int columns` | `partition_col` isn't integer — it auto-falls back to single-threaded read; set `partition_num: 1` to silence. |
| `Duplicate row detected during DML` | composite grain — set `merge_keys` to the full uniqueness key. |
| Deletes not reflected | run `--reconcile` (and set `reconcile: true`). |
