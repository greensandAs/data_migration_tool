# MSSQL → Snowflake Migration App — Demo Video Plan

> **Format:** No audio — use text overlays/captions throughout
> **Target Length:** 8-12 minutes (after editing)
> **Resolution:** 1080p MP4, 30fps

---

## Recording Tools

| Platform | Tool | Notes |
|----------|------|-------|
| Windows | OBS Studio (free) | Best quality, most control |
| Windows | Xbox Game Bar (Win+G) | Quick, built-in |
| Mac | QuickTime Player | Built-in, simple |
| Any | Loom (free tier) | Auto-generates captions |
| Any | ShareX | Lightweight, GIF support |

---

## Video Editing & Cutting Tools

| Tool | Best For | Cost |
|------|----------|------|
| **CapCut Desktop** | Text overlays, speed up, drag-and-drop | Free |
| **DaVinci Resolve** | Pro editing, color, effects | Free |
| **Shotcut** | Simple cuts and trims | Free |
| **Canva Video Editor** | Quick text + transitions | Free tier |

### How to Edit (CapCut — Recommended)
1. Import your screen recording
2. **Cut dead time:** Split clip (Ctrl+B) at boring waits → Delete segment
3. **Speed up waits:** Select BCP/COPY INTO segments → Right-click → Speed → 2x-4x
4. **Add text overlays:** Text tool → Add caption explaining current step
5. **Transitions:** Use simple fade between sections (no flashy effects)
6. **Title cards:** Add black slides with white text between major sections
7. **Export:** 1080p, H.264, 30fps

### How to Edit (DaVinci Resolve — Pro)
1. Import to Media Pool → Drag to Timeline
2. **Edit page:** Blade tool (B) to cut, delete dead sections
3. **Speed:** Right-click clip → Change Clip Speed → 200-400%
4. **Text:** Effects Library → Titles → Text+ → Drag to timeline above video
5. **Fusion page:** Advanced text with background boxes
6. **Deliver page:** H.264, 1080p, YouTube preset

---

## Demo Tips (No Audio)

- Add **text overlay captions** at every step explaining what's happening
- Use **zoom/crop** on important UI elements (mode selector, SCD type, logs)
- **Pause 2-3 seconds** on key results so viewer can read
- Use **transition slides** (black screen + white text) between sections
- **Mouse movements** should be deliberate and slow — no random cursor movement
- **Highlight clicks** with a subtle circle animation (CapCut has this built-in)

---

## Demo Script — 6 Sections

---

### Section 1: Configuration (2-3 min)

**Caption:** "Step 1: Configure tables for migration"

| Step | Action | Caption |
|------|--------|---------|
| 1 | Open Streamlit app | "MSSQL → Snowflake Migration Console" |
| 2 | Show Configuration tab | "KPI metrics: Total Tables, Active, Inactive" |
| 3 | Click "Discover Tables from MSSQL" | "Auto-discovery scans MSSQL metadata" |
| 4 | Enter TestDB / dbo → Click Scan | "Detecting PKs, watermark columns, data types" |
| 5 | Show discovered tables | "3 tables found with auto-detected settings" |
| 6 | Click "Add 3 tables" | "Tables added to migration config" |
| 7 | Edit Customers → SCD: 2, Load: incremental | "SCD Type 2 = preserve full change history" |
| 8 | Edit Products → SCD: 2, Load: incremental | "Products also tracked with SCD2" |
| 9 | Keep Orders → SCD: 1, Load: incremental | "SCD Type 1 = simple overwrite (no history)" |
| 10 | Show final config grid | "All settings visible: PK, CDC column, SCD type, mode" |

---

### Section 2: FULL Mode Run (2-3 min)

**Caption:** "Step 2: Run FULL pipeline — Export + Load + Merge"

| Step | Action | Caption |
|------|--------|---------|
| 1 | Switch to Run tab | "Execution modes: FULL, EXPORT, LOAD" |
| 2 | Select all 3 tables | "Running all tables in parallel" |
| 3 | Set Mode: FULL | "FULL = BCP → GZip → Blob → COPY INTO → MERGE" |
| 4 | Click Start Migration | "Pipeline started..." |
| 5 | Show progress bar | *(speed up 2-4x during wait)* |
| 6 | Show completion status | "All 3 tables migrated successfully" |
| 7 | Expand log for Customers | "BCP: 100K rows → 4 chunks → GZip → Upload → COPY → MERGE" |
| 8 | Show Results tab | "LOG_TABLE: batch history with row counts & duration" |

**Title Card:** "FULL mode: 1.1M rows extracted, compressed, uploaded, and merged"

---

### Section 3: EXPORT Mode (1-2 min)

**Caption:** "Step 3: EXPORT mode — Extract & stage only (no Snowflake load)"

| Step | Action | Caption |
|------|--------|---------|
| 1 | Set Mode: EXPORT | "EXPORT = BCP + GZip + Upload only" |
| 2 | Click Start | "No COPY INTO or MERGE — files staged to Blob" |
| 3 | Show completion (faster) | "Completed faster — no Snowflake operations" |
| 4 | Expand log | "Only BCP + Upload steps visible (no COPY/MERGE)" |

**Title Card:** "Export-only mode: useful for staging data before loading"

---

### Section 4: LOAD Mode (1-2 min)

**Caption:** "Step 4: LOAD mode — Load from stage only (no re-extraction)"

| Step | Action | Caption |
|------|--------|---------|
| 1 | Set Mode: LOAD | "LOAD = COPY INTO + MERGE (uses staged files)" |
| 2 | Click Start | "Skips BCP entirely — reads from Azure Blob" |
| 3 | Show completion | "Only COPY INTO + MERGE steps executed" |
| 4 | Expand log | "No BCP rows — starts directly from COPY INTO" |

**Title Card:** "Load-only mode: re-process staged files without re-extracting from MSSQL"

---

### Section 5: Incremental + SCD2 (2-3 min)

**Caption:** "Step 5: Incremental changes with SCD Type 2 history tracking"

| Step | Action | Caption |
|------|--------|---------|
| 1 | Show SSMS/Azure Data Studio | "Running incremental changes on MSSQL..." |
| 2 | Run Cell 3 from mssql.ipynb | "50 customer updates + 10 inserts + 10 product price changes + 100 new orders" |
| 3 | Switch to Streamlit app | "Now running incremental migration..." |
| 4 | Select Customers + Products (SCD2) | "These tables use SCD Type 2" |
| 5 | Mode: FULL, Click Start | "Extracts only CHANGED rows (watermark-based CDC)" |
| 6 | Show log: only ~60 rows extracted | "Incremental: only 60 rows vs 100K full table" |
| 7 | Show "SCD2 MERGE" in log | "SCD2: Expired old rows + inserted new versions" |
| 8 | Run Snowflake verification query | "CustomerID=1: old version expired, new version active" |
| 9 | Show IS_CURRENT, EFF_START_DATE, EFF_END_DATE | "Full audit trail preserved" |
| 10 | Show current vs historical count | "100,060 total: 100,010 current + 50 historical" |

**Title Card:** "SCD Type 2: old versions expired (IS_CURRENT=FALSE), new versions inserted with timestamps"

---

### Section 6: Results Dashboard (30 sec)

**Caption:** "Step 6: Migration history and monitoring"

| Step | Action | Caption |
|------|--------|---------|
| 1 | Show Results tab | "Complete batch history" |
| 2 | Show KPIs | "Total Jobs, Successful, Failed, Avg Duration" |
| 3 | Highlight EXECUTION_MODE column | "All 3 modes tracked: FULL, EXPORT, LOAD" |
| 4 | Highlight LOAD_TYPE column | "Incremental vs Full load visible" |

---

## Final Title Card

```
MSSQL → Snowflake Migration Console
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BCP → GZip → Azure Blob → COPY INTO → MERGE
Supports: SCD Type 0 / 1 / 2
Modes: FULL | EXPORT | LOAD
Incremental CDC: Timestamp & Row-Version

Tiger Analytics
```

---

## Pre-Demo Checklist

| # | Step | Done |
|---|------|------|
| 1 | Azure SQL Server running with TestDB | ☐ |
| 2 | Run Cell 1 (Initial Data — 1.1M rows) on MSSQL | ☐ |
| 3 | Run Cell 2 (Snowflake target tables) | ☐ |
| 4 | Update migration_config.json (Cell 5 or use app UI) | ☐ |
| 5 | Azure Blob container accessible | ☐ |
| 6 | Streamlit app running | ☐ |
| 7 | Screen recorder configured (1080p, no mic) | ☐ |
| 8 | Record Section 1-4 (Config + 3 modes) | ☐ |
| 9 | Run Cell 3 (Incremental changes on MSSQL) | ☐ |
| 10 | Record Section 5 (Incremental + SCD2) | ☐ |
| 11 | Record Section 6 (Results) | ☐ |
| 12 | Edit video: cut dead time, add captions, speed up waits | ☐ |
| 13 | Export final MP4 (1080p, 8-12 min) | ☐ |

---

## Quick Reference: SCD Types

| SCD Type | Behavior | Target Columns | MERGE Logic |
|----------|----------|----------------|-------------|
| **0** | Append only (no updates) | Standard columns | INSERT only |
| **1** | Overwrite (no history) | Standard columns | UPDATE matched, INSERT new |
| **2** | Full history | + IS_CURRENT, EFF_START_DATE, EFF_END_DATE | Expire old (IS_CURRENT=FALSE, set EFF_END_DATE), INSERT new version |

## Quick Reference: Execution Modes

| Mode | Steps | Use Case |
|------|-------|----------|
| **FULL** | BCP → GZip → Upload → COPY INTO → MERGE | End-to-end migration |
| **EXPORT** | BCP → GZip → Upload | Stage files without loading |
| **LOAD** | COPY INTO → MERGE | Re-load from already-staged files |
