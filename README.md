# AeroSurf – BlueGuard Case Study

Incremental data pipeline on **Databricks** using **PySpark, Spark Structured Streaming, Auto Loader and Delta Lake**.

> 📄 For the business problem, requirements and the Gold layer design, see [`AeroSurf_BlueGuard_Case_Study.pdf`](./AeroSurf_BlueGuard_Case_Study.pdf).

## What's Implemented
```
JSON inspection files (UC Volume)
        │  Auto Loader (cloudFiles)
        ▼
Bronze: databricksbootcamp.aerosurf.raw_inspections
        │  Dedup + SCD Type 2 (MERGE)
        ▼
Silver: databricksbootcamp.aerosurf.asset_status
```

| Notebook | Purpose |
|---|---|
| `AeroSurf_Bronze.ipynb` | Incremental ingestion of raw inspection JSON into Bronze |
| `AeroSurf_Silver.ipynb` | Builds an SCD Type 2 asset status table from Bronze |

### Bronze – `AeroSurf_Bronze.ipynb`
- Reads JSON files from a Unity Catalog Volume using **Auto Loader** (`cloudFiles`)
- `cloudFiles.schemaLocation` for schema tracking, `cloudFiles.inferColumnTypes = true`
- `cloudFiles.schemaEvolutionMode = addNewColumns` so new source columns don't break ingestion
- Adds an audit column `bronze_ingested_at` (`current_timestamp()`)
- Writes to a Delta table with `checkpointLocation`, `mergeSchema = true` and `trigger(availableNow=True)` (incremental batch style)

### Silver – `AeroSurf_Silver.ipynb`
- **Table design:** `asset_status` with identity surrogate key (`asset_sk`), business columns (`asset_id`, `beach`, `category`, `condition`, `hire_status`, `inspected_ts`, `defect_notes`) and SCD2 columns (`start_ts`, `end_ts`, `is_current`)
- **Change Data Feed** enabled on the table (`delta.enableChangeDataFeed = true`)
- **Schema drift handling:** checks whether `defect_notes` has arrived and, if missing, adds it to Bronze and Silver via `ALTER TABLE ... ADD COLUMNS`
- **Dedup:** `ROW_NUMBER()` per `asset_id`, ordered by `inspected_ts` then `bronze_ingested_at`, keeping the latest record
- **Change detection:** compares the incoming row with the current Silver row using null-safe equality (`<=>`) across all tracked columns
- **SCD Type 2 via MERGE:** a staged view emits two rows for a changed asset, an `EXPIRE` row (matched on `asset_id`) and an `INSERT` row (`merge_key = NULL`, so it never matches). A single `MERGE` then closes the old version (`is_current = FALSE`, `end_ts` set) and inserts the new one
- **Validation queries:** full history, current records only, and a check for duplicate current rows per asset

---

## Learnings

### 1. SCD (Slowly Changing Dimensions)
- **Type 1** overwrites old values (no history). **Type 2** keeps history using `start_ts`, `end_ts`, `is_current`.
- In a single MERGE, one source row can't both update the old record and insert the new one. The fix is staging two rows per changed key: one with the real key (expires old), one with a `NULL` merge key (always inserts).
- Null-safe comparison (`<=>`) matters for change detection, otherwise `NULL != NULL` causes false changes or missed changes.
- Always dedupe the source to the latest record per key first, or one batch can create multiple "current" rows. A duplicate-current check query is a cheap safety net.

### 2. CDF (Change Data Feed)
- CDF records row-level changes (insert, update pre/post image, delete) so downstream tables can process only what changed.
- Enabled on the Silver table here. Reading it (`readChangeFeed`) to feed Gold would be the natural next step (see PDF), but that isn't implemented.
- It only captures changes made after it is enabled.

### 3. Schema Evolution vs Schema Enforcement
- **Enforcement:** Delta rejects writes that don't match the table schema (default behavior).
- **Evolution:** the schema is allowed to change, opt-in via `mergeSchema` or Auto Loader's `addNewColumns`.
- In this project, Bronze is permissive (evolution on) while Silver is controlled: new columns are added explicitly with `ALTER TABLE` rather than letting any drift through.
- Evolution handles new columns well, but renames and type changes still need explicit handling.

### 4. Auto Loader
- Incremental file ingestion that tracks already-processed files, so reruns only pick up new files.
- `schemaLocation` stores inferred schema versions and `checkpointLocation` stores stream progress. They're different things.
- `availableNow` trigger gives batch-style runs with streaming guarantees.

### 5. Watermark vs Checkpoint
- **Checkpoint:** persists stream progress and state so a query can resume after failure without reprocessing. Used in the Bronze write here.
- **Watermark:** sets how late event-time data may arrive so Spark can drop old state in windowed aggregations, streaming dedup and stream-stream joins.
- Rule of thumb: checkpoint = where to resume, watermark = what late data/state to drop. Don't delete a checkpoint casually.
