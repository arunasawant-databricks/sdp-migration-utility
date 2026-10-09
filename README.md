# SDP Migration Utility

A Databricks Asset Bundle that backfills a **source region's** historical bronze
data (exposed via **Delta Share**) into the **target region's** pipeline-owned
bronze streaming tables.

For each table it runs a safe, resumable sequence:

1. **DELETE** rows in the target up to a `cut_off_date` (containment)
2. **INSERT** the Delta-shared rows up to the same `cut_off_date`
3. **VERIFY** row-count parity

Progress and state are tracked in a control table, so runs are **resumable** and
tables are migrated **in parallel** with bounded concurrency.

## Folder layout

```
sdp_migration_utility/
├── databricks.yml                 # Bundle definition: variables + target workspace
├── resources/
│   ├── sdp_migration_job.yml          # Serverless job (default)
│   └── sdp_migration_job_classic.yml  # Classic-compute variant (instance pools)
└── src/
    ├── config/
    │   └── tables.csv                  # Per-table list: table_name, cut_off_date, checkpoint_col, ... (edit this)
    ├── lib/                            # Utility library (config, control table, migration, ...)
    └── notebooks/
        ├── 00_setup.py                 # Load config, ensure control table, validate, register
        ├── 10_migrate_table.py         # Per-table DELETE -> INSERT -> verify worker
        └── 99_report.py                # Summarize, alert, release run lock
```

## Prerequisites

- Databricks CLI (bundles) authenticated to the target workspace.
- A Delta Share from the source region exposing its bronze streaming tables.
- The target bronze streaming tables already created by their pipelines.

## Configure

This deployment serves **one Business Unit**. There are only two files to edit:

1. **`databricks.yml`** — the single config surface. Replace the placeholders:
   - `target` workspace `host` → your workspace URL
   - `shared_catalog` / `shared_schema` → the Delta-share read path
   - `target_catalog` / `target_schema` → where the backfill writes
   - `control_catalog` / `control_schema` / `control_table` → the control table
   - `business_unit` → the BU this deployment migrates
   - `timezone` (default `Asia/Kolkata`), `tables_csv` (default `tables.csv`), `max_parallel_tables`

   Every value is passed to the job as a parameter and consumed at run time.

2. **`src/config/tables.csv`** — one row per table to migrate (replace the example rows).
   The only file besides `databricks.yml` you edit.
   - **Mandatory** (must be filled for every row): `table_name`, `cut_off_date`, `checkpoint_col`.
   - **Optional** — leave blank to use the `databricks.yml` values: `shared_catalog`, `shared_schema`, `target_catalog`, `target_schema`, `partition_col`.
   - **Optional** — leave blank for the built-in default: `chunk_backfill` (blank = `off`; `auto` = partition-at-a-time, resumable), `backfill_days` (blank = copy all history ≤ cut_off; else last N days only).

## Deploy & run

**Serverless (default):**

```bash
databricks bundle deploy -t target --profile <your-profile>
databricks bundle run sdp_migration_job -t target --profile <your-profile>
```

**Classic compute (instance pools):** supply a real pool id at deploy time. The job
cluster uses **Standard access mode (`USER_ISOLATION`)**. Do not switch it to
`SINGLE_USER` (Dedicated): reading Delta-Shared streaming tables on dedicated compute
goes through serverless data filtering, which fails on private-only networking.

```bash
databricks bundle deploy -t target --profile <your-profile> \
  --var="instance_pool_id=<pool-id>" --var="driver_instance_pool_id=<pool-id>"
databricks bundle run sdp_migration_job_classic -t target --profile <your-profile>
```

## Job flow

`setup` → `migrate` (For each table, bounded concurrency) → `report` (always runs).
Only one run executes at a time (run-level guard + control-table run lock), so
overlapping destructive DML is prevented.
