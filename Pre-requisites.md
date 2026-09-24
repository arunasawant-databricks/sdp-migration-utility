# Pre-requisites

Complete these three steps before running the migration job.

## 1. Deploy the target (R2) pipelines

The target bronze streaming tables must already exist in R2. Deploy and run
your R2 pipelines first so the target tables are created and owned by them.
The migration job backfills into these tables — it does not create them.

## 2. Share R1's tables to R2 over Delta Share

**On R1 (source / provider)** — create a share, add the bronze tables, and grant
it to the R2 recipient:

```sql
CREATE SHARE IF NOT EXISTS sdp_bronze_share;

-- Repeat for each table you plan to migrate
ALTER SHARE sdp_bronze_share ADD TABLE sdp_bronze.<table_name>;

GRANT SELECT ON SHARE sdp_bronze_share TO RECIPIENT <r2_recipient>;
```

**On R2 (target / recipient)** — mount the share as a catalog:

```sql
CREATE CATALOG IF NOT EXISTS sdp_shared_from_source
  USING SHARE <provider_name>.sdp_bronze_share;
```

The catalog name here (`sdp_shared_from_source`) and its schema must match the
`shared_catalog` / `shared_schema` values in the config (step 3).

## 3. Update the config files

**`databricks.yml`** — set these values:

| Value | What to provide |
|-------|-----------------|
| `target` workspace `host` | Your R2 workspace URL |
| `target_catalog` | R2 catalog holding the target bronze tables |
| `control_catalog` | Catalog for the migration control table (usually same as target) |

**`src/config/sdp_migration_config.yaml`** — global settings only:

| Value | What to provide |
|-------|-----------------|
| `business_unit` | Your BU name |
| `defaults.shared_catalog` / `shared_schema` | The Delta-shared catalog/schema from step 2 |
| `defaults.target_catalog` / `target_schema` | R2 catalog/schema of the target tables |

**`src/config/tables.csv`** — one row per table to migrate (replace the example rows):

| Column | What to provide |
|--------|-----------------|
| `table_name` | Name of the table to migrate |
| `cut_off_date` | Cut-over timestamp — backfill rows up to this |
| `checkpoint_col` | The table's timestamp/date column used for the cut-off |
| (optional) `partition_col`, per-table catalog/schema overrides | Blank = use the YAML `defaults` |
| (optional) `chunk_backfill` | Blank = `off`; `auto` = partition-at-a-time, resumable backfill |
| (optional) `backfill_days` | Blank = copy all history ≤ cut_off; else backfill only the last N days |
