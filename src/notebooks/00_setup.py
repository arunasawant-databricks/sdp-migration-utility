# Databricks notebook source
# MAGIC %md
# MAGIC # 00_setup — SDP migration utility
# MAGIC Loads the YAML config, ensures the control tables, acquires the run lock,
# MAGIC bulk-validates all tables (marks invalid ones SKIPPED), registers the rest
# MAGIC PENDING, and emits the to-process table list as a task value for the
# MAGIC downstream **For each** task.

# COMMAND ----------
import json
import os
import sys

dbutils.widgets.text("source_root", "", "Deployed src/ root (workspace path)")
dbutils.widgets.text("config_path", "", "Path to sdp_migration_config.yaml")
dbutils.widgets.text("run_id", "", "Job run id")

source_root = dbutils.widgets.get("source_root")
config_path = dbutils.widgets.get("config_path")
run_id = dbutils.widgets.get("run_id") or "manual"

# Make the utility library importable.
sys.path.insert(0, os.path.join(source_root, "lib"))

from config_loader import load_config          # noqa: E402
from control_table import ControlTable, Status  # noqa: E402
from validation import bulk_validate            # noqa: E402
from pipeline_lookup import resolve_many         # noqa: E402


def _read_path(p):
    """Open a bundle-deployed file, tolerating the /Workspace prefix."""
    for cand in (p, "/Workspace" + p if not p.startswith("/Workspace") else p):
        if os.path.exists(cand):
            return cand
    return p

# COMMAND ----------
cfg = load_config(_read_path(config_path))
spark.conf.set("spark.sql.session.timeZone", cfg.timezone)
print(f"BU={cfg.business_unit} tz={cfg.timezone} tables={len(cfg.tables)} "
      f"control={cfg.control.fqn}")

# COMMAND ----------
control = ControlTable(spark, cfg.control.fqn)
control.ensure()

# Non-blocking: multiple runs of the same BU may run in parallel (e.g. 5 jobs, 1000
# tables each). Concurrency is enforced per-table via claim_table(); this is audit only.
control.record_run_start(run_id, cfg.business_unit, started_by="job")

# COMMAND ----------
# Register every config table (idempotent; SUCCESS rows preserved), then mark
# invalid ones SKIPPED via live-schema validation (reads spark.table(...).schema;
# no information_schema).
batch_id = run_id
# Resolve the owning Lakeflow pipeline (name + id) of each target ST so the control
# table shows, per table, which pipeline to open on failure. Best-effort: any table
# that isn't a streaming table (or can't be resolved) simply gets NULL.
pipeline_info = resolve_many({t.table_name: t.target_fqn for t in cfg.tables}, spark=spark)
# Count tables mapped to a pipeline by ID (the id can resolve without a name — e.g. the
# privilege-free SQL fallback, or when the Pipelines API name lookup is not permitted).
resolved = sum(1 for v in pipeline_info.values() if v.get("id"))
print(f"pipeline lookup: {resolved}/{len(cfg.tables)} tables mapped to a pipeline")
control.register(run_id, cfg.business_unit, batch_id, cfg.tables, pipeline_info=pipeline_info)

verdicts = bulk_validate(spark, cfg.tables)
skipped = 0
valid_tables = []
for t in cfg.tables:
    # A non-fatal per-table config error (e.g. bad backfill_days) takes precedence: skip just
    # this table with the reason, so the rest of the config still migrates. Otherwise fall back
    # to the live-schema validation verdict.
    reason = t.config_error
    if not reason:
        v = verdicts.get(t.table_name)
        if v and not v.ok:
            reason = v.reason
    if reason:
        # Guarded: never clobber an already-SUCCESS row (a transient validation failure on
        # a rerun of a completed table must not flip it to SKIPPED). Count only rows actually
        # flipped, so the tally stays accurate when the guard no-ops on a SUCCESS row.
        skipped += control.skip_if_not_success(cfg.business_unit, t.table_name, reason)
    else:
        valid_tables.append(t.table_name)
# Self-heal any now-valid tables that a PRIOR run left SKIPPED (e.g. a shared table whose
# metadata had not yet propagated) — reset them to PENDING in ONE bulk UPDATE so they are
# re-queued this run, without a per-table round-trip.
control.clear_skips_bulk(cfg.business_unit, valid_tables)
print(f"validation: {skipped} SKIPPED, {len(cfg.tables) - skipped} valid")

# COMMAND ----------
# Emit the to-process list (valid + not already SUCCESS) for the For each task.
done = {Status.SUCCESS, Status.SKIPPED}
to_process = []
for t in cfg.tables:
    st = control.get_status(cfg.business_unit, t.table_name)
    if st in done:
        continue
    to_process.append({
        "table_name": t.table_name,
        "cut_off_date": t.cut_off_date,
        "checkpoint_col": t.checkpoint_col,
        "shared_fqn": t.shared_fqn,
        "target_fqn": t.target_fqn,
        "partition_col": t.partition_col,
        "chunk_backfill": t.chunk_backfill,
        "backfill_days": t.backfill_days,
    })

dbutils.jobs.taskValues.set(key="tables", value=to_process)
dbutils.jobs.taskValues.set(key="control_fqn", value=cfg.control.fqn)
dbutils.jobs.taskValues.set(key="business_unit", value=cfg.business_unit)
dbutils.jobs.taskValues.set(key="timezone", value=cfg.timezone)
dbutils.jobs.taskValues.set(key="retry_attempts", value=cfg.runtime.retry_attempts)
dbutils.jobs.taskValues.set(key="retry_backoff_seconds", value=cfg.runtime.retry_backoff_seconds)
dbutils.jobs.taskValues.set(key="vacuum_after", value=str(cfg.runtime.vacuum_after).lower())
dbutils.jobs.taskValues.set(key="vacuum_retain_hours", value=cfg.runtime.vacuum_retain_hours)
dbutils.jobs.taskValues.set(key="vacuum_override_retention_check",
                            value=str(cfg.runtime.vacuum_override_retention_check).lower())
dbutils.jobs.taskValues.set(key="claim_lease_minutes", value=cfg.runtime.claim_lease_minutes)
print(f"emitted {len(to_process)} tables to process")
