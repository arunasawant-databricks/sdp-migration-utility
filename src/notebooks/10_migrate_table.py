# Databricks notebook source
# MAGIC %md
# MAGIC # 10_migrate_table — one table (invoked per iteration by the For each task)
# MAGIC Receives one table spec as `input` (JSON) plus shared run params, and runs
# MAGIC containment DELETE → backfill INSERT → verify with resume-by-status.

# COMMAND ----------
import json
import os
import sys

dbutils.widgets.text("source_root", "", "Deployed src/ root (workspace path)")
dbutils.widgets.text("input", "", "One table spec (JSON) from For each")
dbutils.widgets.text("control_fqn", "", "Control table FQN")
dbutils.widgets.text("business_unit", "", "Business unit")
dbutils.widgets.text("run_id", "", "Job run id")
dbutils.widgets.text("timezone", "Asia/Kolkata", "Session timezone")
dbutils.widgets.text("retry_attempts", "5", "Retry attempts")
dbutils.widgets.text("retry_backoff_seconds", "5", "Retry backoff seconds")
dbutils.widgets.text("vacuum_after", "false", "Run VACUUM after verify")
dbutils.widgets.text("vacuum_retain_hours", "168", "VACUUM retain hours")
dbutils.widgets.text("vacuum_override_retention_check", "false", "Allow VACUUM < 168h")
dbutils.widgets.text("claim_lease_minutes", "120", "Per-table claim lease minutes")

source_root = dbutils.widgets.get("source_root")
sys.path.insert(0, os.path.join(source_root, "lib"))

from config_loader import TableSpec            # noqa: E402
from control_table import ControlTable         # noqa: E402
from migration import migrate_table, set_timezone  # noqa: E402

# COMMAND ----------
spec_raw = json.loads(dbutils.widgets.get("input"))
spec = TableSpec(
    table_name=spec_raw["table_name"],
    cut_off_date=spec_raw["cut_off_date"],
    checkpoint_col=spec_raw["checkpoint_col"],
    shared_fqn=spec_raw["shared_fqn"],
    target_fqn=spec_raw["target_fqn"],
    partition_col=spec_raw.get("partition_col"),
    chunk_backfill=spec_raw.get("chunk_backfill", "off"),
    backfill_days=spec_raw.get("backfill_days"),
)

control_fqn = dbutils.widgets.get("control_fqn")
business_unit = dbutils.widgets.get("business_unit")
run_id = dbutils.widgets.get("run_id") or "manual"
timezone = dbutils.widgets.get("timezone") or "Asia/Kolkata"
attempts = int(dbutils.widgets.get("retry_attempts") or 5)
backoff = int(dbutils.widgets.get("retry_backoff_seconds") or 5)
lease_minutes = int(dbutils.widgets.get("claim_lease_minutes") or 120)

# Vacuum settings (from setup task values); build a lightweight runtime holder.
from config_loader import RuntimeSpec  # noqa: E402
runtime = RuntimeSpec(
    max_parallel_tables=1,
    retry_attempts=attempts,
    retry_backoff_seconds=backoff,
    vacuum_after=(dbutils.widgets.get("vacuum_after") or "false").lower() == "true",
    vacuum_retain_hours=int(dbutils.widgets.get("vacuum_retain_hours") or 168),
    vacuum_override_retention_check=(
        dbutils.widgets.get("vacuum_override_retention_check") or "false").lower() == "true",
)

# COMMAND ----------
set_timezone(spark, timezone)
control = ControlTable(spark, control_fqn)

final = migrate_table(
    spark, control, spec,
    business_unit=business_unit, run_id=run_id, batch_id=run_id,
    retry_attempts=attempts, retry_backoff_seconds=backoff,
    lease_minutes=lease_minutes, runtime=runtime, logger=print,
)
print(f"{spec.table_name} -> {final}")

# Fail the task iteration on FAILED so the For each surfaces it (other iterations
# continue; the report task still runs with run_if all_done).
if final == "FAILED":
    raise RuntimeError(f"migration FAILED for {spec.table_name}")
