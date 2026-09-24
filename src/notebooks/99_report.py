# Databricks notebook source
# MAGIC %md
# MAGIC # 99_report — aggregate summary, alert, release run lock
# MAGIC Runs after the For each task (run_if: all_done). Reads the control table,
# MAGIC posts a summary/alert, and releases the run-level lock.

# COMMAND ----------
import os
import sys

dbutils.widgets.text("source_root", "", "Deployed src/ root (workspace path)")
dbutils.widgets.text("control_fqn", "", "Control table FQN")
dbutils.widgets.text("business_unit", "", "Business unit")
dbutils.widgets.text("run_id", "", "Job run id")
dbutils.widgets.text("webhook_url", "", "Optional Slack/Teams webhook URL")

source_root = dbutils.widgets.get("source_root")
sys.path.insert(0, os.path.join(source_root, "lib"))

from control_table import ControlTable  # noqa: E402
from alerting import alert_if_needed     # noqa: E402

# COMMAND ----------
control_fqn = dbutils.widgets.get("control_fqn")
business_unit = dbutils.widgets.get("business_unit")
run_id = dbutils.widgets.get("run_id") or "manual"
webhook_url = dbutils.widgets.get("webhook_url")

control = ControlTable(spark, control_fqn)
counts = control.summary(run_id)

# COMMAND ----------
# Per-table detail for the run (auditable).
display(spark.sql(
    f"SELECT table_name, status, share_count, deleted_count, inserted_count, "
    f"target_le_cutoff_count, live_gt_cutoff_before, live_gt_cutoff_after, "
    f"error_message, start_ts, end_ts "
    f"FROM {control_fqn} WHERE run_id = '{run_id}' ORDER BY status, table_name"
))

# COMMAND ----------
alert_if_needed(counts, business_unit, run_id, webhook_url, logger=print)

# Record run end (audit). Mark FAILED if any table failed/was skipped.
run_status = "FAILED" if (counts.get("FAILED", 0) or counts.get("SKIPPED", 0)) else "COMPLETED"
control.record_run_end(run_id, run_status)
print(f"run {run_id} -> {run_status} | {counts}")

# Job-level gate: fail the whole job if ANY table is FAILED or SKIPPED, so the run
# surfaces as FAILED and the job's on_failure email notification fires. This task runs
# under run_if: ALL_DONE, so it is the single place that also catches SKIPPED tables
# (they never enter the migrate For-each loop). Raised last, after the audit/run-end and
# summary are recorded. Per-table detail (which table, why) is in the control table.
if run_status == "FAILED":
    raise RuntimeError(
        f"SDP migration run {run_id} did not fully succeed: {counts} — "
        f"one or more tables FAILED or were SKIPPED; see the control table for detail")
