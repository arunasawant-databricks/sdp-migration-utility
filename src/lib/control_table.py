"""Control / state table for the SDP migration utility.

Durable identity of a table's migration state is (business_unit, table_name), so
re-running the job resumes: SUCCESS tables are skipped; PENDING/FAILED retried.
A companion `<control>_runs` table provides the run-level concurrency guard.

SQL builders are module-level pure functions (unit-testable / runnable via CLI);
the `ControlTable` class wraps a SparkSession to execute them on-cluster.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# Substrings identifying a retryable Delta concurrency conflict on the control table
# (mirrors retry._RETRYABLE; kept local to avoid a lib import cycle).
_CONFLICT_TOKENS = (
    "ConcurrentAppendException",
    "ConcurrentDeleteReadException",
    "ConcurrentDeleteDeleteException",
    "ConcurrentTransactionException",
    "ProtocolChangedException",
    "MetadataChangedException",
    "DELTA_CONCURRENT",
    "concurrent update",
    "concurrent transaction",
)


class Status:
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    DELETED = "DELETED"        # containment delete done, insert not yet
    BACKFILLED = "BACKFILLED"  # insert done, not yet verified
    SUCCESS = "SUCCESS"      # terminal success
    FAILED = "FAILED"          # terminal failure (needs investigation / retry)
    SKIPPED = "SKIPPED"        # pre-flight excluded (e.g. bad name, ST absent)

    TERMINAL_OK = (SUCCESS,)
    RESUMABLE = (PENDING, IN_PROGRESS, DELETED, BACKFILLED, FAILED, SKIPPED)


# --- DDL --------------------------------------------------------------------
def ddl_control_sql(control_fqn: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {control_fqn} (
  business_unit           STRING,
  table_name              STRING,
  run_id                  STRING,
  batch_id                STRING,
  cut_off_date            STRING,
  checkpoint_col          STRING,
  source_fqn              STRING,
  target_fqn              STRING,
  pipeline_name           STRING,      -- Lakeflow pipeline that owns the target ST (operator triage)
  pipeline_id             STRING,      -- owning pipeline id (jump to /pipelines/<id> on failure)
  status                  STRING,
  attempt                 INT,
  share_count             BIGINT,
  deleted_count           BIGINT,
  inserted_count          BIGINT,
  target_le_cutoff_count  BIGINT,
  live_gt_cutoff_before   BIGINT,
  live_gt_cutoff_after    BIGINT,
  partition_pruned        BOOLEAN,     -- retention: containment DELETE was partition-pruned
  delete_predicate        STRING,      -- retention: predicate used for the single-shot DELETE
  backfill_newest_partition STRING,    -- chunked-backfill: newest partition committed (walk is descending; set once, on first chunk)
  backfill_oldest_partition STRING,    -- chunked-backfill: oldest partition committed so far; ALSO the resume watermark
  owner_run_id            STRING,      -- parallel-claim: run holding the per-row claim (lock)
  lease_until             TIMESTAMP,   -- parallel-claim: claim expiry; a stale claim is reclaimable
  error_message           STRING,
  start_ts                TIMESTAMP,
  end_ts                  TIMESTAMP,
  updated_ts              TIMESTAMP
) USING DELTA
CLUSTER BY (business_unit)
TBLPROPERTIES (
  delta.enableChangeDataFeed = false,
  delta.enableDeletionVectors = true   -- row-level concurrency: fewer conflicts when
                                        -- many parallel jobs claim/update disjoint rows
)
""".strip()


def ddl_runs_sql(runs_fqn: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {runs_fqn} (
  run_id         STRING,
  business_unit  STRING,
  status         STRING,      -- RUNNING | COMPLETED | FAILED
  started_by     STRING,
  start_ts       TIMESTAMP,
  end_ts         TIMESTAMP
) USING DELTA
""".strip()


def _sql_str(v: Any) -> str:
    """Render a Python value as a SQL literal (NULL / quoted string / number)."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def update_row_sql(control_fqn: str, business_unit: str, table_name: str,
                   fields: Dict[str, Any]) -> str:
    """Atomic single-row UPDATE (each table is owned by one For each task)."""
    assigns = ", ".join(f"{k} = {_sql_str(v)}" for k, v in fields.items())
    if assigns:
        assigns += ", "
    assigns += "updated_ts = current_timestamp()"
    return (
        f"UPDATE {control_fqn} SET {assigns} "
        f"WHERE business_unit = {_sql_str(business_unit)} "
        f"AND table_name = {_sql_str(table_name)}"
    )


# --- Runtime wrapper --------------------------------------------------------
class ControlTable:
    def __init__(self, spark, control_fqn: str, runs_fqn: Optional[str] = None):
        self.spark = spark
        self.control_fqn = control_fqn
        self.runs_fqn = runs_fqn or f"{control_fqn}_runs"
        # Serializes control-table mutations WITHIN this process so the lease
        # heartbeat thread and the main migration thread never commit to the same
        # row simultaneously (which Delta rejects as a concurrent-append conflict).
        self._write_lock = threading.RLock()

    def _write(self, sql: str, attempts: int = 8, backoff_seconds: float = 1.0):
        """Execute a control-table mutation: lock (in-process) + conflict-retry.

        The lock removes heartbeat-vs-main conflicts deterministically; the retry
        absorbs cross-process conflicts (another job briefly touching the same row).
        Control writes are small idempotent single-row SETs, so retrying is safe.
        """
        for i in range(attempts):
            try:
                with self._write_lock:
                    return self.spark.sql(sql)
            except BaseException as exc:  # noqa: BLE001 - re-raised if not a conflict / exhausted
                # Last attempt (or a non-conflict error) re-raises, so the loop always
                # exits via return or raise — there is no fall-through past it.
                if not any(t in str(exc) for t in _CONFLICT_TOKENS) or i == attempts - 1:
                    raise
                time.sleep(min(backoff_seconds * (2 ** i), 8.0))

    # Columns added after the table's first release; ensure() back-fills them on
    # pre-existing control tables (CREATE IF NOT EXISTS won't alter an existing one).
    _EVOLVE_COLUMNS = (
        ("partition_pruned", "BOOLEAN"),          # retention
        ("delete_predicate", "STRING"),           # retention
        ("backfill_newest_partition", "STRING"),  # chunked-backfill: newest partition committed
        ("backfill_oldest_partition", "STRING"),  # chunked-backfill: oldest committed so far (= resume watermark)
        ("owner_run_id", "STRING"),               # parallel-claim
        ("lease_until", "TIMESTAMP"),             # parallel-claim
        ("pipeline_name", "STRING"),              # owning Lakeflow pipeline of the target ST
        ("pipeline_id", "STRING"),                # owning pipeline id
    )

    def ensure(self) -> None:
        self.spark.sql(ddl_control_sql(self.control_fqn))
        self.spark.sql(ddl_runs_sql(self.runs_fqn))
        existing = {f.name.lower() for f in self.spark.table(self.control_fqn).schema.fields}
        # One-time migration for control tables created before the newest/oldest split:
        # RENAME the legacy watermark column (preserving its values) instead of adding a new
        # column and orphaning the old one. RENAME COLUMN needs Delta column-mapping mode
        # 'name' — already on for this CLUSTER BY (liquid-clustered) table, but set it
        # defensively. Idempotent: the guard makes it run only until the rename is done.
        if "last_backfilled_partition" in existing and "backfill_oldest_partition" not in existing:
            try:
                self.spark.sql(f"ALTER TABLE {self.control_fqn} SET TBLPROPERTIES "
                               f"('delta.columnMapping.mode' = 'name')")
                self.spark.sql(f"ALTER TABLE {self.control_fqn} "
                               f"RENAME COLUMN last_backfilled_partition TO backfill_oldest_partition")
            except BaseException:  # noqa: BLE001 - a concurrent run may have renamed it first
                pass
            # Re-read the ACTUAL schema so the ADD loop reflects reality regardless of who
            # won the rename race (idempotent: never re-adds an already-present column).
            existing = {f.name.lower() for f in self.spark.table(self.control_fqn).schema.fields}
            # If the rename genuinely did NOT happen (not a race — e.g. protocol/permission),
            # do NOT fall through to ADD COLUMNS, which would create a fresh NULL
            # backfill_oldest_partition and STRAND the old column's resume watermark. Surface it.
            if ("last_backfilled_partition" in existing
                    and "backfill_oldest_partition" not in existing):
                raise RuntimeError(
                    f"Failed to migrate control table {self.control_fqn}: could not RENAME "
                    f"last_backfilled_partition -> backfill_oldest_partition (check Delta "
                    f"column-mapping support / permissions). Not adding a fresh column, to "
                    f"avoid stranding the chunked-backfill resume watermark.")
        for col, typ in self._EVOLVE_COLUMNS:
            if col.lower() not in existing:
                self.spark.sql(f"ALTER TABLE {self.control_fqn} ADD COLUMNS ({col} {typ})")

    def register(self, run_id: str, business_unit: str, batch_id: str, specs: List,
                 pipeline_info: Optional[Dict[str, Dict[str, Any]]] = None) -> None:
        """Idempotently upsert one PENDING row per table.

        Existing SUCCESS rows are left untouched (skip on resume). Existing
        non-verified rows keep their status (so resume logic can act) but pick
        up the new run_id/batch_id and any refreshed cut_off/checkpoint.

        `pipeline_info` optionally maps table_name -> {"name": .., "id": ..} for the
        Lakeflow pipeline that owns the target ST (resolved by 00_setup via
        pipeline_lookup). It is stamped onto every row — including already-SUCCESS
        rows whose pipeline_name is still NULL — so an operator can see, per table,
        which pipeline to open when a migration FAILS. Missing/None values store NULL.
        """
        if not specs:      # nothing to register (config_loader normally guarantees >=1) -> no-op
            return
        from pyspark.sql.types import StructType, StructField, StringType

        pipeline_info = pipeline_info or {}
        rows = [
            {
                "business_unit": business_unit,
                "table_name": s.table_name,
                "run_id": run_id,
                "batch_id": batch_id,
                "cut_off_date": s.cut_off_date,
                "checkpoint_col": s.checkpoint_col,
                "source_fqn": s.source,
                "target_fqn": s.target,
                "pipeline_name": (pipeline_info.get(s.table_name) or {}).get("name"),
                "pipeline_id": (pipeline_info.get(s.table_name) or {}).get("id"),
            }
            for s in specs
        ]
        # Explicit all-STRING schema so a fully-NULL pipeline column (every lookup
        # failed) still infers as STRING rather than NullType.
        schema = StructType([StructField(k, StringType(), True) for k in rows[0].keys()])
        src = self.spark.createDataFrame(rows, schema)
        src.createOrReplaceTempView("_sdp_reg_src")
        self._write(f"""
            MERGE INTO {self.control_fqn} t
            USING _sdp_reg_src s
            ON t.business_unit = s.business_unit AND t.table_name = s.table_name
            WHEN MATCHED AND t.status <> '{Status.SUCCESS}' THEN UPDATE SET
              t.run_id = s.run_id, t.batch_id = s.batch_id,
              t.cut_off_date = s.cut_off_date, t.checkpoint_col = s.checkpoint_col,
              t.source_fqn = s.source_fqn, t.target_fqn = s.target_fqn,
              -- COALESCE: a transient lookup failure (NULL) must not wipe a
              -- previously-resolved pipeline name/id (operator triage pointer).
              t.pipeline_name = COALESCE(s.pipeline_name, t.pipeline_name),
              t.pipeline_id = COALESCE(s.pipeline_id, t.pipeline_id),
              t.updated_ts = current_timestamp()
            WHEN MATCHED AND t.pipeline_name IS NULL AND s.pipeline_name IS NOT NULL THEN UPDATE SET
              t.pipeline_name = s.pipeline_name, t.pipeline_id = s.pipeline_id,
              t.updated_ts = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (
              business_unit, table_name, run_id, batch_id, cut_off_date, checkpoint_col,
              source_fqn, target_fqn, pipeline_name, pipeline_id, status, attempt, updated_ts
            ) VALUES (
              s.business_unit, s.table_name, s.run_id, s.batch_id, s.cut_off_date, s.checkpoint_col,
              s.source_fqn, s.target_fqn, s.pipeline_name, s.pipeline_id, '{Status.PENDING}', 0, current_timestamp()
            )
        """)

    def get_status(self, business_unit: str, table_name: str) -> Optional[str]:
        df = self.spark.sql(
            f"SELECT status FROM {self.control_fqn} "
            f"WHERE business_unit = {_sql_str(business_unit)} "
            f"AND table_name = {_sql_str(table_name)}"
        )
        rows = df.collect()
        return rows[0]["status"] if rows else None

    def update(self, business_unit: str, table_name: str, **fields: Any) -> None:
        self._write(update_row_sql(self.control_fqn, business_unit, table_name, fields))

    def skip_if_not_success(self, business_unit: str, table_name: str, reason: str) -> int:
        """Mark a row SKIPPED with `reason`, but NEVER overwrite an already-SUCCESS row.
        Returns the number of rows actually flipped (0 if the row was already SUCCESS).

        Pre-flight validation can transiently fail for an already-migrated table (e.g. a
        Delta-share source momentarily unreadable on a rerun). Guarding on status <> SUCCESS
        (as register() and claim_table() do) prevents a completed migration from being
        clobbered to SKIPPED and falsely re-driven/reported."""
        res = self._write(
            f"UPDATE {self.control_fqn} SET status='{Status.SKIPPED}', "
            f"error_message = {_sql_str(reason)}, updated_ts = current_timestamp() "
            f"WHERE business_unit = {_sql_str(business_unit)} "
            f"AND table_name = {_sql_str(table_name)} "
            f"AND status <> '{Status.SUCCESS}'"
        )
        try:
            return int(res.collect()[0]["num_affected_rows"])
        except BaseException:  # noqa: BLE001 - if the metric is unavailable, assume it applied
            return 1

    def clear_skips_bulk(self, business_unit: str, table_names: List[str]) -> None:
        """Reset now-valid rows from SKIPPED back to PENDING in ONE statement.

        Self-heals TRANSIENT skips (e.g. a shared table's metadata not yet propagated at a
        prior setup) without any manual cleanup. No-op for rows not currently SKIPPED, so a
        still-invalid table stays SKIPPED. A single bulk UPDATE keyed on table_name IN (...)
        avoids one round-trip per table at 5000+ table scale."""
        if not table_names:
            return
        in_list = ", ".join(_sql_str(t) for t in table_names)
        self._write(
            f"UPDATE {self.control_fqn} SET status='{Status.PENDING}', error_message=NULL, "
            f"updated_ts = current_timestamp() "
            f"WHERE business_unit = {_sql_str(business_unit)} "
            f"AND status = '{Status.SKIPPED}' "
            f"AND table_name IN ({in_list})"
        )

    def mark_start(self, business_unit: str, table_name: str) -> None:
        """Mark a table IN_PROGRESS and bump its attempt (one atomic, conflict-safe UPDATE)."""
        self._write(
            f"UPDATE {self.control_fqn} SET status='{Status.IN_PROGRESS}', "
            f"attempt = COALESCE(attempt,0)+1, start_ts = current_timestamp(), "
            f"error_message = NULL, updated_ts = current_timestamp() "
            f"WHERE business_unit = {_sql_str(business_unit)} "
            f"AND table_name = {_sql_str(table_name)}"
        )

    def mark_end(self, business_unit: str, table_name: str) -> None:
        """Stamp end_ts on a terminal outcome (conflict-safe)."""
        self._write(
            f"UPDATE {self.control_fqn} SET end_ts = current_timestamp(), "
            f"updated_ts = current_timestamp() "
            f"WHERE business_unit = {_sql_str(business_unit)} "
            f"AND table_name = {_sql_str(table_name)}"
        )

    def get_field(self, business_unit: str, table_name: str, field: str) -> Any:
        """Read a single column for one (business_unit, table_name) row (None if absent)."""
        df = self.spark.sql(
            f"SELECT {field} AS v FROM {self.control_fqn} "
            f"WHERE business_unit = {_sql_str(business_unit)} "
            f"AND table_name = {_sql_str(table_name)}"
        )
        rows = df.collect()
        return rows[0]["v"] if rows else None

    def summary(self, run_id: str) -> Dict[str, int]:
        df = self.spark.sql(
            f"SELECT status, COUNT(*) AS n FROM {self.control_fqn} "
            f"WHERE run_id = {_sql_str(run_id)} GROUP BY status"
        )
        return {r["status"]: r["n"] for r in df.collect()}

    # --- run audit (NON-blocking) ---
    # Multiple runs of the same business_unit may run in parallel (e.g. 5 jobs each
    # migrating 1000 of a BU's 5000 tables). Concurrency safety is enforced per-table
    # via claim_table(), NOT by a BU-level gate. The runs table is audit only.
    def record_run_start(self, run_id: str, business_unit: str, started_by: str = "") -> None:
        self._write(
            f"INSERT INTO {self.runs_fqn} VALUES ("
            f"{_sql_str(run_id)}, {_sql_str(business_unit)}, 'RUNNING', "
            f"{_sql_str(started_by)}, current_timestamp(), NULL)"
        )

    def record_run_end(self, run_id: str, status: str = "COMPLETED") -> None:
        self._write(
            f"UPDATE {self.runs_fqn} SET status = {_sql_str(status)}, "
            f"end_ts = current_timestamp() WHERE run_id = {_sql_str(run_id)}"
        )

    # --- per-table atomic claim (the real concurrency guard) ---
    def claim_table(self, business_unit: str, table_name: str, run_id: str,
                    lease_minutes: int = 120) -> bool:
        """Atomically claim a table row for this run. Returns True iff won.

        A row is claimable when it is NOT terminal (SUCCESS/SKIPPED) AND is either
        unowned, already owned by this run, or its lease has expired (stale owner
        from a crashed run). The conditional UPDATE + Delta's serialized commits +
        the caller's retry wrapper make this a safe compare-and-set: two racers
        cannot both end up owning the row (the loser's commit re-evaluates the
        WHERE against the winner's claim and matches 0 rows). Locking columns
        (owner_run_id/lease_until) are separate from `status`, so claiming never
        destroys resume progress.
        """
        self._write(
            f"UPDATE {self.control_fqn} SET "
            f"owner_run_id = {_sql_str(run_id)}, "
            f"lease_until = current_timestamp() + INTERVAL {int(lease_minutes)} MINUTES, "
            f"updated_ts = current_timestamp() "
            f"WHERE business_unit = {_sql_str(business_unit)} "
            f"AND table_name = {_sql_str(table_name)} "
            f"AND status NOT IN ('{Status.SUCCESS}', '{Status.SKIPPED}') "
            f"AND (owner_run_id IS NULL OR owner_run_id = {_sql_str(run_id)} "
            f"     OR lease_until IS NULL OR lease_until < current_timestamp())"
        )
        # Confirm ownership after the (retried) commit settles.
        rows = self.spark.sql(
            f"SELECT owner_run_id FROM {self.control_fqn} "
            f"WHERE business_unit = {_sql_str(business_unit)} "
            f"AND table_name = {_sql_str(table_name)}"
        ).collect()
        return bool(rows) and rows[0]["owner_run_id"] == run_id

    def renew_lease(self, business_unit: str, table_name: str, run_id: str,
                    lease_minutes: int = 120) -> bool:
        """Extend the lease. Returns True iff we STILL own the row (the UPDATE
        matched it). A False means another run reclaimed the table (owner_run_id
        changed) — the caller must stop writing to avoid a double-writer."""
        df = self._write(
            f"UPDATE {self.control_fqn} SET "
            f"lease_until = current_timestamp() + INTERVAL {int(lease_minutes)} MINUTES, "
            f"updated_ts = current_timestamp() "
            f"WHERE business_unit = {_sql_str(business_unit)} "
            f"AND table_name = {_sql_str(table_name)} "
            f"AND owner_run_id = {_sql_str(run_id)}"
        )
        try:
            rows = df.collect()
            return bool(rows) and int(rows[0]["num_affected_rows"]) > 0
        except Exception:
            return True  # can't read the count -> assume still owned (don't false-alarm)

    def release_claim(self, business_unit: str, table_name: str, run_id: str) -> None:
        """Clear the claim on a terminal outcome (only if we still own it)."""
        self._write(
            f"UPDATE {self.control_fqn} SET owner_run_id = NULL, lease_until = NULL, "
            f"updated_ts = current_timestamp() "
            f"WHERE business_unit = {_sql_str(business_unit)} "
            f"AND table_name = {_sql_str(table_name)} "
            f"AND owner_run_id = {_sql_str(run_id)}"
        )
