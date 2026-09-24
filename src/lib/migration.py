"""Core per-table migration: containment DELETE -> backfill INSERT -> verify.

Runs against Region 2's pipeline-owned bronze STs while the continuous pipeline
feeds them (proven safe). Comparison of checkpoint_col <= cut_off_date happens in
the configured timezone (IST). Resume-by-status makes re-runs idempotent.
"""
from __future__ import annotations

import dataclasses
import threading
from typing import List, Optional

from control_table import ControlTable, Status
from retry import with_retry
import partition_prune


class _LeaseHeartbeat:
    """Background daemon that periodically renews a table's claim lease.

    A PB-scale backfill can run for hours — far longer than the initial
    ``claim_lease_minutes``. Once the lease expires, a concurrent run's
    ``claim_table`` WHERE clause matches (``lease_until < current_timestamp()``)
    and it reclaims the row → double claim → possible double INSERT (TC A1).
    This thread pushes ``lease_until`` forward at ~1/3 of the lease window,
    covering the chunked loop, a long single-shot INSERT, and one oversized
    partition (A7) uniformly. A renew failure is logged and retried on the next
    tick — it must never crash the migration.
    """

    def __init__(self, control: ControlTable, business_unit: str, table_name: str,
                 run_id: str, lease_minutes: int, logger=print):
        self._control = control
        self._bu = business_unit
        self._table = table_name
        self._run_id = run_id
        self._lease_minutes = max(1, int(lease_minutes))
        self._logger = logger
        # Renew ~3x per lease window (floor 15s) so the lease never lapses even
        # with a tiny test lease (e.g. claim_lease_minutes=1 -> renew every 20s).
        self._interval = max(15, (self._lease_minutes * 60) // 3)
        self._stop = threading.Event()
        # Set if a renew finds we NO LONGER own the row (another run reclaimed the
        # table). The backfill checks this and aborts to avoid a double-writer.
        self._lost = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name=f"lease-hb-{table_name}", daemon=True)

    def _run(self) -> None:
        # wait() returns True when stop() is signalled (exit); False on timeout (renew).
        while not self._stop.wait(self._interval):
            try:
                still_owned = self._control.renew_lease(
                    self._bu, self._table, self._run_id, self._lease_minutes)
                if still_owned:
                    self._logger(f"[{self._table}] lease renewed (+{self._lease_minutes}m)")
                else:
                    # Reservation lapsed and another run took the table -> stop renewing
                    # and signal the backfill to abort.
                    self._lost.set()
                    self._logger(f"[{self._table}] LEASE LOST — another run reclaimed this "
                                 f"table; aborting to avoid a double-writer")
                    return
            except BaseException as exc:  # noqa: BLE001 - heartbeat must never fail the run
                self._logger(f"[{self._table}] lease renew failed (will retry): {exc}")

    def lease_lost(self) -> bool:
        return self._lost.is_set()

    def start(self) -> "_LeaseHeartbeat":
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)


def _abort_if_lease_lost(heartbeat, table_name: str) -> None:
    """Raise if the heartbeat has detected the claim was reclaimed by another run,
    so the main thread stops writing (containment DELETE/INSERT) immediately."""
    if heartbeat is not None and heartbeat.lease_lost():
        raise RuntimeError(
            f"[{table_name}] lease lost mid-backfill (table reclaimed by another run) — "
            f"aborting before further writes")


def _chunk_mode(spec) -> str:
    """Normalize spec.chunk_backfill to 'off' | 'on' | 'auto' (tolerates bool or string)."""
    v = getattr(spec, "chunk_backfill", "off")
    if isinstance(v, bool):
        return "on" if v else "off"
    s = str(v).strip().lower()
    if s in ("on", "true", "yes"):
        return "on"
    if s == "auto":
        return "auto"
    return "off"


# --- SQL builders (pure; unit-testable) -------------------------------------
def _ckpt(spec) -> str:
    """The checkpoint_col expression used in every cutoff comparison.

    When the column's resolved type is a string (spec.checkpoint_type == "string"),
    wrap it in try_cast(... AS TIMESTAMP) so ISO-ish strings compare temporally and a
    malformed/unparseable value becomes NULL (excluded from both <=cutoff and >cutoff,
    never an ANSI cast error). try_cast is an identity/midnight no-op on timestamp/date,
    so this single expression is correct on both source and target even if their types
    differ; for non-string checkpoints we return the raw column, preserving Delta
    data-skipping on the checkpoint filter.
    """
    if (getattr(spec, "checkpoint_type", None) or "").lower() == "string":
        return f"try_cast({spec.checkpoint_col} AS TIMESTAMP)"
    return spec.checkpoint_col


def _window_lower(spec) -> str:
    """Optional lower bound for the backfill window (INSERT + expected-count side only).

    If spec.backfill_days is set to N, restrict to the last N days ending at cut_off:
    `AND checkpoint_col > (TIMESTAMP(cut_off) - INTERVAL N DAYS)`. Unset -> "" (copy all
    history <= cut_off, the default). NOT applied to the containment DELETE: the DELETE
    still clears everything <= cut_off, so with a window the target ends up holding ONLY
    the last N days (strict), and verify parity (target<=cutoff == windowed share) holds.
    """
    n = getattr(spec, "backfill_days", None)
    if n:
        return (f" AND {_ckpt(spec)} > "
                f"(TIMESTAMP('{spec.cut_off_date}') - INTERVAL {int(n)} DAYS)")
    return ""


def share_count_sql(spec) -> str:
    return (f"SELECT COUNT(*) AS n FROM {spec.source} "
            f"WHERE {_ckpt(spec)} <= TIMESTAMP('{spec.cut_off_date}')"
            f"{_window_lower(spec)}")


def target_le_count_sql(spec) -> str:
    return (f"SELECT COUNT(*) AS n FROM {spec.target} "
            f"WHERE {_ckpt(spec)} <= TIMESTAMP('{spec.cut_off_date}')")


def live_gt_count_sql(spec) -> str:
    return (f"SELECT COUNT(*) AS n FROM {spec.target} "
            f"WHERE {_ckpt(spec)} > TIMESTAMP('{spec.cut_off_date}')")


def containment_delete_sql(spec, predicate: Optional[str] = None) -> str:
    """Containment DELETE. `predicate` (from partition_prune) overrides the plain
    checkpoint filter to enable partition pruning; both are equivalent in the
    rows they match (partition predicate is AND-ed with the exact filter)."""
    where = predicate or f"{_ckpt(spec)} <= TIMESTAMP('{spec.cut_off_date}')"
    return f"DELETE FROM {spec.target} WHERE {where}"


def vacuum_sql(spec, retain_hours: int) -> str:
    return f"VACUUM {spec.target} RETAIN {retain_hours} HOURS"


def backfill_insert_sql(spec, columns: List[str]) -> str:
    cols = ", ".join(columns)
    return (f"INSERT INTO {spec.target} ({cols}) "
            f"SELECT {cols} FROM {spec.source} "
            f"WHERE {_ckpt(spec)} <= TIMESTAMP('{spec.cut_off_date}')"
            f"{_window_lower(spec)}")


def backfill_insert_predicate_sql(spec, columns: List[str], predicate: str) -> str:
    """Backfill INSERT for an explicit WHERE `predicate` (used by the chunked path;
    predicate always AND-s the exact checkpoint filter, so rows match single-shot).

    The optional backfill-window lower bound is appended here (INSERT side only) so a
    chunked backfill inserts only the last N days, while the per-chunk DELETE (which uses
    `predicate` unchanged) still clears the whole partition slice <= cut_off."""
    cols = ", ".join(columns)
    return (f"INSERT INTO {spec.target} ({cols}) "
            f"SELECT {cols} FROM {spec.source} WHERE {predicate}{_window_lower(spec)}")


# --- Spark helpers ----------------------------------------------------------
def set_timezone(spark, tz: str) -> None:
    spark.conf.set("spark.sql.session.timeZone", tz)


def _scalar(spark, sql: str) -> int:
    return int(spark.sql(sql).collect()[0]["n"])


def _target_columns(spark, spec) -> List[str]:
    """Column list from the TARGET ST (authority); backfill selects these from source."""
    return [f.name for f in spark.table(spec.target).schema.fields]


# --- Orchestration ----------------------------------------------------------
def migrate_table(spark, control: ControlTable, spec, business_unit: str,
                  run_id: str, batch_id: str, retry_attempts: int = 5,
                  retry_backoff_seconds: int = 5, lease_minutes: int = 120,
                  runtime=None, logger=print) -> str:
    """Migrate one table end-to-end with resume-by-status. Returns final status."""
    status = control.get_status(business_unit, spec.table_name)

    if status == Status.SUCCESS:
        logger(f"[{spec.table_name}] already SUCCESS — skip")
        return Status.SUCCESS
    if status == Status.SKIPPED:
        logger(f"[{spec.table_name}] SKIPPED (pre-flight) — skip")
        return Status.SKIPPED

    # Atomically CLAIM the table (per-table concurrency guard, retry-wrapped so a
    # Delta write conflict re-evaluates the claim). If another running job owns it
    # (valid lease), skip — that job will process it.
    won = with_retry(
        lambda: control.claim_table(business_unit, spec.table_name, run_id, lease_minutes),
        retry_attempts, retry_backoff_seconds, logger)
    if not won:
        logger(f"[{spec.table_name}] claimed by another run — skipping")
        return "CLAIMED_ELSEWHERE"

    # mark start + bump attempt (single atomic, conflict-safe UPDATE)
    control.mark_start(business_unit, spec.table_name)

    # Keep the claim lease alive for the whole run so a long backfill (chunked,
    # single-shot, or one oversized partition) is never reclaimed by a concurrent
    # run (A1). Stopped exactly once in the finally below.
    heartbeat = _LeaseHeartbeat(control, business_unit, spec.table_name, run_id,
                                lease_minutes, logger).start()
    try:
        # Resolve the checkpoint col's live type so cutoff comparisons normalize a
        # string column via try_cast (see _ckpt). Probe both sides; a string on either
        # side flips the whole table to the wrapped expression (a no-op on timestamp/
        # date, so it is safe even when the two sides differ).
        if getattr(spec, "checkpoint_type", None) is None:
            src_t = (partition_prune.column_type(spark, spec.source, spec.checkpoint_col) or "").lower()
            tgt_t = (partition_prune.column_type(spark, spec.target, spec.checkpoint_col) or "").lower()
            if "string" in (src_t, tgt_t):
                spec = dataclasses.replace(spec, checkpoint_type="string")
                logger(f"[{spec.table_name}] checkpoint_col '{spec.checkpoint_col}' is a string "
                       f"(source={src_t}, target={tgt_t}) — comparing via try_cast(... AS TIMESTAMP)")

        share_n = _scalar(spark, share_count_sql(spec))
        control.update(business_unit, spec.table_name, share_count=share_n)

        # Edge case: no history to migrate -> verify 0 and finish.
        if share_n == 0:
            live_before = _scalar(spark, live_gt_count_sql(spec))
            control.update(business_unit, spec.table_name, status=Status.SUCCESS,
                           deleted_count=0, inserted_count=0,
                           target_le_cutoff_count=0,
                           live_gt_cutoff_before=live_before,
                           live_gt_cutoff_after=live_before)
            logger(f"[{spec.table_name}] share has 0 rows <= cut_off — SUCCESS (nothing to do)")
            return Status.SUCCESS

        live_before = _scalar(spark, live_gt_count_sql(spec))
        control.update(business_unit, spec.table_name, live_gt_cutoff_before=live_before)

        # Steps: containment DELETE + backfill INSERT, in one of two modes.
        #  * Chunked/resumable: backfill one partition at a time, each its own atomic
        #    commit, recording a `backfill_oldest_partition` watermark. A mid-run
        #    failure (e.g. Delta-share connection drop) resumes without re-loading the
        #    partitions already committed. Adopts the retention framework's technique.
        #  * Single-shot (default): one containment DELETE (partition-pruned when
        #    possible) then one atomic backfill INSERT, with resume-by-status.
        # chunk_backfill mode selects between them:
        #   'off'  -> single-shot.
        #   'on'   -> chunk if the configured partition_col resolves (else warn + single-shot).
        #   'auto' -> chunk if a date/timestamp partition column (configured OR auto-discovered)
        #             is proven aligned with checkpoint_col; else silent single-shot.
        partition_col = getattr(spec, "partition_col", None)
        mode = _chunk_mode(spec)
        ptype = (partition_prune.resolve_partition(spark, spec.target, partition_col)
                 if partition_col else None)

        if mode == "auto" and ptype is None:
            # No usable configured partition_col — try to discover one and verify it is
            # aligned with checkpoint_col before trusting it for chunking.
            disc = partition_prune.discover_date_partition(spark, spec.target)
            if disc:
                cand_col, cand_type = disc
                if partition_prune.is_partition_aligned(
                        spark, spec.source, cand_col, cand_type,
                        _ckpt(spec), spec.cut_off_date):
                    partition_col, ptype = cand_col, cand_type
                    logger(f"[{spec.table_name}] auto: chunking on discovered partition "
                           f"'{cand_col}' ({cand_type})")
                else:
                    logger(f"[{spec.table_name}] auto: partition '{cand_col}' not aligned with "
                           f"checkpoint_col '{spec.checkpoint_col}' — single-shot backfill")
            else:
                logger(f"[{spec.table_name}] auto: target not partitioned on a date/timestamp "
                       f"column — single-shot backfill")

        use_chunked = mode in ("on", "auto") and ptype is not None

        if use_chunked:
            if status != Status.BACKFILLED:
                _chunked_backfill(spark, control, spec, partition_col, ptype,
                                  business_unit, retry_attempts, retry_backoff_seconds,
                                  logger, heartbeat)
        else:
            if mode == "on" and ptype is None:
                logger(f"[{spec.table_name}] chunk_backfill=on but partition_col "
                       f"'{partition_col}' is not a valid date/timestamp partition column "
                       f"— using single-shot backfill")

            # Step: containment DELETE (skip if already DELETED/BACKFILLED on resume).
            # Partition-pruned when a valid partition_col is configured; otherwise the
            # plain checkpoint filter. The rows matched are identical either way.
            if status not in (Status.DELETED, Status.BACKFILLED):
                _abort_if_lease_lost(heartbeat, spec.table_name)
                plan = partition_prune.plan_delete(
                    spark, spec.target, _ckpt(spec), spec.cut_off_date,
                    partition_col, logger)
                control.update(business_unit, spec.table_name,
                               partition_pruned=plan.pruned, delete_predicate=plan.predicate)
                deleted = with_retry(
                    lambda: _affected(spark, containment_delete_sql(spec, plan.predicate)),
                    retry_attempts, retry_backoff_seconds, logger)
                control.update(business_unit, spec.table_name, status=Status.DELETED,
                               deleted_count=deleted)
                logger(f"[{spec.table_name}] containment delete removed {deleted} "
                       f"(pruned={plan.pruned})")

            # Step: backfill INSERT (skip if already BACKFILLED on resume).
            if status != Status.BACKFILLED:
                cols = _target_columns(spark, spec)
                _abort_if_lease_lost(heartbeat, spec.table_name)
                inserted = with_retry(
                    lambda: _affected(spark, backfill_insert_sql(spec, cols),
                                      key="num_inserted_rows"),
                    retry_attempts, retry_backoff_seconds, logger)
                control.update(business_unit, spec.table_name, status=Status.BACKFILLED,
                               inserted_count=inserted)
                logger(f"[{spec.table_name}] backfill inserted {inserted}")

        # Step: VERIFY (row-count parity + live untouched).
        target_le = _scalar(spark, target_le_count_sql(spec))
        live_after = _scalar(spark, live_gt_count_sql(spec))
        control.update(business_unit, spec.table_name,
                       target_le_cutoff_count=target_le, live_gt_cutoff_after=live_after)

        parity = (target_le == share_n)
        live_ok = (live_after >= live_before)  # live only grows (pipeline appends)
        if parity and live_ok:
            # Optional VACUUM after a successful verify (guarded to the Delta safety
            # floor of 168h unless explicitly overridden). Adopted from the retention
            # framework; off by default.
            if runtime is not None and getattr(runtime, "vacuum_after", False):
                _maybe_vacuum(spark, spec, runtime, logger)
            control.update(business_unit, spec.table_name, status=Status.SUCCESS)
            logger(f"[{spec.table_name}] SUCCESS (target<=cutoff={target_le} == share={share_n})")
            return Status.SUCCESS

        msg = (f"verify failed: target_le={target_le} share={share_n} "
               f"live_before={live_before} live_after={live_after}")
        control.update(business_unit, spec.table_name, status=Status.FAILED, error_message=msg)
        logger(f"[{spec.table_name}] {msg}")
        return Status.FAILED

    except BaseException as exc:  # noqa: BLE001
        control.update(business_unit, spec.table_name, status=Status.FAILED,
                       error_message=str(exc)[:2000])
        logger(f"[{spec.table_name}] FAILED: {exc}")
        return Status.FAILED

    finally:
        # Stop the lease heartbeat and release the claim exactly once, on every
        # terminal path (success, verify-fail, or exception).
        heartbeat.stop()
        _end(spark, control, business_unit, spec.table_name, run_id)


def _chunked_backfill(spark, control: ControlTable, spec, partition_col: str, ptype: str,
                      business_unit: str, retry_attempts: int, retry_backoff_seconds: int,
                      logger=print, heartbeat=None) -> None:
    """Resumable per-partition backfill.

    Partitions are visited NEWEST FIRST (union_partition_values returns them descending),
    so the latest partition lands in the target ahead of the older ones. For each
    partition value <= cut_off (union of source & target), run a self-contained
    DELETE-then-INSERT of just that partition's slice, each its own atomic Delta commit,
    then advance the `backfill_oldest_partition` watermark (and, on the FIRST committed
    chunk, record the newest partition in `backfill_newest_partition` so the row shows the
    full newest->oldest range). Because the walk is descending, the watermark is the LOWEST
    partition committed so far; on resume, partitions already committed (>= watermark) are
    skipped via `v < watermark`, so a mid-run failure never re-loads them; the partition being processed when it failed is
    simply redone (DELETE-before-INSERT makes that idempotent). Every chunk predicate
    AND-s the exact checkpoint filter, so the union of chunks equals the single-shot
    `checkpoint_col <= cut_off` row set exactly.

    Ends with status=BACKFILLED and inserted_count set from the authoritative
    target<=cutoff count (accurate regardless of how many resumes it took).
    """
    watermark = control.get_field(business_unit, spec.table_name, "backfill_oldest_partition")
    values = partition_prune.union_partition_values(
        spark, spec.source, spec.target, partition_col, spec.cut_off_date, ptype)
    todo = [v for v in values if watermark is None or v < watermark]
    logger(f"[{spec.table_name}] chunked backfill on '{partition_col}': "
           f"{len(values)} partitions <= cut_off, {len(todo)} to process "
           f"(resume watermark={watermark})")

    control.update(business_unit, spec.table_name, partition_pruned=True)
    cols = _target_columns(spark, spec)
    total_deleted = 0
    # Newest partition committed (walk is descending, so the first committed chunk is the
    # newest). Read the existing value so a RESUME does not overwrite it; set it once.
    newest_seen = control.get_field(business_unit, spec.table_name, "backfill_newest_partition")
    for v in todo:
        # Stop before starting a new partition if we've lost the claim (another run
        # reclaimed the table) — prevents a double-writer on the remaining partitions.
        _abort_if_lease_lost(heartbeat, spec.table_name)
        pred = partition_prune.chunk_predicate(
            partition_col, v, ptype, _ckpt(spec), spec.cut_off_date)
        deleted = with_retry(
            lambda: _affected(spark, containment_delete_sql(spec, pred)),
            retry_attempts, retry_backoff_seconds, logger)
        inserted = with_retry(
            lambda: _affected(spark, backfill_insert_predicate_sql(spec, cols, pred),
                              key="num_inserted_rows"),
            retry_attempts, retry_backoff_seconds, logger)
        total_deleted += deleted
        # Watermark AFTER the chunk commits — so a crash before this leaves the chunk to
        # be redone (idempotent), never skipped. backfill_oldest_partition = the watermark;
        # backfill_newest_partition is set only on the first committed chunk of the backfill.
        fields = dict(backfill_oldest_partition=v, deleted_count=total_deleted)
        if newest_seen is None:
            newest_seen = v
            fields["backfill_newest_partition"] = v
        control.update(business_unit, spec.table_name, **fields)
        logger(f"[{spec.table_name}] partition {v}: -{deleted} +{inserted}")

    inserted_total = _scalar(spark, target_le_count_sql(spec))
    control.update(business_unit, spec.table_name, status=Status.BACKFILLED,
                   inserted_count=inserted_total)
    logger(f"[{spec.table_name}] chunked backfill complete: "
           f"target<=cutoff={inserted_total} ({len(todo)} partitions this run)")


def _maybe_vacuum(spark, spec, runtime, logger=print) -> None:
    """VACUUM the target, honoring the 168h Delta safety floor unless overridden."""
    hours = int(getattr(runtime, "vacuum_retain_hours", 168))
    if hours < 168 and not getattr(runtime, "vacuum_override_retention_check", False):
        logger(f"[{spec.table_name}] vacuum_retain_hours={hours} < 168 and override off "
               f"— skipping vacuum")
        return
    try:
        spark.sql(vacuum_sql(spec, hours))
        logger(f"[{spec.table_name}] vacuumed (retain {hours}h)")
    except BaseException as exc:  # noqa: BLE001 - vacuum failure must not fail the migration
        logger(f"[{spec.table_name}] vacuum skipped/failed (non-fatal): {exc}")


def _affected(spark, sql: str, key: str = "num_affected_rows") -> int:
    rows = spark.sql(sql).collect()
    if rows and key in rows[0].asDict():
        return int(rows[0][key])
    if rows and "num_affected_rows" in rows[0].asDict():
        return int(rows[0]["num_affected_rows"])
    return 0


def _end(spark, control: ControlTable, business_unit: str, table_name: str,
         run_id: Optional[str] = None) -> None:
    control.mark_end(business_unit, table_name)
    # release the per-table claim on any terminal outcome
    if run_id is not None:
        control.release_claim(business_unit, table_name, run_id)
