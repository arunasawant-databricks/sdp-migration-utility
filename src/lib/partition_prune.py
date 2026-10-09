"""Partition-pruning helper for the containment DELETE.

Adopts a common data-retention technique: when the target
table is partitioned on a date/timestamp column that is aligned with the
checkpoint column, add a coarse partition predicate so Delta only rewrites the
relevant partitions, instead of scanning the whole table.

SAFETY: the partition predicate is ALWAYS AND-ed with the exact
`checkpoint_col <= cut_off` filter, so it only affects performance (which files
Delta touches) — never which rows are deleted. Pruning is OPT-IN via a
configured `partition_col`; without it (or if the column is not a real partition
column) we fall back to the plain checkpoint filter.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

_DATE_TYPES = {"date"}
_TS_TYPES = {"timestamp", "timestamp_ntz"}


@dataclass(frozen=True)
class PrunePlan:
    predicate: str          # full WHERE predicate for the DELETE
    pruned: bool            # True if a partition predicate was added
    partition_col: Optional[str] = None


# --- pure SQL builders (unit-testable, no Spark) ----------------------------
def checkpoint_filter(checkpoint_col: str, cut_off: str) -> str:
    return f"{checkpoint_col} <= TIMESTAMP('{cut_off}')"


def partition_predicate(partition_col: str, partition_type: str, cut_off: str) -> Optional[str]:
    """Coarse partition bound for rows whose checkpoint_col <= cut_off.

    Only valid when partition_col is the date/timestamp derived from
    checkpoint_col. Returns None if the type is not date/timestamp.
    """
    t = (partition_type or "").lower()
    if t in _DATE_TYPES:
        return f"{partition_col} <= DATE('{cut_off}')"
    if t in _TS_TYPES:
        return f"{partition_col} <= TIMESTAMP('{cut_off}')"
    return None


def build_delete_predicate(checkpoint_col: str, cut_off: str,
                           partition_col: Optional[str] = None,
                           partition_type: Optional[str] = None) -> PrunePlan:
    """Build the DELETE WHERE predicate, pruned when possible."""
    base = checkpoint_filter(checkpoint_col, cut_off)
    if partition_col:
        pp = partition_predicate(partition_col, partition_type or "", cut_off)
        if pp:
            # partition predicate FIRST so the optimizer prunes, then exact filter.
            return PrunePlan(predicate=f"{pp} AND {base}", pruned=True,
                             partition_col=partition_col)
    return PrunePlan(predicate=base, pruned=False)


# --- Spark metadata lookups -------------------------------------------------
def partition_columns(spark, table_fqn: str) -> List[str]:
    """Partition columns of a Delta table (empty if non-partitioned)."""
    try:
        row = spark.sql(f"DESCRIBE DETAIL {table_fqn}").select("partitionColumns").collect()
        return list(row[0][0]) if row and row[0][0] else []
    except Exception:
        return []


def column_type(spark, table_fqn: str, col: str) -> Optional[str]:
    try:
        for f in spark.table(table_fqn).schema.fields:
            if f.name.lower() == col.lower():
                return f.dataType.simpleString()
    except Exception:
        return None
    return None


def plan_delete(spark, table_fqn: str, checkpoint_col: str, cut_off: str,
                partition_col: Optional[str], logger=print) -> PrunePlan:
    """Resolve a PrunePlan against live table metadata.

    Prune only when the configured partition_col is an actual partition column
    of date/timestamp type; otherwise fall back (log why).
    """
    if not partition_col:
        return build_delete_predicate(checkpoint_col, cut_off)

    parts = [c.lower() for c in partition_columns(spark, table_fqn)]
    if partition_col.lower() not in parts:
        logger(f"[{table_fqn}] partition_col '{partition_col}' is not a partition "
               f"column ({parts or 'none'}); using plain checkpoint filter")
        return build_delete_predicate(checkpoint_col, cut_off)

    ptype = column_type(spark, table_fqn, partition_col)
    plan = build_delete_predicate(checkpoint_col, cut_off, partition_col, ptype)
    if not plan.pruned:
        logger(f"[{table_fqn}] partition_col '{partition_col}' type '{ptype}' not "
               f"date/timestamp; using plain checkpoint filter")
    return plan


# --- Chunked backfill support (INSERT side) ---------------------------------
# The retention framework prunes the DELETE by partition. The same partition layout
# lets the backfill INSERT run one partition at a time (each its own atomic commit),
# with a `backfill_oldest_partition` watermark for restart-safe resume. These helpers
# supply the per-partition predicates and the ordered partition list.
def resolve_partition(spark, table_fqn: str, partition_col: Optional[str]) -> Optional[str]:
    """Return the column's simple type IFF `partition_col` is a real date/timestamp
    partition column of the table; else None (caller falls back to single-shot)."""
    if not partition_col:
        return None
    parts = [c.lower() for c in partition_columns(spark, table_fqn)]
    if partition_col.lower() not in parts:
        return None
    t = (column_type(spark, table_fqn, partition_col) or "").lower()
    return t if (t in _DATE_TYPES or t in _TS_TYPES) else None


def _literal(value: str, ptype: str) -> str:
    """SQL literal for a date/timestamp value, matching the partition column type."""
    return f"DATE('{value}')" if (ptype or "").lower() in _DATE_TYPES else f"TIMESTAMP('{value}')"


def list_partition_values(spark, table_fqn: str, partition_col: str, cut_off: str,
                          ptype: str) -> List[str]:
    """Distinct partition values (as strings) with partition_col <= cut_off.

    Uses a SELECT (not table metadata), so it works on the Delta-shared source too.
    """
    bound = _literal(cut_off, ptype)
    sql = (f"SELECT DISTINCT CAST({partition_col} AS STRING) AS p FROM {table_fqn} "
           f"WHERE {partition_col} IS NOT NULL AND {partition_col} <= {bound}")
    try:
        return [r["p"] for r in spark.sql(sql).collect()]
    except Exception:
        return []


def union_partition_values(spark, source_fqn: str, target_fqn: str, partition_col: str,
                           cut_off: str, ptype: str) -> List[str]:
    """Distinct partition values <= cut_off across source AND target, NEWEST FIRST.

    Target is included so any stray target rows in a partition the source lacks are
    still cleaned by that chunk's DELETE (matches single-shot containment semantics).
    ISO date/timestamp strings sort chronologically, so reverse-sorting yields a valid
    newest->oldest order: the latest partition (e.g. Aug 17) is backfilled first and
    the loop works backwards to the earliest. The final row set is identical to any
    order (the union of chunks is order-independent); only arrival order changes.
    """
    vals = set(list_partition_values(spark, source_fqn, partition_col, cut_off, ptype))
    vals |= set(list_partition_values(spark, target_fqn, partition_col, cut_off, ptype))
    return sorted(vals, reverse=True)


def discover_date_partition(spark, table_fqn: str) -> Optional[tuple]:
    """Return (col, simple_type) of the first date/timestamp PARTITION column of the
    table, or None. Used by chunk_backfill='auto' to find a partition column without
    it being spelled out in config. Reads DESCRIBE DETAIL (not information_schema —
    UC does not expose partition columns there reliably)."""
    for c in partition_columns(spark, table_fqn):
        t = (column_type(spark, table_fqn, c) or "").lower()
        if t in _DATE_TYPES or t in _TS_TYPES:
            return (c, t)
    return None


def is_partition_aligned(spark, table_fqn: str, partition_col: str, ptype: str,
                         checkpoint_col: str, cut_off: str) -> bool:
    """True iff every row with checkpoint_col <= cut_off lives in a partition with
    partition_col <= cut_off (i.e. the partition is aligned with the checkpoint).

    This is the safety guard for chunk_backfill='auto': the chunked backfill only
    visits partitions with partition_col <= cut_off, so a MISaligned partition column
    would silently miss in-range rows. Probing the SOURCE (which already holds the
    history) for any counter-example makes auto-chunking safe; if not provably aligned
    we fall back to single-shot. Returns False on any error (fail safe)."""
    bound = _literal(cut_off, ptype)
    sql = (f"SELECT COUNT(*) AS n FROM {table_fqn} "
           f"WHERE {checkpoint_col} <= TIMESTAMP('{cut_off}') "
           f"AND ({partition_col} IS NULL OR {partition_col} > {bound})")
    try:
        return int(spark.sql(sql).collect()[0]["n"]) == 0
    except Exception:
        return False


def chunk_predicate(partition_col: str, value: str, ptype: str,
                    checkpoint_col: str, cut_off: str) -> str:
    """One partition's slice: `partition_col = <value> AND checkpoint_col <= cut_off`.

    Always AND-ed with the exact checkpoint filter, so the union of all chunks matches
    exactly the single-shot `checkpoint_col <= cut_off` row set — never more, never less.
    """
    return (f"{partition_col} = {_literal(value, ptype)} "
            f"AND {checkpoint_col} <= TIMESTAMP('{cut_off}')")
