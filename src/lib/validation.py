"""Pre-flight validation for the SDP migration utility.

Fail-fast BEFORE any destructive DML. Validates existence/columns by reading each
table's LIVE schema via the Spark catalog (`spark.table(fqn).schema`) — it does NOT
query `information_schema`, so it works in environments where access to
`information_schema` is restricted, and it is immune to Delta-Share metadata
propagation lag (the live schema is always authoritative). Returns a per-table
verdict the caller uses to mark SKIPPED.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

# Types we accept for checkpoint_col. All compare correctly against the TIMESTAMP
# cutoff: timestamp/timestamp_ntz directly, date coerces to midnight, and string
# coerces temporally (a common bronze pattern — dates stored as strings). A string
# checkpoint is normalized with try_cast(... AS TIMESTAMP) at migrate time so a
# malformed value becomes NULL (excluded from the window) instead of erroring.
_ACCEPTED_CKPT_TYPES = {"timestamp", "timestamp_ntz", "date", "string"}


@dataclass(frozen=True)
class Verdict:
    ok: bool
    reason: str = ""


def _live_schema(spark, fqn: str) -> Optional[Dict[str, str]]:
    """Return {col_lower -> type_lower} read from the LIVE table schema via the Spark
    catalog (no information_schema). Returns None if the table cannot be read (does not
    exist / no access) — the caller treats that as absent.

    Reading the live schema (rather than information_schema) means a freshly Delta-shared
    table validates as soon as it is SELECT-able, even before its catalog metadata rows
    have propagated — and it needs no information_schema privilege at all.
    """
    try:
        return {f.name.lower(): f.dataType.typeName().lower()
                for f in spark.table(fqn).schema.fields}
    except Exception:  # noqa: BLE001 - unreadable table -> caller treats it as absent
        return None


def bulk_validate(spark, specs: List) -> Dict[str, Verdict]:
    """Validate all table specs. Key = table_name -> Verdict.

    Checks (per-table, live-schema based — NO information_schema):
      - shared source table exists (readable);
      - target ST exists (readable);
      - checkpoint_col exists on BOTH source and target, and is a comparable type
        (timestamp/timestamp_ntz/date/string — see _ACCEPTED_CKPT_TYPES).
    (checkpoint_col NON-NULL is a documented data contract, not scanned here.)

    Each distinct FQN is read at most once (cached), so a config that references the
    same table repeatedly does not re-read its schema.
    """
    schema_cache: Dict[str, Optional[Dict[str, str]]] = {}

    def schema_of(fqn: str) -> Optional[Dict[str, str]]:
        key = fqn.lower()
        if key not in schema_cache:
            schema_cache[key] = _live_schema(spark, fqn)
        return schema_cache[key]

    out: Dict[str, Verdict] = {}
    for s in specs:
        ck = s.checkpoint_col.lower()

        src = schema_of(s.shared_fqn)
        if src is None:
            out[s.table_name] = Verdict(
                False, f"shared source table not found or not readable "
                       f"(check existence and SELECT/USE grants): {s.shared_fqn}")
            continue
        tgt = schema_of(s.target_fqn)
        if tgt is None:
            # Unreadable can mean the ST does not exist yet (pipeline never ran) OR a
            # missing grant; the message covers both so the operator checks access too.
            out[s.table_name] = Verdict(
                False, f"target ST not found or not readable "
                       f"(check existence and SELECT/USE grants): {s.target_fqn}")
            continue

        src_dt = src.get(ck)
        tgt_dt = tgt.get(ck)
        if src_dt is None:
            out[s.table_name] = Verdict(False, f"checkpoint_col '{s.checkpoint_col}' missing on source")
            continue
        if tgt_dt is None:
            out[s.table_name] = Verdict(False, f"checkpoint_col '{s.checkpoint_col}' missing on target")
            continue
        if src_dt not in _ACCEPTED_CKPT_TYPES or tgt_dt not in _ACCEPTED_CKPT_TYPES:
            out[s.table_name] = Verdict(
                False, f"checkpoint_col type not comparable to cutoff — need one of "
                       f"{sorted(_ACCEPTED_CKPT_TYPES)} (source={src_dt}, target={tgt_dt})")
            continue
        out[s.table_name] = Verdict(True, "ok")
    return out
