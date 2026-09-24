"""Config loader for the SDP migration utility.

Parses the YAML config, applies `defaults` to each table entry, resolves
fully-qualified source/target names, and validates the structure. Pure Python
(no Spark) so it is unit-testable off-cluster.
"""
from __future__ import annotations

import csv
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import yaml


# --- Resolved, per-table view the rest of the utility consumes ------------------
@dataclass(frozen=True)
class TableSpec:
    table_name: str
    cut_off_date: str          # IST timestamp string, e.g. "2026-08-20 23:59:59"
    checkpoint_col: str
    shared_fqn: str            # <shared_catalog>.<shared_schema>.<table_name>  (Delta-share read)
    target_fqn: str            # <target_catalog>.<target_schema>.<table_name>  (backfill target)
    partition_col: Optional[str] = None   # optional date/timestamp partition col aligned with
    #                                       checkpoint_col; enables partition-pruned containment DELETE
    config_error: Optional[str] = None    # set by the loader when a non-fatal per-table setting
    #                                       (e.g. a bad backfill_days) is invalid: the table is still
    #                                       parsed/registered, then 00_setup marks it SKIPPED with this
    #                                       reason so the rest of the config still migrates.
    backfill_days: Optional[int] = None   # optional; if set, backfill only the last N days
    #                                       ending at cut_off_date, i.e. rows with
    #                                       (cut_off - N days) < checkpoint_col <= cut_off. Unset =
    #                                       copy ALL history <= cut_off (default). Strict: the
    #                                       containment DELETE still clears everything <= cut_off,
    #                                       so the target ends up holding ONLY the last N days.
    chunk_backfill: str = "off"           # "off" | "on" | "auto". Backfill one partition at a
    #                                       time (atomic + resumable via watermark) instead of one
    #                                       big INSERT. "on": chunk if a valid partition_col is
    #                                       given (else warn + single-shot). "auto": chunk when the
    #                                       target is partitioned on a date/timestamp column aligned
    #                                       with checkpoint_col (auto-discovered + alignment-checked;
    #                                       else silent single-shot). "off"/absent: never chunk.
    checkpoint_type: Optional[str] = None  # NOT from config — resolved at migrate time from the
    #                                        live checkpoint_col type. When "string", cutoff
    #                                        comparisons wrap the column in try_cast(... AS TIMESTAMP)
    #                                        (see migration._ckpt); otherwise the raw column is used.

    @property
    def source(self) -> str:
        return self.shared_fqn

    @property
    def target(self) -> str:
        return self.target_fqn


@dataclass(frozen=True)
class ControlSpec:
    catalog: str
    schema: str
    table: str

    @property
    def fqn(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


@dataclass(frozen=True)
class RuntimeSpec:
    max_parallel_tables: int = 10   # authoritative concurrency is the databricks.yml var; this default is unused at runtime
    retry_attempts: int = 5
    retry_backoff_seconds: int = 5
    vacuum_after: bool = False          # run VACUUM after a table verifies
    vacuum_retain_hours: int = 168      # >= 168h unless override (Delta safety floor)
    vacuum_override_retention_check: bool = False
    claim_lease_minutes: int = 120   # per-table claim lease; a stale claim (crashed
    #                                  run) becomes reclaimable after this many minutes


@dataclass(frozen=True)
class MigrationConfig:
    version: int
    business_unit: str
    timezone: str
    control: ControlSpec
    runtime: RuntimeSpec
    tables: List[TableSpec] = field(default_factory=list)


class ConfigError(ValueError):
    """Raised when the YAML config is missing required fields or is inconsistent."""


def _norm_chunk_backfill(v: Any) -> str:
    """Normalize a chunk_backfill value to 'off' | 'on' | 'auto'.

    Accepts YAML bools (true/false) and strings (on/off/true/false/yes/no/auto).
    """
    if isinstance(v, bool):
        return "on" if v else "off"
    s = str(v).strip().lower()
    if s in ("on", "true", "yes"):
        return "on"
    if s in ("off", "false", "no", ""):
        return "off"
    if s == "auto":
        return "auto"
    raise ConfigError(f"chunk_backfill must be one of true/false/auto, got: {v!r}")


def _norm_backfill_days(v: Any, table_name: str) -> Optional[int]:
    """Normalize an optional backfill_days value to a positive int, or None.

    None/absent means copy all history <= cut_off (default). A provided value must be a
    positive integer (number of days ending at cut_off_date).
    """
    if v is None or (isinstance(v, str) and v.strip() == ""):
        return None
    try:
        n = int(v)
    except (TypeError, ValueError):
        raise ConfigError(f"tables ({table_name}) backfill_days must be a positive integer, got: {v!r}")
    if n <= 0:
        raise ConfigError(f"tables ({table_name}) backfill_days must be a positive integer, got: {v!r}")
    return n


_REQUIRED_DEFAULTS = (
    "shared_catalog",
    "shared_schema",
    "target_catalog",
    "target_schema",
)


# Per-table columns understood in the CSV. table_name/cut_off_date/checkpoint_col are
# required (enforced by parse_config); the rest are optional overrides that fall back to
# the YAML `defaults` when the cell is blank.
_CSV_COLUMNS = (
    "table_name",
    "cut_off_date",
    "checkpoint_col",
    "shared_catalog",
    "shared_schema",
    "target_catalog",
    "target_schema",
    "partition_col",
    "chunk_backfill",
    "backfill_days",
)


def _read_tables_csv(path: str) -> List[Dict[str, Any]]:
    """Read a per-table CSV into the same list-of-dicts shape parse_config expects.

    One row per table. Cells are stripped; blank cells are DROPPED from the row so the
    existing `entry.get(key, defaults[...])` lookup in parse_config applies the YAML
    `defaults` — a blank override column behaves like leaving that override unset. Stdlib
    csv only (no pandas) so this module stays dependency-light and unit-testable off-cluster.
    """
    with open(path, newline="") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            raise ConfigError(f"tables_csv is empty (no header row): {path}")
        headers = [(h or "").strip() for h in reader.fieldnames]
        unknown = [h for h in headers if h and h not in _CSV_COLUMNS]
        if unknown:
            raise ConfigError(
                f"tables_csv has unknown column(s) {unknown}; allowed: {list(_CSV_COLUMNS)}")

        rows: List[Dict[str, Any]] = []
        for lineno, raw_row in enumerate(reader, start=2):  # line 1 is the header
            entry: Dict[str, Any] = {}
            for key, val in raw_row.items():
                key = (key or "").strip()
                if not key or key not in _CSV_COLUMNS:
                    continue
                val = val.strip() if isinstance(val, str) else val
                if val in (None, ""):      # blank cell -> omit -> YAML default applies
                    continue
                entry[key] = val
            if not entry:                  # skip fully blank lines
                continue
            rows.append(entry)
    if not rows:
        raise ConfigError(f"tables_csv has a header but no table rows: {path}")
    return rows


def load_config(path: str) -> MigrationConfig:
    """Load and validate the migration config from a YAML file path.

    The per-table list is ALWAYS read from a CSV: the YAML must set `tables_csv` (path
    resolved relative to the YAML file's own directory). Global settings (`defaults`,
    `control`, `business_unit`, `timezone`) stay in the YAML; only the table rows live in
    the CSV.
    """
    with open(path, "r") as fh:
        raw = yaml.safe_load(fh)
    if not isinstance(raw, dict):
        raise ConfigError("config root must be a mapping")

    csv_ref = raw.get("tables_csv")
    if not csv_ref:
        raise ConfigError("config must set `tables_csv` (path to the per-table CSV)")
    csv_ref = str(csv_ref)
    csv_path = csv_ref if os.path.isabs(csv_ref) else os.path.join(
        os.path.dirname(os.path.abspath(path)), csv_ref)
    if not os.path.exists(csv_path):
        raise ConfigError(f"tables_csv file not found: {csv_path}")
    raw["tables"] = _read_tables_csv(csv_path)

    return parse_config(raw)


def parse_config(raw: Dict[str, Any]) -> MigrationConfig:
    """Validate + resolve an already-parsed config dict into a MigrationConfig."""
    if not isinstance(raw, dict):
        raise ConfigError("config root must be a mapping")

    defaults = raw.get("defaults") or {}
    missing = [k for k in _REQUIRED_DEFAULTS if not defaults.get(k)]
    if missing:
        raise ConfigError(f"defaults missing required keys: {missing}")

    timezone = defaults.get("timezone", "Asia/Kolkata")

    control_raw = raw.get("control") or {}
    for k in ("catalog", "schema", "table"):
        if not control_raw.get(k):
            raise ConfigError(f"control.{k} is required")
    control = ControlSpec(
        catalog=control_raw["catalog"],
        schema=control_raw["schema"],
        table=control_raw["table"],
    )

    # Concurrency, retry, and claim-lease are fixed operational constants in the scripts
    # (max_parallel_tables via the databricks.yml bundle variable; retry_attempts /
    # retry_backoff_seconds / claim_lease_minutes via RuntimeSpec defaults). They are no
    # longer read from the config file. Only the vacuum knobs remain config-tolerant.
    runtime_raw = raw.get("runtime") or {}
    runtime = RuntimeSpec(
        vacuum_after=bool(runtime_raw.get("vacuum_after", False)),
        vacuum_retain_hours=int(runtime_raw.get("vacuum_retain_hours", 168)),
        vacuum_override_retention_check=bool(
            runtime_raw.get("vacuum_override_retention_check", False)),
    )

    tables_raw = raw.get("tables")
    if not tables_raw or not isinstance(tables_raw, list):
        raise ConfigError("config must contain a non-empty `tables` list")

    tables: List[TableSpec] = []
    seen: set = set()
    for i, entry in enumerate(tables_raw):
        if not isinstance(entry, dict):
            raise ConfigError(f"tables[{i}] must be a mapping")
        name = entry.get("table_name")
        cut_off = entry.get("cut_off_date")
        if not name:
            raise ConfigError(f"tables[{i}] missing required `table_name`")
        if not cut_off:
            raise ConfigError(f"tables[{i}] ({name}) missing required `cut_off_date`")
        if name in seen:
            raise ConfigError(f"duplicate table_name in config: {name}")
        seen.add(name)

        # checkpoint_col is REQUIRED per table (each table has its own checkpoint
        # column); there is no global default.
        ck = entry.get("checkpoint_col")
        if not ck:
            raise ConfigError(f"tables[{i}] ({name}) missing required `checkpoint_col`")

        sc = entry.get("shared_catalog", defaults["shared_catalog"])
        ss = entry.get("shared_schema", defaults["shared_schema"])
        tc = entry.get("target_catalog", defaults["target_catalog"])
        ts = entry.get("target_schema", defaults["target_schema"])
        # optional; enables partition-pruned containment DELETE. per-table or default.
        pcol = entry.get("partition_col", defaults.get("partition_col"))
        # optional; per-table or default. "off"|"on"|"auto" (bools accepted too).
        chunk = _norm_chunk_backfill(entry.get("chunk_backfill", defaults.get("chunk_backfill", "off")))
        # optional; per-table or default. None = copy all history <= cut_off (default).
        # A bad value (e.g. 0/negative/non-int) is NOT fatal to the whole config: record the
        # reason in config_error and leave backfill_days=None; 00_setup marks just this table
        # SKIPPED so the remaining tables still migrate.
        bdays, cfg_err = None, None
        try:
            bdays = _norm_backfill_days(entry.get("backfill_days", defaults.get("backfill_days")), name)
        except ConfigError as e:
            cfg_err = str(e)

        tables.append(
            TableSpec(
                table_name=name,
                cut_off_date=str(cut_off),
                checkpoint_col=ck,
                shared_fqn=f"{sc}.{ss}.{name}",
                target_fqn=f"{tc}.{ts}.{name}",
                partition_col=pcol,
                chunk_backfill=chunk,
                backfill_days=bdays,
                config_error=cfg_err,
            )
        )

    return MigrationConfig(
        version=int(raw.get("version", 1)),
        business_unit=str(raw.get("business_unit", "unknown")),
        timezone=timezone,
        control=control,
        runtime=runtime,
        tables=tables,
    )
