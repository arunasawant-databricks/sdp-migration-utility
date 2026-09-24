"""Resolve the Lakeflow pipeline that owns a streaming-table target.

Used at registration (00_setup) to stamp each control row with the owning
pipeline (name + id), so an operator scanning the control table can see, per
table, which pipeline to open when a migration FAILS — e.g. one pipeline that
loads three STs shows all three rows, and a failed one points straight at the
pipeline to investigate.

Resolution of the pipeline ID:
  1) UC Tables API: `tables.get(target_fqn).pipeline_id` (primary), then
  2) privilege-free SQL fallback: `SHOW TBLPROPERTIES <fqn> ('pipelines.pipelineId')`
     — needs only access to the table itself (no Tables API, no information_schema,
     no system tables). A managed streaming table records its producing pipeline id
     as this table property; a plain Delta table has none.
The pipeline NAME is resolved from the ID via the Pipelines API `pipelines.get(id).name`.

BEST-EFFORT: any lookup problem — the target is not a streaming table, has no
pipeline_id, or a permission/SDK/SQL call raises — yields (None, None) and NEVER
raises. Populating this column is an operator convenience, not a correctness
requirement, so it must never fail a migration.
"""
from __future__ import annotations

from typing import Dict, Optional, Tuple


def _default_client():
    """Notebook-context WorkspaceClient (uses the job's own auth)."""
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient()


def _pipeline_id_via_api(client, target_fqn: str) -> Optional[str]:
    """UC Tables API: the managed ST records its producing pipeline id. None on any error."""
    try:
        return getattr(client.tables.get(target_fqn), "pipeline_id", None) or None
    except BaseException:  # noqa: BLE001 - permission/absent -> fall back to SQL
        return None


def _pipeline_id_via_sql(spark, target_fqn: str) -> Optional[str]:
    """Privilege-free fallback: read the `pipelines.pipelineId` table property via SQL.
    Needs only access to the table itself. None if absent/unreadable."""
    if spark is None:
        return None
    try:
        rows = spark.sql(f"SHOW TBLPROPERTIES {target_fqn}").collect()
        for r in rows:
            if r["key"] == "pipelines.pipelineId":
                return r["value"] or None
    except BaseException:  # noqa: BLE001 - unreadable -> no id
        pass
    return None


def resolve_pipeline(target_fqn: str, client=None,
                     name_cache: Optional[Dict[str, Optional[str]]] = None,
                     spark=None, build_client: bool = True) -> Tuple[Optional[str], Optional[str]]:
    """Return (pipeline_name, pipeline_id) for a streaming-table target, else (None, None).

    ID: Tables API first, then the privilege-free SQL table-property fallback.
    Name: Pipelines API (returns None if that call is not permitted — the ID is still returned).
    `name_cache` (pipeline_id -> name) lets a pipeline that owns several tables be
    resolved to its name once instead of per table.
    `build_client=False` skips constructing a WorkspaceClient here — the batch caller
    (resolve_many) already tried once, so we must not rebuild it per table on the failure path.
    """
    if client is None and build_client:
        try:
            client = _default_client()
        except BaseException:  # noqa: BLE001 - no client -> SQL-only ID, no name
            client = None

    # --- pipeline ID: Tables API (primary) -> SQL table-property (fallback) ---
    pid = _pipeline_id_via_api(client, target_fqn) if client is not None else None
    if not pid:
        pid = _pipeline_id_via_sql(spark, target_fqn)
    if not pid:
        return (None, None)

    # --- pipeline NAME: from the ID via the Pipelines API (cached ONCE per id) ---
    # The outcome is cached per pipeline id even when None, so we make AT MOST ONE
    # Pipelines-API call per pipeline — important for permission-restricted callers, where
    # a permanent denial would otherwise re-fire once per owned table. Trade-off: a rare
    # transient failure on the first table of a pipeline leaves its siblings' name blank for
    # that run (they still get the id). Acceptable for a best-effort operator-triage field.
    if name_cache is not None and pid in name_cache:
        return (name_cache[pid], pid)
    name = None
    if client is not None:
        try:
            name = getattr(client.pipelines.get(pid), "name", None)
        except BaseException:  # noqa: BLE001 - name not permitted/transient -> id-only
            name = None
    if name_cache is not None:
        name_cache[pid] = name
    return (name, pid)


def resolve_many(target_fqn_by_table: Dict[str, str], client=None, spark=None
                 ) -> Dict[str, Dict[str, Optional[str]]]:
    """Map {table_name: target_fqn} -> {table_name: {"name": .., "id": ..}}.

    Caches by pipeline id, so N tables owned by one pipeline cost one name lookup.
    """
    # Build the WorkspaceClient ONCE for the whole batch (a config may have 5000+ tables);
    # constructing one per table would re-run auth/config discovery every iteration.
    if client is None:
        try:
            client = _default_client()
        except BaseException:  # noqa: BLE001 - no client -> SQL-only id, no name
            client = None
    cache: Dict[str, Optional[str]] = {}
    out: Dict[str, Dict[str, Optional[str]]] = {}
    for table_name, target_fqn in target_fqn_by_table.items():
        # build_client=False: the client was built once above (or is deliberately None on a
        # build failure); resolve_pipeline must NOT reconstruct it per table.
        name, pid = resolve_pipeline(target_fqn, client=client, name_cache=cache,
                                     spark=spark, build_client=False)
        out[table_name] = {"name": name, "id": pid}
    return out
