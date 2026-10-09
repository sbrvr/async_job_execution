"""Discover a Job's / Pipeline's target tables and upstream sources and upsert its row in the entity tracker.

Given a Job or Pipeline name:
  1. Resolve the name to exactly one Job / Pipeline.
  2. Pipelines: the pipeline itself, or the job's pipeline tasks (non-pipeline tasks are skipped).
  3. Owned tables: lineage targets of those pipelines that Unity Catalog still shows as managed by them (pipeline_id).
  4. Inputs per owned table come from lineage of that table's latest write (latest pipeline update).
  5. target_tables = owned tables not read by another owned table (final outputs);
     src_tables = inputs the entity does not produce itself, with UC views expanded via their current dependencies.
  6. Source group (--src_group, default "default"): --src_tables narrows the group's sources to a subset of the
     discovered ones, so one entity can have e.g. a "daily" and a "weekly" row with different sources and windows.
  7. src_table_producers = what produces each source: PIPELINE (UC pipeline_id) / JOB (latest lineage writer) / NONE.
  8. MERGE on entity_name + entity_type + src_group, replacing target_tables, src_tables and src_table_producers.
"""
from databricks.sdk.errors import NotFound, PermissionDenied

from async_job_execution.common import ENTITY_TYPES, base_parser, get_spark, get_workspace_client, resolve_entity, str_to_bool

VIEW_TYPES = {"VIEW", "METRIC_VIEW"}


def parse_args():
    parser = base_parser("Populate the entity tracker row for a Job or Pipeline")
    parser.add_argument("--entity_type", required=True, choices=ENTITY_TYPES)
    parser.add_argument("--entity_name", required=True, help="Exact name of the Job or Pipeline")
    parser.add_argument("--src_group", default="default", help="Source group of this row (e.g. daily / weekly)")
    parser.add_argument("--src_tables", default="",
                        help="Comma-separated subset of the discovered sources for this group; empty = all")
    parser.add_argument("--max_recursion_depth", type=int, default=5, help="Max depth for view expansion")
    parser.add_argument("--dry_run", default="false", help="true = print the tracker row without writing it")
    return parser.parse_args()


def pipelines_of(w, entity_type, entity_id):
    """PIPELINE -> itself; JOB -> its pipeline tasks (system.lakeflow.job_tasks has no task type / pipeline_id)."""
    if entity_type == "PIPELINE":
        return [entity_id]
    pipeline_ids, skipped = [], []
    for t in w.jobs.get(job_id=int(entity_id)).settings.tasks or []:
        if t.pipeline_task and t.pipeline_task.pipeline_id:
            pipeline_ids.append(t.pipeline_task.pipeline_id)
        else:
            skipped.append(t.task_key)
    if skipped:
        print(f"Skipping non-pipeline tasks (not tracked): {skipped}")
    return sorted(set(pipeline_ids))


def owned_tables(spark, w, workspace_id, pipeline_ids):
    """Lineage targets of the pipelines that UC still shows as managed by them."""
    candidates = [r["target_table_full_name"] for r in spark.sql(
        """
        SELECT DISTINCT target_table_full_name
        FROM system.access.table_lineage
        WHERE workspace_id = :ws
          AND entity_type = 'PIPELINE'  -- also when a job triggers the pipeline
          AND array_contains(:pipeline_ids, entity_id)
          AND target_table_full_name IS NOT NULL
        """,
        args={"ws": workspace_id, "pipeline_ids": pipeline_ids},
    ).collect()]
    owned, ignored = [], []
    for name in sorted(candidates):
        short = name.split(".")[-1]
        if short.startswith("__materialization") or short.startswith("event_log_"):
            continue  # pipeline bookkeeping tables
        try:
            info = w.tables.get(full_name=name)
        except (NotFound, PermissionDenied) as e:
            ignored.append((name, type(e).__name__))
            continue
        if info.pipeline_id in pipeline_ids:
            owned.append(name)
        else:
            ignored.append((name, f"pipeline_id={info.pipeline_id}"))  # e.g. written through a sink
    print(f"Owned tables ({len(owned)}): {owned}")
    print(f"Ignored lineage targets ({len(ignored)}): {ignored}")
    return owned


def table_edges(spark, workspace_id, pipeline_ids, owned):
    """(target, source) pairs from each owned table's latest write (latest pipeline update = lineage entity_run_id)."""
    return spark.sql(
        """
        WITH writes AS (
          SELECT target_table_full_name, source_table_full_name, entity_run_id, event_time
          FROM system.access.table_lineage
          WHERE workspace_id = :ws
            AND entity_type = 'PIPELINE'
            AND array_contains(:pipeline_ids, entity_id)
            AND array_contains(:owned, target_table_full_name)
            AND entity_run_id IS NOT NULL
        ),
        latest_run AS (
          SELECT target_table_full_name, max_by(entity_run_id, event_time) AS entity_run_id
          FROM writes
          GROUP BY target_table_full_name
        )
        SELECT DISTINCT w.target_table_full_name AS target, w.source_table_full_name AS source
        FROM writes w
        JOIN latest_run l USING (target_table_full_name, entity_run_id)
        WHERE w.source_table_full_name IS NOT NULL
          AND w.source_table_full_name != w.target_table_full_name
        """,
        args={"ws": workspace_id, "pipeline_ids": pipeline_ids, "owned": owned},
    ).collect()


def expand_views(w, names, max_depth):
    """Expand UC views to the tables they read using their current dependencies (views have no write history).
    Returns (tables, missing, unresolved views)."""

    def expand(name, depth, seen):
        if name in seen:  # circular view references
            return set(), set(), set()
        seen.add(name)
        try:
            info = w.tables.get(full_name=name)
        except NotFound:
            return set(), {name}, set()
        except PermissionDenied:
            return set(), {f"{name} (no access)"}, set()
        if (info.table_type.value if info.table_type else "TABLE") not in VIEW_TYPES:
            return {name}, set(), set()  # tables, materialized views and streaming tables are kept
        if depth >= max_depth:
            return set(), set(), {name}
        deps = [d.table.table_full_name for d in (info.view_dependencies.dependencies or [])
                if d.table] if info.view_dependencies else []  # function dependencies are ignored
        tables, missing, unresolved = set(), set(), set()
        for dep in deps:
            t, m, u = expand(dep, depth + 1, seen)
            tables, missing, unresolved = tables | t, missing | m, unresolved | u
        return tables, missing, unresolved

    tables, missing, unresolved = set(), set(), set()
    for name in names:
        t, m, u = expand(name, 0, set())
        tables, missing, unresolved = tables | t, missing | m, unresolved | u
    return tables, missing, unresolved


def source_producers(spark, w, workspace_id, src_tables):
    """PIPELINE (UC pipeline_id, exact) / JOB (job behind the latest lineage write, best effort) / NONE."""
    job_writers = {r["table"]: r["job_id"] for r in spark.sql(
        """
        SELECT target_table_full_name AS table, max_by(entity_id, event_time) AS job_id
        FROM system.access.table_lineage
        WHERE workspace_id = :ws
          AND entity_type = 'JOB'
          AND array_contains(:tables, target_table_full_name)
          AND event_date >= current_date() - INTERVAL 90 DAYS
        GROUP BY target_table_full_name
        """,
        args={"ws": workspace_id, "tables": src_tables or [""]},
    ).collect()}
    producers = {}
    for t in src_tables:
        try:
            pipeline_id = w.tables.get(full_name=t).pipeline_id
        except (NotFound, PermissionDenied):
            pipeline_id = None
        if pipeline_id:
            producers[t] = ("PIPELINE", pipeline_id)
        elif t in job_writers:
            producers[t] = ("JOB", str(job_writers[t]))
        else:
            producers[t] = ("NONE", None)
    return producers


MERGE_SQL = """
MERGE INTO IDENTIFIER(:tracker_table) AS target
USING tracker_source AS source
ON target.entity_name = source.entity_name
  AND target.entity_type = source.entity_type
  AND coalesce(target.src_group, 'default') = source.src_group
-- Existing Job/Pipeline: replace lineage-derived columns (each run rebuilds the full entity);
-- src_tables_to_skip and the refresh window keep their maintained values
WHEN MATCHED THEN UPDATE SET
  target.target_tables = source.target_tables,
  target.src_tables = source.src_tables,
  target.src_table_producers = source.src_table_producers,
  target.updated_on = current_timestamp(),
  target.updated_by = current_user()
-- New Job/Pipeline: empty skip list and a default 5-minute refresh window
WHEN NOT MATCHED THEN INSERT (
  entity_name, entity_type, src_group, target_tables, src_tables, src_tables_to_skip, src_table_producers,
  refresh_window_btn_tables_in_mins, created_on, updated_on, created_by, updated_by
) VALUES (
  source.entity_name, source.entity_type, source.src_group, source.target_tables, source.src_tables, cast(array() AS array<string>),
  source.src_table_producers, 5, current_timestamp(), current_timestamp(), current_user(), current_user()
)
"""


def main():
    args = parse_args()
    dry_run = str_to_bool(args.dry_run)
    spark, w = get_spark(), get_workspace_client()
    workspace_id = str(w.get_workspace_id())
    print(f"Entity: {args.entity_type} '{args.entity_name}' | src_group: {args.src_group} | "
          f"tracker: {args.tracker_table} | dry_run: {dry_run}")

    entity_id, error = resolve_entity(w, args.entity_type, args.entity_name)
    if error:
        raise ValueError(error)
    pipeline_ids = pipelines_of(w, args.entity_type, entity_id)
    if not pipeline_ids:
        raise ValueError(f"{args.entity_type} '{args.entity_name}' has no pipeline tasks — nothing to track.")
    print(f"Resolved {args.entity_type} -> {entity_id}; pipelines: {pipeline_ids}")

    owned = owned_tables(spark, w, workspace_id, pipeline_ids)
    if not owned:
        raise ValueError(f"No tables currently managed by {pipeline_ids}. Has the pipeline run at least once?")

    edges = table_edges(spark, workspace_id, pipeline_ids, owned)
    owned_set = set(owned)
    target_tables = sorted(owned_set - {e["source"] for e in edges if e["source"] in owned_set})
    external_inputs = sorted({e["source"] for e in edges if e["source"] not in owned_set})
    if not target_tables:
        raise ValueError("No final tables found — every owned table is read by another owned table (cycle?).")

    resolved, missing, unresolved = expand_views(w, external_inputs, args.max_recursion_depth)
    src_tables = sorted(resolved - owned_set)  # views expanded into the entity's own tables are not sources
    subset = {t.strip() for t in args.src_tables.replace(" ", ",").split(",") if t.strip()}
    if subset:
        unknown = sorted(subset - set(src_tables))
        if unknown:
            raise ValueError(f"--src_tables not among the discovered sources {src_tables}: {unknown}")
        src_tables = sorted(subset)
    producers = source_producers(spark, w, workspace_id, src_tables)

    print(f"target_tables ({len(target_tables)}): {target_tables}")
    print(f"src_tables ({len(src_tables)}):")
    for t, (ptype, pid) in producers.items():
        print(f"  {t}: produced by {ptype} {pid or ''}")
    if missing:
        print(f"Sources that no longer exist or are not accessible (excluded): {sorted(missing)}")
    if unresolved:
        print(f"Views not expanded (deeper than max_recursion_depth={args.max_recursion_depth}): {sorted(unresolved)}")

    tracker_row = spark.createDataFrame(
        [(args.entity_name, args.entity_type, args.src_group, target_tables, src_tables, producers)],
        "entity_name STRING, entity_type STRING, src_group STRING, target_tables ARRAY<STRING>, src_tables ARRAY<STRING>, "
        "src_table_producers MAP<STRING, STRUCT<producer_type: STRING, producer_id: STRING>>",
    )
    if dry_run:
        print("dry_run = true — tracker table not modified.")
        return
    tracker_row.createOrReplaceTempView("tracker_source")
    spark.sql(MERGE_SQL, args={"tracker_table": args.tracker_table}).show()


if __name__ == "__main__":
    main()
