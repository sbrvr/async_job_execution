"""Launch the downstream Jobs / Pipelines of tables an upstream job has just refreshed, once all their sources are fresh.

Run as the LAST task of an upstream job (depends on all its other tasks), with --src_tables = the tables that job
refreshes (one or more, comma-separated):
  0. Candidates: tracked entities whose src_tables include any of them (and not in src_tables_to_skip). The given
     tables count as refreshed now — the job's earlier tasks just processed them, even if no rows changed.
For each candidate (optionally filtered by --entity_type):
  1. Freshness: every src table not in src_tables_to_skip refreshed within refresh_window_btn_tables_in_mins.
  2. Resolve entity_name to exactly one Job / Pipeline.
  3. Skip if it is running now or already started after the newest source refresh (launch once per refresh).
  4. Launch: JOB -> jobs.run_now, PIPELINE -> pipelines.start_update (logged only with --dry_run true).
Fails if any entity could not be resolved or launched.
"""
from datetime import datetime, timezone

from databricks.sdk.service.jobs import RunLifeCycleState

from async_job_execution.common import ENTITY_TYPES, base_parser, get_spark, get_workspace_client, resolve_entity, str_to_bool
from async_job_execution.freshness import FreshnessChecker

ACTIVE_RUN_STATES = {RunLifeCycleState.PENDING, RunLifeCycleState.QUEUED, RunLifeCycleState.RUNNING,
                     RunLifeCycleState.BLOCKED, RunLifeCycleState.WAITING_FOR_RETRY}
ACTIVE_PIPELINE_STATES = {"RUNNING", "STARTING", "DEPLOYING", "RESETTING"}


def parse_args():
    parser = base_parser("Launch downstream Jobs / Pipelines whose upstream sources are fresh")
    parser.add_argument("--src_tables", required=True,
                        help="Comma-separated fully qualified tables the calling upstream job has just refreshed")
    parser.add_argument("--entity_type", default="ALL", choices=("ALL",) + ENTITY_TYPES)
    parser.add_argument("--dry_run", default="true", help="true = check and log only, launch nothing")
    return parser.parse_args()


def _utc(epoch_ms):
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).replace(tzinfo=None)


def already_handled(w, entity_type, entity_id, newest_refresh):
    """Reason to skip if the entity is running now or already started after the newest source refresh."""
    if entity_type == "JOB":
        runs = list(w.jobs.list_runs(job_id=int(entity_id), limit=5))
        if any(r.state and r.state.life_cycle_state in ACTIVE_RUN_STATES for r in runs):
            return "job run already in progress"
        if runs and runs[0].start_time and _utc(runs[0].start_time) >= newest_refresh:
            return f"already ran after the newest source refresh (run {runs[0].run_id})"
    else:
        p = w.pipelines.get(pipeline_id=entity_id)
        if p.state and p.state.value in ACTIVE_PIPELINE_STATES:
            return "pipeline update already in progress"
        latest = (p.latest_updates or [None])[0]
        if latest and latest.creation_time:
            started = datetime.fromisoformat(latest.creation_time.replace("Z", "+00:00")).replace(tzinfo=None)
            if started >= newest_refresh:
                return f"already updated after the newest source refresh (update {latest.update_id})"
    return None


def launch(w, entity_type, entity_id, name):
    if entity_type == "JOB":
        run = w.jobs.run_now(job_id=int(entity_id))
        return f"launched job '{name}' (job_id={entity_id}, run_id={run.run_id})"
    update = w.pipelines.start_update(pipeline_id=entity_id)
    return f"started pipeline '{name}' (pipeline_id={entity_id}, update_id={update.update_id})"


def main():
    args = parse_args()
    dry_run = str_to_bool(args.dry_run)
    spark, w = get_spark(), get_workspace_client()
    refreshed = sorted({t.strip() for t in args.src_tables.replace(" ", ",").split(",") if t.strip()})
    if not refreshed:
        raise ValueError("--src_tables needs at least one table.")
    checker = FreshnessChecker(spark, w)  # caches lookups: a source shared by several entities is checked once
    print(f"Tracker: {args.tracker_table} | refreshed src_tables: {refreshed} | "
          f"entity_type: {args.entity_type} | dry_run: {dry_run}")

    entities = spark.sql(
        """
        SELECT entity_name, entity_type, src_tables, src_tables_to_skip, src_table_producers,
               refresh_window_btn_tables_in_mins
        FROM IDENTIFIER(:tracker_table)
        WHERE (:entity_type = 'ALL' OR entity_type = :entity_type)
          -- downstream of the refreshed tables: uses one of them and does not skip it
          AND exists(src_tables, t -> array_contains(:refreshed, t)
                                     AND NOT array_contains(coalesce(src_tables_to_skip, array()), t))
        ORDER BY entity_type, entity_name
        """,
        args={"tracker_table": args.tracker_table, "entity_type": args.entity_type, "refreshed": refreshed},
    ).collect()
    print(f"Downstream entities: {[e['entity_name'] for e in entities] or 'none'}")
    now = spark.sql("SELECT current_timestamp() AS ts").collect()[0]["ts"]
    results = []  # (entity_type, entity_name, status, detail)

    for e in entities:
        etype, name = (e["entity_type"] or "").upper(), e["entity_name"]
        skip = set(e["src_tables_to_skip"] or [])
        tables = [t for t in (e["src_tables"] or []) if t not in skip]
        window = e["refresh_window_btn_tables_in_mins"]
        print(f"\n{etype} '{name}'")
        if etype not in ENTITY_TYPES:
            results.append((etype, name, "SKIPPED", f"unknown entity_type '{etype}'"))
            continue
        if window is None or not tables:
            results.append((etype, name, "SKIPPED", "refresh window is NULL or no source tables to check"))
            continue

        producers = {k: v.asDict() for k, v in (e["src_table_producers"] or {}).items()}
        cutoff, statuses = checker.check(tables, window, now, producers, refreshed_now=set(refreshed))
        for s in statuses:
            print(f"  {'FRESH' if s.is_fresh else 'STALE'}  {s.table}  refreshed_at={s.refreshed_at} "
                  f"[{s.producer_signal or 'last write'}] cutoff={cutoff}")
        stale = [s.table for s in statuses if not s.is_fresh]
        if stale:
            results.append((etype, name, "SKIPPED", f"stale sources: {stale}"))
            continue

        entity_id, error = resolve_entity(w, etype, name)
        if error:
            results.append((etype, name, "NOT_FOUND", error))
            continue
        reason = already_handled(w, etype, entity_id, max(s.refreshed_at for s in statuses))
        if reason:
            results.append((etype, name, "ALREADY_HANDLED", reason))
            continue
        if dry_run:
            results.append((etype, name, "DRY_RUN", f"would launch {etype} id={entity_id}"))
            continue
        try:
            results.append((etype, name, "LAUNCHED", launch(w, etype, entity_id, name)))
        except Exception as ex:
            results.append((etype, name, "FAILED", f"launch failed: {ex}"))

    print("\nSummary:")
    for etype, name, status, detail in results:
        print(f"  {status:<15} {etype:<8} {name}: {detail}")
    if any(status in ("FAILED", "NOT_FOUND") for _, _, status, _ in results):
        raise Exception("One or more entities could not be launched — see summary above.")


if __name__ == "__main__":
    main()
