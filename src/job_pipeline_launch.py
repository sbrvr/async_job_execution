"""Launch the downstream Jobs / Pipelines of tables an upstream job has just refreshed, once all their sources are fresh.

Run as the LAST task of an upstream job (depends on all its other tasks), with --src_tables = the tables that job
refreshes (one or more, comma-separated):
  0. Candidates: tracked entities whose src_tables include any of them (and not in src_tables_to_skip). The given
     tables count as refreshed now — the job's earlier tasks just processed them, even if no rows changed.
For each candidate (optionally filtered by --entity_type):
  1. Freshness: every src table not in src_tables_to_skip refreshed within refresh_window_btn_tables_in_mins.
  2. Resolve entity_name to exactly one Job / Pipeline.
  3. Each row (entity + src_group) is its own launch rule. If the entity's latest run / update started after the
     row's newest source refresh it already covers it (skip). If a run is in progress but started before the refresh
     — e.g. launched for a different src_group — queue another; otherwise launch. An entity is launched at most once
     per task even if several of its groups are fresh.
  4. Launch: JOB -> jobs.run_now (queueing enabled), PIPELINE -> pipelines.start_update (to queue, wait for the
     running update to finish first). Logged only with --dry_run true.
Fails if any entity could not be resolved or launched.
"""
import time
from datetime import datetime, timezone

from databricks.sdk.service.jobs import QueueSettings, RunLifeCycleState

from async_job_execution.common import (ENTITY_TYPES, base_parser, get_spark, get_workspace_client, log_tracker_row,
                                        resolve_entity, str_to_bool)
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
    parser.add_argument("--queue_timeout_mins", type=int, default=60,
                        help="How long to wait for a running pipeline update to finish before queueing another")
    return parser.parse_args()


def _utc(epoch_ms):
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).replace(tzinfo=None)


def run_state(w, entity_type, entity_id):
    """Return (active, latest_start, latest_id) of the entity's most recent run / update."""
    if entity_type == "JOB":
        runs = list(w.jobs.list_runs(job_id=int(entity_id), limit=10))
        active = any(r.state and r.state.life_cycle_state in ACTIVE_RUN_STATES for r in runs)
        if not runs or not runs[0].start_time:
            return active, None, None
        return active, _utc(runs[0].start_time), runs[0].run_id
    p = w.pipelines.get(pipeline_id=entity_id)
    active = bool(p.state and p.state.value in ACTIVE_PIPELINE_STATES)
    latest = (p.latest_updates or [None])[0]
    if not latest or not latest.creation_time:
        return active, None, None
    return active, datetime.fromisoformat(latest.creation_time.replace("Z", "+00:00")).replace(tzinfo=None), latest.update_id


def decide(w, entity_type, entity_id, newest_refresh):
    """LAUNCH, QUEUE (a run is in progress but started before this group's sources were refreshed, so it cannot
    include them) or (SKIP, reason) when the latest run started after the refresh and already covers it."""
    active, latest_start, latest_id = run_state(w, entity_type, entity_id)
    if latest_start and latest_start >= newest_refresh:
        state = "in progress" if active else "finished"
        return "SKIP", f"run {latest_id} ({state}) started after the newest source refresh"
    return ("QUEUE", f"run {latest_id} in progress started before the newest source refresh") if active else ("LAUNCH", "")


def launch(w, entity_type, entity_id, name, queue, queue_timeout_mins):
    if entity_type == "JOB":
        # queue=True: if the job is at max_concurrent_runs, the run waits instead of being skipped
        run = w.jobs.run_now(job_id=int(entity_id), queue=QueueSettings(enabled=True))
        return f"{'queued' if queue else 'launched'} job '{name}' (job_id={entity_id}, run_id={run.run_id})"
    if queue:  # pipelines cannot queue an update: wait for the running one to finish, then start
        deadline = time.time() + queue_timeout_mins * 60
        while run_state(w, entity_type, entity_id)[0]:
            if time.time() > deadline:
                raise TimeoutError(f"pipeline still running after {queue_timeout_mins} min")
            time.sleep(30)
    update = w.pipelines.start_update(pipeline_id=entity_id)
    return (f"{'queued (after running update) ' if queue else ''}started pipeline '{name}' "
            f"(pipeline_id={entity_id}, update_id={update.update_id})")


def main():
    args = parse_args()
    dry_run = str_to_bool(args.dry_run)
    spark, w = get_spark(), get_workspace_client()
    refreshed = sorted({t.strip() for t in args.src_tables.replace(" ", ",").split(",") if t.strip()})
    if not refreshed:
        raise ValueError("--src_tables needs at least one table.")
    checker = FreshnessChecker(spark, w)  # caches lookups: a source shared by several rows is checked once
    print(f"Tracker: {args.tracker_table} | refreshed src_tables: {refreshed} | "
          f"entity_type: {args.entity_type} | dry_run: {dry_run}")

    rows = spark.sql(
        """
        SELECT *
        FROM IDENTIFIER(:tracker_table)
        WHERE (:entity_type = 'ALL' OR entity_type = :entity_type)
          -- downstream of the refreshed tables: uses one of them and does not skip it
          AND exists(src_tables, t -> array_contains(:refreshed, t)
                                     AND NOT array_contains(coalesce(src_tables_to_skip, array()), t))
        ORDER BY entity_type, entity_name, coalesce(src_group, 'default')
        """,
        args={"tracker_table": args.tracker_table, "entity_type": args.entity_type, "refreshed": refreshed},
    ).collect()
    print(f"Selected {len(rows)} tracker row(s) downstream of {refreshed}: "
          f"{[(r['entity_name'], r['src_group'] or 'default') for r in rows] or 'none'}")
    now = spark.sql("SELECT current_timestamp() AS ts").collect()[0]["ts"]
    results = []    # (entity_type, entity_name, src_group, status, detail)
    planned = []    # rows that passed the checks: (etype, name, group, entity_id, action, reason, src_tables)

    for r in rows:  # each row (entity + source group) is its own launch rule
        log_tracker_row(r)
        etype, name, group = (r["entity_type"] or "").upper(), r["entity_name"], r["src_group"] or "default"
        skip = set(r["src_tables_to_skip"] or [])
        tables = [t for t in (r["src_tables"] or []) if t not in skip]
        window = r["refresh_window_btn_tables_in_mins"]
        if etype not in ENTITY_TYPES:
            results.append((etype, name, group, "SKIPPED", f"unknown entity_type '{etype}'"))
            continue
        if window is None or not tables:
            results.append((etype, name, group, "SKIPPED", "refresh window is NULL or no source tables to check"))
            continue

        producers = {k: v.asDict() for k, v in (r["src_table_producers"] or {}).items()}
        cutoff, statuses = checker.check(tables, window, now, producers, refreshed_now=set(refreshed))
        print(f"    -> checking {len(tables)} src_table(s) against refresh_window_btn_tables_in_mins={window}:")
        for st in statuses:
            print(f"       {st.describe()}")
        stale = [st.table for st in statuses if not st.is_fresh]
        if stale:
            results.append((etype, name, group, "SKIPPED", f"stale sources: {stale}"))
            continue

        entity_id, error = resolve_entity(w, etype, name)
        if error:
            results.append((etype, name, group, "NOT_FOUND", error))
            continue
        action, reason = decide(w, etype, entity_id, max(st.refreshed_at for st in statuses))
        if action == "SKIP":
            results.append((etype, name, group, "ALREADY_HANDLED", reason))
            continue
        planned.append((etype, name, group, entity_id, action, reason, set(tables)))

    # Don't launch an entity whose sources are about to be refreshed again: a source is a target table of another
    # entity launched in this task, or its producing pipeline is running now. That upstream entity's own last task
    # launches it once its refresh is done (avoids a premature run on old data plus a second run).
    targets_of = {(r["entity_type"].upper(), r["entity_name"]): set(r["target_tables"] or []) for r in rows}
    launching = {(etype, name) for etype, name, *_ in planned}
    launched = {}   # (entity_type, entity_id) -> src_group that launched / queued it in this task
    for etype, name, group, entity_id, action, reason, src in planned:
        pending = sorted(t for other in launching - {(etype, name)} for t in src & targets_of.get(other, set()))
        running = sorted(t for t in src if checker.producer_running(t))
        if pending or running:
            results.append((etype, name, group, "WAITING_ON_UPSTREAM",
                            f"sources about to be refreshed: {pending + running} — launched by their producer's task"))
            continue
        if (etype, entity_id) in launched:  # another group of the same entity already launched it in this task
            results.append((etype, name, group, "ALREADY_HANDLED",
                            f"covered by the run launched for src_group '{launched[(etype, entity_id)]}'"))
            continue
        if dry_run:
            launched[(etype, entity_id)] = group
            results.append((etype, name, group, f"DRY_RUN_{action}", f"would {action.lower()} {etype} id={entity_id}"
                            + (f" ({reason})" if reason else "")))
            continue
        try:
            detail = launch(w, etype, entity_id, name, action == "QUEUE", args.queue_timeout_mins)
            launched[(etype, entity_id)] = group
            results.append((etype, name, group, "QUEUED" if action == "QUEUE" else "LAUNCHED",
                            detail + (f" — {reason}" if reason else "")))
        except Exception as ex:
            results.append((etype, name, group, "FAILED", f"launch failed: {ex}"))

    print("\nSummary:")
    for etype, name, group, status, detail in results:
        print(f"  {status:<15} {etype:<8} {name} [{group}]: {detail}")
    if any(status in ("FAILED", "NOT_FOUND") for *_, status, _ in results):
        raise Exception("One or more entities could not be launched — see summary above.")


if __name__ == "__main__":
    main()
