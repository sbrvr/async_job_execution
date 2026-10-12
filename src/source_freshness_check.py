"""Fail unless every upstream source of a Job / Pipeline was refreshed within its tracker window.

With several source groups (tracker rows) for the entity, every group must be fresh within its own window, unless
--src_group selects one.

Use as the first task of the job being checked (pass --entity_name {{job.name}}) so the rest of the job runs only
when all sources are fresh. A source counts as refreshed at the later of its last data write and its producer's last
successful run (see async_job_execution.freshness), so a producer run that changed no rows still counts.

--skip_if_already_processed true (with --job_run_id {{job.run_id}}): when the sources are fresh but the entity's
previous successful (or still running) run / update started after the newest source refresh (it already processed
this data; failed and cancelled runs, including ones this check cancelled, are passed over),
cancel this job run instead of passing, so the rest of the job does not run again on the same data.
"""
from itertools import islice

from async_job_execution.common import (ENTITY_TYPES, base_parser, get_spark, get_workspace_client, log_tracker_row,
                                        resolve_entity, str_to_bool)
from async_job_execution.freshness import FreshnessChecker, _naive_utc, utc

RECENT_RUNS = 25  # how many recent runs / updates to look through for the previous successful one
ACTIVE_RUN_STATES = {"PENDING", "QUEUED", "RUNNING", "BLOCKED", "WAITING_FOR_RETRY", "TERMINATING"}


def parse_args():
    parser = base_parser("Check upstream source freshness for a Job or Pipeline")
    parser.add_argument("--entity_type", default="JOB", choices=ENTITY_TYPES)
    parser.add_argument("--entity_name", required=True, help="Exact name of the Job or Pipeline")
    parser.add_argument("--src_group", default="", help="Check only this source group; empty = all groups")
    parser.add_argument("--skip_if_already_processed", default="false",
                        help="true = cancel this job run if the entity's previous run already processed this data")
    parser.add_argument("--job_run_id", default="",
                        help="Run id of the job running this check ({{job.run_id}}); required with "
                             "--skip_if_already_processed true")
    args = parser.parse_args()
    if str_to_bool(args.skip_if_already_processed) and not args.job_run_id.strip():
        parser.error("--skip_if_already_processed true needs --job_run_id={{job.run_id}}")
    return args


def previous_run(w, entity_type, entity_id, current_run_id):
    """(start, id, state) of the entity's latest successful or still-running run / update other than this job run.

    Failed and cancelled runs are passed over — including runs this check cancelled itself — so they neither count
    as having processed the data nor hide the successful run before them.
    """
    if entity_type == "JOB":
        # list_runs pages through the whole run history: look at the latest runs only
        for run in islice(w.jobs.list_runs(job_id=int(entity_id), limit=RECENT_RUNS), RECENT_RUNS):
            if str(run.run_id) == current_run_id or not run.start_time or not run.state:
                continue
            result = run.state.result_state.value if run.state.result_state else None
            life = run.state.life_cycle_state.value if run.state.life_cycle_state else None
            # no result and still active: it will include the data; no result and ended (SKIPPED, INTERNAL_ERROR): not
            if result == "SUCCESS" or (result is None and life in ACTIVE_RUN_STATES):
                return _naive_utc(run.start_time), run.run_id, result or life
        return None
    for u in w.pipelines.get(pipeline_id=entity_id).latest_updates or []:  # not started yet by this job
        state = u.state.value if u.state else None
        if u.creation_time and state not in ("FAILED", "CANCELED"):
            return _naive_utc(u.creation_time), u.update_id, state
    return None


def main():
    args = parse_args()
    spark, w = get_spark(), get_workspace_client()
    skip_processed = str_to_bool(args.skip_if_already_processed)
    label = f"{args.entity_type} '{args.entity_name}'" + (f" [src_group {args.src_group}]" if args.src_group else "")

    rows = spark.sql(
        """
        SELECT *
        FROM IDENTIFIER(:tracker_table)
        WHERE entity_name = :entity_name AND entity_type = :entity_type
          AND (:src_group = '' OR coalesce(src_group, 'default') = :src_group)
        ORDER BY coalesce(src_group, 'default')
        """,
        args={"tracker_table": args.tracker_table, "entity_name": args.entity_name, "entity_type": args.entity_type,
              "src_group": args.src_group},
    ).collect()
    if not rows:
        raise ValueError(f"No tracker row for {label} in {args.tracker_table}. "
                         f"Run upstream_source_tracking.py for this entity first.")

    checker = FreshnessChecker(spark, w)
    now = spark.sql("SELECT unix_millis(current_timestamp()) AS ts").collect()[0]["ts"]  # epoch ms: no session time zone
    print(f"Now: {utc(_naive_utc(now))} (epoch ms {now}); all times below are UTC, independent of the session time zone "
          f"({spark.conf.get('spark.sql.session.timeZone')})")
    failures, no_change, refreshes = [], [], []
    print(f"Selected {len(rows)} tracker row(s) for {label}")
    for row in rows:  # every source group must be fresh within its own window
        log_tracker_row(row)
        group, window = row["src_group"] or "default", row["refresh_window_btn_tables_in_mins"]
        skip = set(row["src_tables_to_skip"] or [])
        tables = [t for t in (row["src_tables"] or []) if t not in skip]
        if window is None or not tables:
            failures.append(f"src_group '{group}': refresh window is NULL or no source tables to check")
            continue
        producers = {k: v.asDict() for k, v in (row["src_table_producers"] or {}).items()}
        cutoff, statuses = checker.check(tables, window, now, producers)
        print(f"    -> checking {len(tables)} src_table(s) against refresh_window_btn_tables_in_mins={window}:")
        for s in statuses:
            print(f"       {s.describe()}")
        no_change += [f"{s.table} [{group}]" for s in statuses if s.met_without_row_changes]
        refreshes += [s.data_refreshed_at for s in statuses if s.data_refreshed_at]
        stale = [s.table for s in statuses if not s.is_fresh]
        if stale:
            failures.append(f"src_group '{group}': {len(stale)} of {len(tables)} source(s) not refreshed since "
                            f"{utc(cutoff)}: {stale}")

    if failures:
        raise Exception(f"Freshness check FAILED for {label}:\n  " + "\n  ".join(failures))

    if skip_processed:
        newest = max(refreshes)
        entity_id, error = resolve_entity(w, args.entity_type, args.entity_name)
        if error:
            raise ValueError(error)
        prev = previous_run(w, args.entity_type, entity_id, args.job_run_id.strip())
        if prev is None:
            print(f"\nNo previous successful run of {label}: not processed yet.")
        else:
            start, prev_id, state = prev
            if start >= newest:
                print(f"\nAlready processed: previous successful run {prev_id} ({state}) of {label} started {utc(start)}, after the "
                      f"newest source refresh {utc(newest)}. Cancelling job run {args.job_run_id} so the rest of the job "
                      f"does not run again on the same data.", flush=True)
                w.jobs.cancel_run(run_id=int(args.job_run_id))
                return
            print(f"\nNot processed yet: previous successful run {prev_id} started {utc(start)}, before the newest "
                  f"source refresh {utc(newest)}.")

    print(f"\nFreshness check PASSED for {label}: all {len(rows)} source group(s) fresh."
          + (f" Met by producer run without row changes: {no_change}" if no_change else ""))


if __name__ == "__main__":
    main()
