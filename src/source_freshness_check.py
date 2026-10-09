"""Fail unless every upstream source of a Job / Pipeline was refreshed within its tracker window.

With several source groups (tracker rows) for the entity, every group must be fresh within its own window, unless
--src_group selects one.

Use as the first task of the job being checked (pass --entity_name {{job.name}}) so the rest of the job runs only
when all sources are fresh. A source counts as refreshed at the later of its last data write and its producer's last
successful run (see async_job_execution.freshness), so a producer run that changed no rows still counts.
"""
from async_job_execution.common import ENTITY_TYPES, base_parser, get_spark, get_workspace_client, log_tracker_row
from async_job_execution.freshness import FreshnessChecker


def parse_args():
    parser = base_parser("Check upstream source freshness for a Job or Pipeline")
    parser.add_argument("--entity_type", default="JOB", choices=ENTITY_TYPES)
    parser.add_argument("--entity_name", required=True, help="Exact name of the Job or Pipeline")
    parser.add_argument("--src_group", default="", help="Check only this source group; empty = all groups")
    return parser.parse_args()


def main():
    args = parse_args()
    spark = get_spark()
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

    checker = FreshnessChecker(spark, get_workspace_client())
    now = spark.sql("SELECT current_timestamp() AS ts").collect()[0]["ts"]
    failures = []
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
        stale = [s.table for s in statuses if not s.is_fresh]
        if stale:
            failures.append(f"src_group '{group}': {len(stale)} of {len(tables)} source(s) not refreshed since "
                            f"{cutoff}: {stale}")

    if failures:
        raise Exception(f"Freshness check FAILED for {label}:\n  " + "\n  ".join(failures))
    print(f"\nFreshness check PASSED for {label}: all {len(rows)} source group(s) fresh.")


if __name__ == "__main__":
    main()
