"""Fail unless every upstream source of a Job / Pipeline was refreshed within its tracker window.

Use as the first task of the job being checked (pass --entity_name {{job.name}}) so the rest of the job runs only
when all sources are fresh. A source counts as refreshed at the later of its last data write and its producer's last
successful run (see async_job_execution.freshness), so a producer run that changed no rows still counts.
"""
from async_job_execution.common import ENTITY_TYPES, base_parser, get_spark, get_workspace_client
from async_job_execution.freshness import FreshnessChecker


def parse_args():
    parser = base_parser("Check upstream source freshness for a Job or Pipeline")
    parser.add_argument("--entity_type", default="JOB", choices=ENTITY_TYPES)
    parser.add_argument("--entity_name", required=True, help="Exact name of the Job or Pipeline")
    return parser.parse_args()


def main():
    args = parse_args()
    spark = get_spark()
    label = f"{args.entity_type} '{args.entity_name}'"

    rows = spark.sql(
        """
        SELECT target_tables, src_tables, src_tables_to_skip, src_table_producers, refresh_window_btn_tables_in_mins
        FROM IDENTIFIER(:tracker_table)
        WHERE entity_name = :entity_name AND entity_type = :entity_type
        """,
        args={"tracker_table": args.tracker_table, "entity_name": args.entity_name, "entity_type": args.entity_type},
    ).collect()
    if len(rows) != 1:
        raise ValueError(f"Expected 1 tracker row for {label} in {args.tracker_table}, found {len(rows)}. "
                         f"Run upstream_source_tracking.py for this entity first.")
    row = rows[0]
    window = row["refresh_window_btn_tables_in_mins"]
    if window is None:
        raise ValueError(f"refresh_window_btn_tables_in_mins is NULL for {label}.")
    skip = set(row["src_tables_to_skip"] or [])
    tables = [t for t in (row["src_tables"] or []) if t not in skip]
    if not tables:
        raise ValueError(f"No source tables to check for {label} (src_tables empty or all skipped).")
    producers = {k: v.asDict() for k, v in (row["src_table_producers"] or {}).items()}

    print(f"Checking {label}: target tables {row['target_tables']}")
    if skip:
        print(f"Skipped sources (src_tables_to_skip): {sorted(skip)}")

    now = spark.sql("SELECT current_timestamp() AS ts").collect()[0]["ts"]
    cutoff, statuses = FreshnessChecker(spark, get_workspace_client()).check(tables, window, now, producers)
    print(f"Window: {window} min | now: {now} | cutoff: {cutoff}")
    for s in statuses:
        print(f"  {'FRESH' if s.is_fresh else 'STALE'}  {s.table}  refreshed_at={s.refreshed_at} "
              f"(last write {s.last_write}; {s.producer_type} {s.producer_id or ''} success {s.producer_success}"
              f" [{s.producer_signal or '-'}])" + (f" error={s.error}" if s.error else ""))

    stale = [s.table for s in statuses if not s.is_fresh]
    if stale:
        raise Exception(f"Freshness check FAILED for {label}: {len(stale)} of {len(tables)} source(s) not refreshed "
                        f"since {cutoff}: {stale}")
    print(f"Freshness check PASSED for {label}: all {len(tables)} source(s) refreshed since {cutoff}.")


if __name__ == "__main__":
    main()
