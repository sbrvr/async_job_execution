"""Create the entity tracker table (one row per Job / Pipeline / src_group). Safe to re-run (CREATE TABLE IF NOT EXISTS).

To rebuild after a schema change, drop the table first (deletes all rows) and re-run upstream_source_tracking.py
for each entity.
"""
from async_job_execution.common import base_parser, get_spark

DDL = """
CREATE TABLE IF NOT EXISTS IDENTIFIER(:tracker_table) (
    entity_name STRING
        COMMENT 'Name of the Databricks Job or Lakeflow Declarative Pipeline that writes the target tables',
    entity_type STRING
        COMMENT 'Type of the producing entity: JOB or PIPELINE',
    src_group STRING
        COMMENT 'Source group of the entity (e.g. daily / weekly): each group has its own src_tables and window and launches the entity on its own; default = default',
    target_tables ARRAY<STRING>
        COMMENT 'Fully qualified names (catalog.schema.table) of the final tables written by this Job/Pipeline',
    src_tables ARRAY<STRING>
        COMMENT 'Upstream base tables / materialized views feeding the target tables (views expanded)',
    src_tables_to_skip ARRAY<STRING>
        COMMENT 'Subset of src_tables to ignore in the freshness check (maintained manually)',
    src_table_producers MAP<STRING, STRUCT<producer_type: STRING, producer_id: STRING>>
        COMMENT 'Per src table: PIPELINE (pipeline_id) / JOB (job_id) / NONE. A successful producer run counts as a refresh even if no rows changed',
    refresh_window_btn_tables_in_mins INT
        COMMENT 'Max minutes allowed between source table refreshes for the Job/Pipeline to be launched',
    created_on DATE COMMENT 'Date the row was first inserted',
    updated_on DATE COMMENT 'Date the row was last updated',
    created_by STRING COMMENT 'User who inserted the row',
    updated_by STRING COMMENT 'User who last updated the row'
)
COMMENT 'Entity tracker: one row per Job/Pipeline and source group with its target tables and upstream source tables'
"""


def main():
    args = base_parser("Create the entity tracker table").parse_args()
    spark = get_spark()
    spark.sql(DDL, args={"tracker_table": args.tracker_table})
    spark.sql("DESCRIBE TABLE IDENTIFIER(:t)", args={"t": args.tracker_table}).show(truncate=False)


if __name__ == "__main__":
    main()
