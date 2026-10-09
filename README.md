# async_job_execution

Launch downstream Databricks Jobs / Lakeflow Spark Declarative Pipelines (SDP) only when **all** of their upstream
source tables have been refreshed within a configurable time window — launched asynchronously by the last task of the
upstream job that refreshes them, or checked by a gating task before a downstream job runs.

## Why not table update triggers

Jobs table update triggers only offer **Any table updated** or **All tables updated**:

1. **Mixed cadences (daily + weekly sources).** For a job which relies on source which has varying cadence with some tables getting updated daily and others getting updated weekly (different file arrival) - its a challenge to set it with any of the option *All tables Updated* or *Any table updated*. The former will make the job wait as there are no updates and later will kick off the job multiple times in a day when weekly files arrive.  Multiple Table triggers are currently not supported for a single job and even if they were supported job would run twice (one for daily and one for weekly) as one trigger cannot be set depend on another trigger(another feature).
2. **Only row-changing writes fire.** A MERGE / UPDATE / DELETE that changes no rows, OPTIMIZE and property changes
   do not fire. An SCD2 (AUTO CDC) update with nothing new writes no table version at all, so with *All tables
   updated* a job that also depends on a quiet SCD2 table **waits forever**.
3. **No time window, no timeout.** "All updated" means *since the last run*, not *recently*; if a source never
   updates, the job silently never runs.
4. **At most 10 tables per trigger.**

## Best suited for SDP jobs

The framework is designed for **upstream jobs whose work is done by SDP pipeline tasks**:

```
upstream job:  [SDP pipeline task] ──┐
               [SDP pipeline task] ──┼──> [job_pipeline_launch.py --src_tables=<tables those pipelines refresh>]
               [SDP pipeline task] ──┘                         │
                                                               └──> launches each downstream Job / Pipeline
                                                                    whose sources are now all fresh
```

- The launch task is the **last task** and depends on all pipeline tasks, so by the time it runs every pipeline has
  **finished** and its tables are complete — the tables passed in `--src_tables` count as refreshed now, even when a
  pipeline update changed no rows (e.g. an SCD2 / AUTO CDC update with nothing new).
- Discovery also relies on SDP metadata (pipeline-managed tables, flow progress); only **pipeline tasks** of a job
  are tracked, other task types are skipped.
- Sources from other producers (other jobs / pipelines) still go through the freshness check, so a downstream entity
  launches only when **all** its sources are fresh.

## Components

| File | Role |
|---|---|
| `src/tracker_setup.py` | Creates the tracker table (safe to re-run). |
| `src/upstream_source_tracking.py` | For a Job / Pipeline name: finds its pipelines (job → `pipeline_task`s), the tables they currently manage (lineage targets confirmed by UC `pipeline_id`), the **final** tables (not read by another owned table) and the **external sources** (inputs of each owned table's latest write, minus owned tables, UC views expanded via `view_dependencies`). Records each source's producer and MERGEs one tracker row. |
| `src/job_pipeline_launch.py` | **Last task of an upstream job**, `--src_tables` = the tables its pipelines just refreshed. Finds tracked entities that use any of them (and don't skip them); those tables count as refreshed now, other sources go through the freshness rule; then exact name resolution → skip if running or already started after the newest source refresh → `run_now` / `start_update`. |
| `src/source_freshness_check.py` | Optional gating task: fails unless every source (minus skip list) of one entity is fresh — first task of a scheduled downstream job. |
| `src/async_job_execution/freshness.py` | Freshness rule shared by the launcher and the check. |
| `src/async_job_execution/common.py` | Spark / SDK helpers, argument parsing, exact name resolution. |

## Tracker table (`surajb.common.entity_tracker`)

One row per downstream Job / Pipeline, keyed by `entity_name` + `entity_type`.

| Column | Type | Meaning |
|---|---|---|
| `entity_name` | STRING | Job or Pipeline name |
| `entity_type` | STRING | `JOB` or `PIPELINE` |
| `target_tables` | ARRAY<STRING> | Final tables written by the entity |
| `src_tables` | ARRAY<STRING> | Upstream source tables (views expanded) |
| `src_tables_to_skip` | ARRAY<STRING> | Sources excluded from the check (manual) |
| `src_table_producers` | MAP<STRING, STRUCT<producer_type, producer_id>> | Per source: `PIPELINE` / `JOB` / `NONE` |
| `refresh_window_btn_tables_in_mins` | INT | Freshness window (default 5) |
| `created_on`, `updated_on`, `created_by`, `updated_by` | | Audit |

## Freshness rule

A source is **refreshed** at the later of:
1. its **last data write** (`DESCRIBE HISTORY`, data operations only), and
2. its **producer's last successful run**, even if no rows changed:
   - `PIPELINE` — latest `COMPLETED` flow for the table, from the system pipeline events table
     (`system.lakeflow_pipeline_events_preview.pipeline_events`, Beta) if available, else the pipeline's
     `event_log()`, else the latest completed pipeline update covering the table;
   - `JOB` — end of the producing job's last successful run;
   - `NONE` — last data write only.

Tables passed to `job_pipeline_launch.py --src_tables` count as refreshed at launch time (the calling job's pipeline
tasks have just finished). A source is fresh if `refreshed_at >= now − refresh_window_btn_tables_in_mins`.

## Running as job tasks

Each script is a **Python script task** (`spark_python_task`) on serverless compute. Point the task at the file in
this repo (job **Git source**, or a Databricks Git folder); `src/` is the script directory, so the
`async_job_execution` package is importable.

| Script | Parameters |
|---|---|
| `tracker_setup.py` | — |
| `upstream_source_tracking.py` | `--entity_type JOB\|PIPELINE --entity_name <name> [--dry_run true] [--max_recursion_depth 5]` |
| `job_pipeline_launch.py` | `--src_tables <t1>,<t2>,... [--entity_type ALL\|JOB\|PIPELINE] [--dry_run true\|false]` |
| `source_freshness_check.py` | `--entity_type JOB\|PIPELINE --entity_name <name>` (as a gating task: `--entity_name={{job.name}}`) |

All scripts accept `--tracker_table` (default `surajb.common.entity_tracker`). `job_pipeline_launch.py` defaults to
`--dry_run true`; set `false` to launch.

Example: launch task added as the last task of an upstream job

```json
{
  "task_key": "launch_downstream",
  "depends_on": [{"task_key": "bronze_pipeline"}, {"task_key": "silver_pipeline"}],
  "environment_key": "default",
  "spark_python_task": {
    "python_file": "src/job_pipeline_launch.py",
    "source": "GIT",
    "parameters": ["--src_tables=main.silver.customers,main.silver.orders", "--dry_run=false"]
  }
}
```

with `"git_source": {"git_url": "https://github.com/sbrvr/async_job_execution", "git_provider": "gitHub", "git_branch": "main"}`
and `"environments": [{"environment_key": "default", "spec": {"environment_version": "4"}}]` on the job.

## Typical setup

1. Run `tracker_setup.py` once.
2. Run `upstream_source_tracking.py` for each downstream Job / Pipeline (re-run after its definition changes).
3. Adjust `refresh_window_btn_tables_in_mins` / `src_tables_to_skip` in the tracker as needed.
4. Add `job_pipeline_launch.py` as the last task of each upstream job, passing the tables its pipelines refresh.

## Requirements

Unity Catalog, serverless jobs, and for the runner: `SELECT` on `system.access` / `system.lakeflow`, read access to
the source tables and their pipelines / jobs, and permission to run the Jobs / Pipelines being launched.

## Known gaps

- Only pipeline tasks of a job are tracked; tables written by notebook / other tasks are not discovered (by design —
  see *Best suited for SDP jobs*).
- `--src_tables` is passed by the upstream job and must match the tracker's fully qualified names.
- Tables written through pipeline **sinks** have no UC `pipeline_id` and are not tracked as targets.
- A JOB producer's success is job-level and found from lineage (best effort with several writers).
- Discovery depends on lineage: a new pipeline / table appears only after its first run's lineage lands.
- Entity names are the tracker key: renaming a Job / Pipeline orphans its row.
- A pipeline tracked standalone *and* inside a tracked job can be launched twice — track it only via the job.
- One window per entity; per-table windows (e.g. daily vs weekly sources) are not implemented yet —
  use `src_tables_to_skip` for rarely refreshed sources.
