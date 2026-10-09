# async_job_execution

Launch downstream Databricks Jobs / Lakeflow Spark Declarative Pipelines (SDP) when **all** of their upstream source
tables have been refreshed within a time window — something Jobs table update triggers cannot express
(see [SPEC.md](SPEC.md)).

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
- Discovery (`upstream_source_tracking.py`) also tracks **pipeline tasks** only: tables, lineage and freshness come
  from SDP pipeline metadata (managed tables, flow progress). Other task types are skipped.
- Sources from other producers (other jobs / pipelines) still go through the freshness check, so a downstream entity
  launches only when **all** its sources are fresh.

## Layout

```
src/
  tracker_setup.py             # create the entity tracker table (one row per Job / Pipeline)
  upstream_source_tracking.py  # discover a Job's / Pipeline's target + source tables, upsert its tracker row
  job_pipeline_launch.py       # last task of an upstream job: launch downstream entities whose sources are fresh
  source_freshness_check.py    # optional gating task: fail unless all sources of one Job / Pipeline are fresh
  async_job_execution/
    common.py                  # Spark / SDK helpers, argument parsing, exact name resolution
    freshness.py               # freshness rule shared by the launcher and the check
```

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
