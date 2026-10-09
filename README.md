# async_job_execution

Launch Databricks Jobs / Lakeflow Declarative Pipelines when **all** of their upstream source tables have been
refreshed within a time window — something Jobs table update triggers cannot express (see [SPEC.md](SPEC.md)).

## Layout

```
src/
  tracker_setup.py             # create the entity tracker table (one row per Job / Pipeline)
  upstream_source_tracking.py  # discover a Job's / Pipeline's target + source tables, upsert its tracker row
  source_freshness_check.py    # fail unless all sources of one Job / Pipeline are fresh (use as a gating task)
  job_pipeline_launch.py       # sweep all tracked entities, launch those whose sources are fresh (schedule it)
  async_job_execution/
    common.py                  # Spark / SDK helpers, argument parsing, exact name resolution
    freshness.py               # freshness rule shared by the check and the launcher
```

## Running as job tasks

Each script is a **Python script task** (`spark_python_task`) on serverless compute. Point the task at the file in
this repo (job **Git source**, or a Databricks Git folder); `src/` is the script directory, so the
`async_job_execution` package is importable. Parameters are command-line arguments:

| Script | Parameters |
|---|---|
| `tracker_setup.py` | `--tracker_table` |
| `upstream_source_tracking.py` | `--entity_type JOB\|PIPELINE --entity_name <name> [--dry_run true] [--max_recursion_depth 5]` |
| `source_freshness_check.py` | `--entity_type JOB\|PIPELINE --entity_name <name>` (as a gating task: `--entity_name={{job.name}}`) |
| `job_pipeline_launch.py` | `[--entity_type ALL\|JOB\|PIPELINE] [--dry_run true\|false]` |

All scripts accept `--tracker_table` (default `surajb.common.entity_tracker`).

Example task (Jobs API / UI JSON):

```json
{
  "task_key": "job_pipeline_launch",
  "environment_key": "default",
  "spark_python_task": {
    "python_file": "src/job_pipeline_launch.py",
    "source": "GIT",
    "parameters": ["--entity_type=ALL", "--dry_run=false"]
  }
}
```

with `"git_source": {"git_url": "https://github.com/sbrvr/async_job_execution", "git_provider": "gitHub", "git_branch": "main"}`
and `"environments": [{"environment_key": "default", "spec": {"environment_version": "4"}}]` on the job.

## Typical setup

1. Run `tracker_setup.py` once.
2. Run `upstream_source_tracking.py` for each Job / Pipeline to orchestrate (re-run after its definition changes).
3. Adjust `refresh_window_btn_tables_in_mins` / `src_tables_to_skip` in the tracker as needed.
4. Either schedule `job_pipeline_launch.py` (e.g. every 5 minutes), or add `source_freshness_check.py` as the first
   task of a scheduled job.

## Requirements

Unity Catalog, serverless jobs, and for the runner: `SELECT` on `system.access` / `system.lakeflow`, read access to
the source tables and their pipelines / jobs, and permission to run the Jobs / Pipelines being launched.
