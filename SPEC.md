# async_job_execution — Spec

## Goal
Run a downstream Job / Pipeline only when **all** of its upstream source tables have been refreshed within a
configurable time window — launched asynchronously by the last task of the upstream job that refreshes them, or
checked by a gating task before the downstream job runs.

**Scope: SDP jobs.** The framework targets upstream jobs whose work is done by Lakeflow Spark Declarative Pipeline
(SDP) tasks. `job_pipeline_launch.py` runs as the job's last task, after every pipeline task has **finished**, so the
tables those pipelines refresh are complete when downstream entities are checked and launched — including pipeline
updates that changed no rows. Discovery also relies on SDP metadata (pipeline-managed tables, flow progress);
non-pipeline tasks are not tracked.

## Why not table update triggers
Jobs table update triggers only offer **Any table updated** or **All tables updated**:

1. **Mixed cadences (daily + weekly sources).** *All* over everything runs at most weekly; *All* over the daily
   tables never checks the weekly ones; *Any* fires on every source update. Multiple triggers on one job are
   **OR-ed** (each fires on its own), so splitting into a daily and a weekly trigger does not give "all of them".
2. **At most 10 tables per trigger.**
3. **Only row-changing writes fire.** A MERGE / UPDATE / DELETE that changes no rows, OPTIMIZE and property changes
   do not fire. An SCD2 (AUTO CDC) update with nothing new writes no table version at all, so with *All tables
   updated* a job that also depends on a quiet SCD2 table **waits forever**.
4. **…while no-op re-sends can fire.** AUTO CDC stores every incoming record; re-sending an unchanged record in a
   later update hides the old physical row and inserts the new one (1 update + 1 insert in storage) although the
   visible history is unchanged — downstream triggers fire.
5. **No time window, no timeout.** "All updated" means *since the last run*, not *recently*; if a source never
   updates the job silently never runs.

## Components
| Script | Role |
|---|---|
| `tracker_setup.py` | Creates the tracker table. |
| `upstream_source_tracking.py` | For a Job / Pipeline name: finds its pipelines (job → `pipeline_task`s; other task types skipped), the tables they currently manage (lineage targets confirmed by UC `pipeline_id`), the **final** tables (not read by another owned table) and the **external sources** (inputs of each owned table's latest write, minus owned tables, UC views expanded via `view_dependencies`). Records each source's producer. MERGEs one tracker row. |
| `source_freshness_check.py` | Optional gating task: fails unless every source (minus skip list) of one entity is fresh — first task of a scheduled downstream job. |
| `job_pipeline_launch.py` | **Last task of an upstream job**, `--src_tables` = the tables its pipelines just refreshed. Finds tracked entities that use any of them (and don't skip them); those tables count as refreshed now, other sources go through the freshness rule; then exact name resolution → skip if running or already started after the newest source refresh → `run_now` / `start_update`. |
| `async_job_execution/freshness.py` | Freshness rule shared by the check and the launcher. |

## Tracker table (`surajb.common.entity_tracker`)
One row per Job / Pipeline, keyed by `entity_name` + `entity_type`.

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

## Known gaps
- Only pipeline tasks of a job are tracked; tables written by notebook / other tasks are not discovered (by design —
  see Scope).
- `--src_tables` is passed by the upstream job and must match the tracker's fully qualified names.
- Tables written through pipeline **sinks** have no UC `pipeline_id` and are not tracked as targets.
- A JOB producer's success is job-level and found from lineage (best effort with several writers).
- Discovery depends on lineage: a new pipeline / table appears only after its first run's lineage lands.
- Entity names are the tracker key: renaming a Job / Pipeline orphans its row.
- A pipeline tracked standalone *and* inside a tracked job can be launched twice — track it only via the job.
- One window per entity; per-table windows (e.g. daily vs weekly sources) are not implemented yet —
  use `src_tables_to_skip` for rarely refreshed sources.
