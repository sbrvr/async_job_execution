# Dependency-Aware Job Launching for Databricks SDP

Launch downstream Databricks Jobs / Lakeflow Spark Declarative Pipelines (SDP) only when **all** of their upstream
source tables have been refreshed within a configurable time window — launched asynchronously by the last task of the
upstream job that refreshes them, or checked by a gating task before a downstream job runs.

## Why not table update triggers

Jobs table update triggers only offer **Any table updated** or **All tables updated**:

1. **Mixed cadences (daily + weekly sources).** For a job which relies on source which has varying cadence with some tables getting updated daily and others getting updated weekly (different file arrival time) - its a challenge to set it with any of the option **All tables Updated** or **Any table updated**. The former will make the job wait as there are no updates and later will kick off the job multiple times in a day when weekly files arrive.  Multiple Table triggers are currently not supported for a single job and even if they were supported job would run twice (one for daily and one for weekly) as one trigger cannot be set depend on another trigger(another feature).
2. **Only row-changing writes fire.** A MERGE / UPDATE / DELETE that changes no rows, OPTIMIZE and property changes
   do not fire. An SCD2 (AUTO CDC) update with nothing new writes no table version at all, so with *All tables
   updated* a job that also depends on a quiet SCD2 table **waits forever**.
3. **No time window, no timeout.** "All updated" means *since the last run*, not *recently*; if a source never
   updates, the job silently never runs.
4. **At most 10 tables per trigger.**

## Best suited for SDP jobs

The framework is designed for jobs whose work is done by **SDP pipeline tasks**. Discovery relies on SDP metadata
(pipeline-managed tables, flow progress); only **pipeline tasks** of a job are tracked, other task types are skipped.
It supports two flows:

### Flow 1 — launch from the upstream job (event driven)

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
- Sources from other producers (other jobs / pipelines) still go through the freshness check, so a downstream entity
  launches only when **all** its sources are fresh.

### Flow 2 — scheduled downstream job checks its own sources (polling)

```
downstream job (on a schedule):

  [source_freshness_check.py --entity_name={{job.name}}] ──> [SDP pipeline task] ──> [SDP pipeline task]
          │
          ├── all src_tables refreshed within the window ──> pipeline tasks run
          ├── (--skip_if_already_processed) previous run already processed this data ──> job run cancelled
          └── any src table stale ──> check task fails, pipeline tasks do not run (retry on the next schedule)
```

- The freshness check is the **first task**; the pipeline tasks depend on it, so they run only when every source
  (minus `src_tables_to_skip`) was refreshed within `refresh_window_btn_tables_in_mins`.
- No upstream job needs changing — useful when the upstream jobs are owned by someone else or are not SDP jobs.
- A stale source shows up as a failed check task with the stale tables in its error, rather than a job that silently
  never runs.
- The check alone does not know whether the job already ran on this data: with a window longer than the schedule
  interval, the job runs again on the same data. `--skip_if_already_processed=true --job_run_id={{job.run_id}}`
  cancels the job run (status *Canceled*, not *Failed*) when the previous successful (or still running) run started
  after the newest source refresh. Failed and cancelled runs — including ticks this check cancelled — are passed over.
  Set the job's *no alert for canceled runs* so those ticks do not alert.

#### Failed checks make noise

A stale tick fails the check task, so the pipeline tasks are *upstream failed* and the **whole job run is Failed**. In
Flow 2 that is normal, not an error: a job scheduled every 15 minutes whose silver sources refresh hourly fails about 3
ticks out of 4 by design. Alerting on every failure trains people to ignore the alert that matters — a source stuck
for hours because an upstream pipeline is broken. Instead:

- **Alert on persistence, not on each failure:** a SQL alert or dashboard on `system.lakeflow.job_run_timeline` that
  fires after N consecutive failed checks, or when the job has not succeeded for more than X hours.
- **Use the error text:** it names the stale group, the stale tables and the cutoff (UTC), so an alert can say what
  is late.
- **Match the schedule to the data:** a schedule much more frequent than the upstream refresh mostly produces failed
  ticks.
- **Already-processed ticks:** with `--skip_if_already_processed` they are *Canceled*, not *Failed*; set
  `notification_settings.no_alert_for_canceled_runs` so they stay silent. Stale ticks still fail.

#### Every source group must pass by default

Without `--src_group`, the check reads all the entity's tracker rows and requires **every group** to be fresh within
its own window — for `J_gold`, `daily` (`silver_sales` within 20 min) **and** `weekly` (`bronze_products`,
`bronze_regions` within 7 days). One late group therefore blocks the others: if the weekly file arrives a day late,
every daily tick fails until it lands, even though the daily data is fine. Choose per job:

- **Default (all groups):** gold runs only when every input is fresh — strictest, right when gold is wrong without
  all of them.
- **`--src_group=daily`:** gold runs whenever the daily group is fresh, whatever the weekly group's state — right
  when the weekly data is slow-changing reference data and last week's copy is acceptable.
- **A weekly window a bit longer than the cadence** (e.g. 8 days = 11520 min), so one late delivery does not block
  immediately.
- **`src_tables_to_skip`** for a source that should never gate the job.

Flow 1 does not have this coupling: each group is its own launch rule, so the weekly upstream launching gold does not
depend on the daily group, and vice versa.

## Why scheduling (Flow 2) over launching (Flow 1)

For gold jobs, the scheduled check (Flow 2) is the lower-risk default; use launching (Flow 1) where its lower latency
is worth the extra moving parts.

| | Scheduling — `source_freshness_check.py` | Launching — `job_pipeline_launch.py` |
|---|---|---|
| Side effects | **None**: reads the tracker, Delta history and event logs; passes, fails or cancels its own run | Starts other jobs / pipelines (`run_now`, `start_update`) |
| Upstream jobs | Unchanged | Need a launch task added; their owners must agree |
| Permissions | Read access to the sources and the system tables | Also permission to run every downstream job (*Can Manage Run*) and pipeline (*Can Run*) |
| Blast radius | One job: a bug skips or delays that job's run | A bug can launch many downstream entities, or launch them twice |
| Moving parts | Freshness rule only | Freshness rule, upstream ordering, waits, handoffs, queueing, timeouts |
| Upstream run time | Not affected | The launch task may wait for running upstreams (up to `--queue_timeout_mins`) |
| Latency | Up to one schedule interval after the data is fresh | As soon as the last source is refreshed |
| Non-SDP / external upstreams | Work: only their tables' freshness is read | Need a launch task in a job that refreshes them |
| Visibility | A stale source is a failed check task naming the tables | A missed launch is a status line in another job's launch-task log |

**Trade-offs of scheduling, and how to handle them:**
- **Latency** is bounded by the schedule interval; pick an interval that matches how quickly gold must follow the data.
- **Window vs. interval:** a window much shorter than the interval can miss a refresh; one much longer reruns on the
  same data — add `--skip_if_already_processed=true --job_run_id={{job.run_id}}` to cancel those ticks.
- **No waiting for a running upstream:** a tick can pass on the previous refresh while an upstream is mid-update; the
  next tick picks up the new data (and `--skip_if_already_processed` keeps it to one run per refresh).
- **Alert noise** from stale ticks — see *Failed checks make noise* above.

**When to launch instead:** gold must follow its sources within minutes, you own the upstream jobs, and they are SDP
jobs in one chain.

**Both together:** Flow 1 for latency, with Flow 2 as a safety net on a slower schedule using
`--skip_if_already_processed`: when Flow 1 already launched the job on this data the scheduled tick cancels itself;
when a launch was missed (e.g. a handoff whose upstream job failed) the tick runs it.

## Components

| File | Role |
|---|---|
| `src/tracker_setup.py` | Creates the tracker table (safe to re-run). |
| `src/upstream_source_tracking.py` | For a Job / Pipeline name: finds its pipelines (job → `pipeline_task`s), the tables they currently manage (lineage targets confirmed by UC `pipeline_id`), the **final** tables (not read by another owned table) and the **external sources** (inputs of each owned table's latest write, minus owned tables, UC views expanded via `view_dependencies`). Records each source's producer and MERGEs one tracker row. |
| `src/job_pipeline_launch.py` | **Last task of an upstream job**, `--src_tables` = the tables its pipelines just refreshed. Finds tracker rows (entity + `src_group`) that use any of them (and don't skip them); those tables count as refreshed now, other sources go through the freshness rule. Latest run started after the row's sources were last refreshed (for the passed tables: their pipeline's actual completion, from the event log) → skip; run in progress that started before it (e.g. launched for another `src_group`) → **queue** another; otherwise `run_now` / `start_update`. Entities are launched upstream-first, at most once per task. An entity whose sources are about to be refreshed again — a target of another entity launched in the same task, or its producer pipeline is running (continuous pipelines excluded) — is left `WAITING_ON_UPSTREAM` if that upstream is a job whose own launch task covers the table; otherwise the task **waits** for the upstream run / update, re-checks the sources and then launches it (`UPSTREAM_FAILED` if that run fails). Waits run in parallel under one `--queue_timeout_mins` deadline. |
| `src/source_freshness_check.py` | Optional gating task: fails unless every source (minus skip list) of one entity is fresh — every `src_group` within its own window, or only `--src_group`. First task of a scheduled downstream job. |
| `src/async_job_execution/freshness.py` | Freshness rule shared by the launcher and the check. |
| `src/async_job_execution/common.py` | Spark / SDK helpers, argument parsing, exact name resolution. |

## Tracker table (`<catalog>.<schema>.entity_tracker`)

One row per downstream Job / Pipeline and source group, keyed by `entity_name` + `entity_type` + `src_group`.

| Column | Type | Meaning |
|---|---|---|
| `entity_name` | STRING | Job or Pipeline name |
| `entity_type` | STRING | `JOB` or `PIPELINE` |
| `src_group` | STRING | Source group (default `default`, e.g. `daily` / `weekly`): own `src_tables` and window; each group launches the entity on its own |
| `target_tables` | ARRAY<STRING> | Final tables written by the entity |
| `src_tables` | ARRAY<STRING> | Upstream source tables (views expanded) |
| `src_tables_to_skip` | ARRAY<STRING> | Sources excluded from the check (manual) |
| `src_table_producers` | MAP<STRING, STRUCT<producer_type, producer_id>> | Per source: `PIPELINE` / `JOB` / `NONE` |
| `refresh_window_btn_tables_in_mins` | INT | Freshness window (default 5) |
| `created_on`, `updated_on`, `created_by`, `updated_by` | | Audit |

Example: a gold job with daily and weekly sources, registered as two source groups (from the test cascade in
[Example logs](#example-logs)):

| entity_name | entity_type | src_group | target_tables | src_tables | src_table_producers | refresh_window_btn_tables_in_mins |
|---|---|---|---|---|---|---|
| `J_gold` | `JOB` | `daily` | `[demo.gold_product_sales, demo.gold_regional_sales]` | `[demo.silver_sales]` | `silver_sales → PIPELINE a7a0…` | `20` |
| `J_gold` | `JOB` | `weekly` | `[demo.gold_product_sales, demo.gold_regional_sales]` | `[demo.bronze_products, demo.bronze_regions]` | `bronze_* → PIPELINE 78ef…` | `10080` (7 days) |

Each row is its own launch rule:

- The silver job's launch task (`--src_tables=…silver_sales`) selects only the `daily` row; it launches `J_gold`
  once `silver_sales` is fresh within 20 min. The `weekly` row is not checked.
- The weekly upstream job's launch task (`--src_tables=…bronze_products,…bronze_regions`) selects only the `weekly`
  row; it launches `J_gold` once both are fresh within 7 days.
- If `J_gold` is already running for one group when the other group fires, another run is queued.
- `source_freshness_check.py --entity_name=J_gold` (no `--src_group`) passes only if **both** groups are fresh within
  their own windows; `--src_group=daily` checks the daily row only.

## Freshness rule

A source is **refreshed** at the later of:
1. its **last data write** (`DESCRIBE HISTORY`, data operations only: `WRITE`, `MERGE`, `UPDATE`, `DELETE`,
   `STREAMING UPDATE`, CTAS / RTAS, `COPY INTO`, `TRUNCATE` and `DLT REFRESH` — the truncate step of an SDP full
   refresh), and
2. its **producer's last successful run**, even if no rows changed:
   - `PIPELINE` — latest `COMPLETED` flow for the table: the later of the system pipeline events table
     (`system.lakeflow_pipeline_events_preview.pipeline_events`, Beta, if available — it can lag) and the pipeline's
     `event_log()`; if neither has one, the latest completed pipeline update covering the table;
   - `JOB` — end of the producing job's last successful run;
   - `NONE` — last data write only.

Tables passed to `job_pipeline_launch.py --src_tables` count as refreshed at launch time (the calling job's pipeline
tasks have just finished). A source is fresh if `refreshed_at >= now − refresh_window_btn_tables_in_mins`.

**Timestamps.** All comparisons are in UTC. Spark timestamps (`current_timestamp()`, `DESCRIBE HISTORY`, the pipeline
event log and events table) are read as epoch milliseconds (`unix_millis`) and the Databricks SDK's are epoch
milliseconds or ISO strings with an offset, so the result does not depend on `spark.sql.session.timeZone`. Every
timestamp in the logs is labelled `UTC`.

## Running as job tasks

Each script is a **Python script task** (`spark_python_task`) on serverless compute. Point the task at the file in
this repo (job **Git source**, or a Databricks Git folder); `src/` is the script directory, so the
`async_job_execution` package is importable.

| Script | Parameters |
|---|---|
| `tracker_setup.py` | `--tracker_table` only |
| `upstream_source_tracking.py` | `--entity_type JOB\|PIPELINE --entity_name <name> [--src_group default] [--src_tables <subset>] [--dry_run true] [--max_recursion_depth 5] [--lineage_lookback_days 90]` |
| `job_pipeline_launch.py` | `--src_tables <t1>,<t2>,... [--entity_type ALL\|JOB\|PIPELINE] [--dry_run true\|false] [--queue_timeout_mins 60] [--fail_on_queue_timeout true\|false]` |
| `source_freshness_check.py` | `--entity_type JOB\|PIPELINE --entity_name <name> [--src_group <group>] [--skip_if_already_processed true\|false --job_run_id <run id>]` (as a gating task: `--entity_name={{job.name}} --job_run_id={{job.run_id}}`) |

All scripts require `--tracker_table <catalog>.<schema>.entity_tracker` (the same table for every task). `job_pipeline_launch.py` defaults to
`--dry_run true`; set `false` to launch. Its waits (for a running upstream, or to queue a pipeline update) share one
`--queue_timeout_mins` deadline; a wait that times out is reported as `QUEUE_TIMEOUT` and fails the task unless
`--fail_on_queue_timeout false`. `upstream_source_tracking.py` reads the last `--lineage_lookback_days` of lineage.

`--src_tables` takes fully qualified names (`catalog.schema.table`), comma-separated; they must match the names in
the tracker. What it means depends on the script:

| Script | `--src_tables` | Example `parameters` |
|---|---|---|
| `upstream_source_tracking.py` | Optional. Subset of the entity's **discovered** sources that belong to this `--src_group`; empty = all of them. A table that was not discovered is rejected. | `["--tracker_table=main.ops.entity_tracker", "--entity_type=JOB", "--entity_name=J_gold", "--src_group=daily", "--src_tables=main.demo.silver_sales"]` |
| | | `["--tracker_table=main.ops.entity_tracker", "--entity_type=JOB", "--entity_name=J_gold", "--src_group=weekly", "--src_tables=main.demo.bronze_products,main.demo.bronze_regions"]` |
| `job_pipeline_launch.py` | Required. The tables the **calling** upstream job's pipeline tasks have just refreshed; selects the tracker rows that use any of them. | `["--tracker_table=main.ops.entity_tracker", "--src_tables=main.demo.silver_sales,main.demo.silver_customer_orders", "--dry_run=false"]` |
| `source_freshness_check.py` | Not used: sources come from the entity's tracker rows. | `["--tracker_table=main.ops.entity_tracker", "--entity_name={{job.name}}"]` or, for one group, `[..., "--entity_name={{job.name}}", "--src_group=daily"]`; to skip data already processed: `[..., "--entity_name={{job.name}}", "--skip_if_already_processed=true", "--job_run_id={{job.run_id}}"]` |

The window and skip list are not script parameters; set them in the tracker after registering the rows (re-running
`upstream_source_tracking.py` keeps them):

```sql
UPDATE main.ops.entity_tracker
SET refresh_window_btn_tables_in_mins = 10080
WHERE entity_name = 'J_gold' AND entity_type = 'JOB' AND src_group = 'weekly';
```

Example: launch task added as the last task of an upstream job

```json
{
  "task_key": "launch_downstream",
  "depends_on": [{"task_key": "bronze_pipeline"}, {"task_key": "silver_pipeline"}],
  "environment_key": "default",
  "spark_python_task": {
    "python_file": "src/job_pipeline_launch.py",
    "source": "GIT",
    "parameters": ["--tracker_table=main.ops.entity_tracker",
                   "--src_tables=main.silver.customers,main.silver.orders", "--dry_run=false"]
  }
}
```

with `"git_source": {"git_url": "https://github.com/sbrvr/async_job_execution", "git_provider": "gitHub", "git_branch": "main"}`
and `"environments": [{"environment_key": "default", "spec": {"environment_version": "4"}}]` on the job.

## Example logs

Captured from a test cascade (`J_upstream` → `J_silver` → `J_gold`, plus a standalone pipeline) where the upstream
run had **no new raw data**, so its SCD2 pipeline changed no rows:

| Log | Shows |
|---|---|
| [`examples/1_upstream_job_launch_task.log`](examples/1_upstream_job_launch_task.log) | Last task of the upstream job: full tracker rows, skipped tables, per-source `MET (refreshed by calling upstream job, no rows changed in window)` / `MET (producer refresh)`, last write rows, and the summary — downstream launched with *"met by producer run without row changes"*. |
| [`examples/2_silver_job_launch_task.log`](examples/2_silver_job_launch_task.log) | Last task of the silver job launching the gold job via its `daily` source group. |
| [`examples/3_source_freshness_check.log`](examples/3_source_freshness_check.log) | Gating check of the gold job across its `daily` and `weekly` source groups (each in its own window). |

Each log starts with a `Now:` line (UTC, its epoch milliseconds and the session time zone). Each source line starts
with `MET (<basis>)` or `NOT MET`, where the basis is `data write`, `producer refresh` (materialized view),
`producer run, no rows changed` or `refreshed by calling upstream job, no rows changed in window`, followed by the
comparison that decided it, all in UTC:

```
MET (producer refresh) my_catalog.demo.bronze_products: refreshed 0.0 min ago — refreshed_at 2026-10-12 10:23:10.292 UTC >= cutoff 2026-10-05 10:23:10.292 UTC (now 2026-10-12 10:23:10.292 UTC − window 10080 min)
```

For tables passed in `--src_tables`, a `data refreshed at … UTC` line shows the producer's actual completion, which
is what a downstream run's start is compared with to decide whether it already includes the data.

## Typical setup

1. Run `tracker_setup.py` once.
2. Run `upstream_source_tracking.py` for each downstream Job / Pipeline (re-run after its definition changes).
3. Adjust `refresh_window_btn_tables_in_mins` / `src_tables_to_skip` in the tracker as needed.
4. Add `job_pipeline_launch.py` as the last task of each upstream job, passing the tables its pipelines refresh
   (Flow 1).
5. And / or add `source_freshness_check.py` as the first task of a scheduled downstream job
   (`--entity_name={{job.name}}`), with its pipeline tasks depending on it, so they run only when all its sources are
   fresh (Flow 2). Use this when the upstream jobs can't be changed or are not SDP jobs. Add
   `--skip_if_already_processed=true --job_run_id={{job.run_id}}` so a tick whose data the previous run already
   processed is cancelled instead of reprocessing it.

## Tests

`python -m pytest tests` — unit tests with a fake Databricks client and fake Spark (no workspace needed): freshness
signals and UTC handling, `decide()` / `run_state()`, the launcher's waits, handoffs, timeouts and once-per-entity
rule, and `--skip_if_already_processed`.

## Requirements

Unity Catalog, serverless jobs, and for the runner: `SELECT` on `system.access` / `system.lakeflow`, read access to
the source tables and their pipelines / jobs, and permission to run the Jobs / Pipelines being launched.

## Known gaps

Two kinds: **freshness** gaps decide whether a source is tracked and counts as fresh (they affect both flows), and
**launch** gaps affect how `job_pipeline_launch.py` launches (Flow 1). Most freshness gaps are worked around by
editing the tracker — see [Manual tracker edits](#manual-tracker-edits).

### Freshness gaps

| Gap | Impact | Workaround |
|---|---|---|
| Only **pipeline tasks** of a job are discovered (by design — see *Best suited for SDP jobs*) | Tables written by notebook / SQL / other tasks are missing from `target_tables` / `src_tables` | Add them to the tracker manually ([add a source](#manual-tracker-edits)), with a `JOB` producer so the job's successful run counts |
| Discovery depends on **lineage**, which lands with a delay, and reads only the last `--lineage_lookback_days` (90) | A new pipeline / table is missing until its first run's lineage lands; one not written within the lookback is not found | Re-run `upstream_source_tracking.py` once lineage has landed; raise `--lineage_lookback_days`; or add the row manually |
| Pipeline **sinks** have no UC `pipeline_id` | Tables written through sinks are not tracked as targets | Add them to `target_tables` manually |
| A **JOB producer** is found from lineage (best effort with several writers; lineage lags) | A source may get `NONE` (last data write only) or the wrong job | Set its producer manually ([set a producer](#manual-tracker-edits)) |
| Entity **names** are the tracker key | Renaming a Job / Pipeline orphans its rows | `UPDATE` the rows' `entity_name` ([rename](#manual-tracker-edits)), or re-run `upstream_source_tracking.py` with the new name and delete the old rows |
| `CLONE` / `RESTORE` are not counted as data writes (not typical ETL operations) | A source refreshed only by a clone / restore counts as fresh only if its producer ran within the window | Give it a `JOB` producer (the job doing the clone / restore), or add it to `src_tables_to_skip` |
| If `event_log()` cannot be read, only the system events table is used (about 1 min p99 lag) | A pipeline that has just finished can look stale for about a minute | Give the runner access to the pipelines' event logs, or allow a couple of minutes more in the window |
| One **window per source group** | Sources on different cadences cannot share one row | One row per group (`--src_group daily --src_tables …`, `--src_group weekly --src_tables …`), each with its own `refresh_window_btn_tables_in_mins` |
| Scheduled check: **every source group** must be fresh unless `--src_group` is given | One late group (e.g. weekly) blocks the others | `--src_group`, a weekly window a bit longer than the cadence, or `src_tables_to_skip` — see *Every source group must pass by default* |
| Scheduled check does **not wait for a running upstream** | A tick can pass on the previous refresh while an upstream is mid-update | The next tick picks up the new data; `--skip_if_already_processed` keeps it to one run per refresh |
| `--skip_if_already_processed` looks at the **last 25 runs** for the previous successful one | If none of them succeeded, the tick runs (errs on the side of running) | None needed |

### Launch gaps

| Gap | Impact | Workaround |
|---|---|---|
| `--src_tables` must match the tracker's fully qualified names **and** be tables the calling job's pipelines refreshed (their event-log completion is compared with the downstream's last run) | A wrong name selects nothing; a table that was not refreshed can make the downstream look `ALREADY_HANDLED` | Pass exactly the tables the job's pipeline tasks refresh, as in the tracker; check with `--dry_run true` |
| A job's launch task is recognised by `python_file` ending in `job_pipeline_launch.py` and **literal** `--src_tables` / `--entity_type` / `--dry_run` values | With `{{job.parameters…}}` references the job is waited on instead of handed off to | Use literal values in launch-task parameters |
| A **handoff** (`WAITING_ON_UPSTREAM`) relies on the upstream job's launch task | If that job fails or is cancelled before its launch task runs, neither task launches the entity | Run the scheduled check (Flow 2) with `--skip_if_already_processed` as a safety net, or re-run the upstream job |
| `UPSTREAM_FAILED` (awaited upstream run failed) and `SKIPPED` (stale after the wait) do not fail the launch task | The entity is not launched and the task is green | The upstream's failure shows on its own run; alert on it, or rely on the Flow 2 safety net |
| **Waiting** keeps the launch task running; all waits share one `--queue_timeout_mins` deadline | The upstream job runs longer; a wait that runs out is `QUEUE_TIMEOUT` and fails the task | Tune `--queue_timeout_mins`; `--fail_on_queue_timeout false` to report without failing |
| Within one task an entity is launched **at most once**, even if several groups are fresh | A group that waited for an upstream queues another run only if the earlier one does not include the new data | By design; jobs queue with `run_now` queueing, pipelines start after the running update |
| A **cycle** between tracked entities (each reads the other's targets) | One side launches without waiting | Avoid cycles, or add the back-edge table to that row's `src_tables_to_skip` |
| A pipeline tracked **standalone and inside a tracked job** | It can be launched twice | Track it only via the job: delete its standalone row |
| A handoff to a **running producer** finds its job by listing all active job runs in the workspace | Extra API calls in busy workspaces (only when a producer is running) | None needed |

### Manual tracker edits

Re-running `upstream_source_tracking.py` for an entity **replaces** `target_tables`, `src_tables` and
`src_table_producers` with what it discovers; `src_tables_to_skip` and `refresh_window_btn_tables_in_mins` are kept.
Re-apply manual edits to the replaced columns after a re-run (or stop re-running discovery for that entity).

```sql
-- add a source not discovered (e.g. written by a notebook task of job 123), with a JOB producer
UPDATE main.ops.entity_tracker
SET src_tables = array_distinct(array_append(src_tables, 'main.raw.orders_feed')),
    src_table_producers = map_concat(src_table_producers,
        map('main.raw.orders_feed', named_struct('producer_type', 'JOB', 'producer_id', '123'))),
    updated_on = current_date(), updated_by = current_user()
WHERE entity_name = 'J_gold' AND entity_type = 'JOB' AND src_group = 'daily';

-- set / correct a source's producer (PIPELINE <pipeline_id>, JOB <job_id> or NONE)
UPDATE main.ops.entity_tracker
SET src_table_producers = map_concat(map_filter(src_table_producers, (k, v) -> k != 'main.demo.silver_sales'),
        map('main.demo.silver_sales', named_struct('producer_type', 'JOB', 'producer_id', '456')))
WHERE entity_name = 'J_gold' AND entity_type = 'JOB' AND src_group = 'daily';

-- add a target written through a pipeline sink
UPDATE main.ops.entity_tracker
SET target_tables = array_distinct(array_append(target_tables, 'main.gold.sales_export'))
WHERE entity_name = 'J_gold' AND entity_type = 'JOB';

-- stop a source from gating the entity (kept across re-runs)
UPDATE main.ops.entity_tracker
SET src_tables_to_skip = array_distinct(array_append(coalesce(src_tables_to_skip, array()), 'main.ref.calendar'))
WHERE entity_name = 'J_gold' AND entity_type = 'JOB';

-- after renaming a job
UPDATE main.ops.entity_tracker SET entity_name = 'J_gold_v2'
WHERE entity_name = 'J_gold' AND entity_type = 'JOB';

-- register an entity discovery cannot see at all (e.g. non-SDP job), one row per source group
INSERT INTO main.ops.entity_tracker VALUES (
  'J_report', 'JOB', 'default',
  array('main.gold.report'),                                   -- target_tables
  array('main.gold.sales', 'main.raw.orders_feed'),            -- src_tables
  cast(array() AS array<string>),                              -- src_tables_to_skip
  map('main.gold.sales', named_struct('producer_type', 'PIPELINE', 'producer_id', '<pipeline_id>'),
      'main.raw.orders_feed', named_struct('producer_type', 'JOB', 'producer_id', '123')),
  60,                                                          -- refresh_window_btn_tables_in_mins
  current_date(), current_date(), current_user(), current_user());
```

For a `PIPELINE` source the producer is always re-read from Unity Catalog (the table's `pipeline_id`), so its map
entry only matters for `JOB` / `NONE` sources.
