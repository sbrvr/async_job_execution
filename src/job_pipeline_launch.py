"""Launch the downstream Jobs / Pipelines of tables an upstream job has just refreshed, once all their sources are fresh.

Run as the LAST task of an upstream job (depends on all its other tasks), with --src_tables = the tables that job
refreshes (one or more, comma-separated):
  0. Candidates: tracked entities whose src_tables include any of them (and not in src_tables_to_skip). The given
     tables count as refreshed now — the job's earlier tasks just processed them, even if no rows changed.
For each candidate (optionally filtered by --entity_type):
  1. Freshness: every src table not in src_tables_to_skip refreshed within refresh_window_btn_tables_in_mins.
  2. Resolve entity_name to exactly one Job / Pipeline.
  3. Each row (entity + src_group) is its own launch rule. If the entity's latest run / update started after the
     row's sources were last refreshed it already includes them (skip). If a run is in progress but started before
     — e.g. launched for a different src_group — queue another; otherwise launch. An entity is launched at most once
     per task even if several of its groups are fresh.
  4. Sources about to be refreshed again — a target of another entity launched in this task, or its (triggered)
     producer pipeline is running:
       - if that upstream is a job whose own job_pipeline_launch.py task covers the table, leave the entity to it
         (WAITING_ON_UPSTREAM);
       - otherwise wait here for the upstream run / update to finish, re-check the sources, then launch — so the
         entity runs once, on the new data. Not launched if the upstream run fails (UPSTREAM_FAILED).
  5. Launch: JOB -> jobs.run_now (queueing enabled), PIPELINE -> pipelines.start_update (to queue, wait for the
     running update to finish first). Logged only with --dry_run true.
Entities are launched in upstream-first order; waits run in parallel and share one deadline (--queue_timeout_mins).
Fails if any entity could not be resolved or launched, or (--fail_on_queue_timeout true) a wait timed out.
UPSTREAM_FAILED (the awaited upstream run failed — that run already shows as failed) and SKIPPED (stale sources) are
reported but do not fail the task.
"""
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from databricks.sdk.service.jobs import QueueSettings

from async_job_execution.common import (ENTITY_TYPES, base_parser, get_spark, get_workspace_client, log_tracker_row,
                                        resolve_entity, str_to_bool)
from async_job_execution.freshness import ACTIVE_PIPELINE_STATES, FINAL_UPDATE_STATES, FreshnessChecker, utc

POLL_SECS = 30
ACTIVE_TASK_STATES = {"PENDING", "QUEUED", "RUNNING", "BLOCKED", "WAITING_FOR_RETRY"}
FINAL_RUN_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
LAUNCH_SCRIPT = "job_pipeline_launch.py"
# statuses of an upstream row that mean its tables are not being refreshed by this task
NOT_REFRESHED = {"FAILED", "NOT_FOUND", "QUEUE_TIMEOUT", "UPSTREAM_FAILED", "SKIPPED"}


def parse_args():
    parser = base_parser("Launch downstream Jobs / Pipelines whose upstream sources are fresh")
    parser.add_argument("--src_tables", required=True,
                        help="Comma-separated fully qualified tables the calling upstream job has just refreshed")
    parser.add_argument("--entity_type", default="ALL", choices=("ALL",) + ENTITY_TYPES)
    parser.add_argument("--dry_run", default="true", help="true = check and log only, launch nothing")
    parser.add_argument("--queue_timeout_mins", type=int, default=60,
                        help="How long to wait, in total, for running upstream runs / pipeline updates to finish")
    parser.add_argument("--fail_on_queue_timeout", default="true",
                        help="true = fail the task if a wait times out; false = report QUEUE_TIMEOUT only")
    return parser.parse_args()


class QueueTimeout(Exception):
    pass


@dataclass
class Plan:
    """A tracker row that passed the checks, to be launched."""
    etype: str
    name: str
    group: str
    entity_id: str
    action: str  # LAUNCH | QUEUE
    reason: str
    tables: list
    window: int
    producers: dict
    # [((entity_type, name), entity_id, shared tables, [Futures of its rows])] of upstreams launched in this task
    upstreams: list = field(default_factory=list)


@dataclass
class Result:
    etype: str
    name: str
    group: str
    status: str
    detail: str
    ref: Optional[tuple] = None  # ("JOB", run_id) | ("PIPELINE", (pipeline_id, update_id)) of the launched run


def _utc(epoch_ms):
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).replace(tzinfo=None)


def run_state(w, entity_type, entity_id):
    """Return (active, latest_start, latest_id) of the entity's most recent run / update."""
    if entity_type == "JOB":
        # list_runs pages through the whole run history: read only the first result of each listing
        latest = next(iter(w.jobs.list_runs(job_id=int(entity_id), limit=1)), None)
        active = next(iter(w.jobs.list_runs(job_id=int(entity_id), active_only=True, limit=1)), None) is not None
        if not latest or not latest.start_time:
            return active, None, None
        return active, _utc(latest.start_time), latest.run_id
    p = w.pipelines.get(pipeline_id=entity_id)
    active = bool(p.state and p.state.value in ACTIVE_PIPELINE_STATES)
    latest = (p.latest_updates or [None])[0]
    if not latest or not latest.creation_time:
        return active, None, None
    started = datetime.fromisoformat(latest.creation_time.replace("Z", "+00:00"))
    return active, started.astimezone(timezone.utc).replace(tzinfo=None), latest.update_id


def decide(w, entity_type, entity_id, data_refreshed_at):
    """LAUNCH, QUEUE (a run is in progress but started before this group's sources were refreshed, so it cannot
    include them) or (SKIP, reason) when the latest run started after the refresh and already includes it."""
    active, latest_start, latest_id = run_state(w, entity_type, entity_id)
    if latest_start and latest_start >= data_refreshed_at:
        state = "in progress" if active else "finished"
        return "SKIP", (f"run {latest_id} ({state}) started {utc(latest_start)}, after the newest source refresh "
                        f"{utc(data_refreshed_at)}")
    if active:
        return "QUEUE", (f"run {latest_id} in progress started {utc(latest_start)}, before the newest source refresh "
                         f"{utc(data_refreshed_at)}")
    return "LAUNCH", ""


def wait_for(w, ref, deadline):
    """Wait for a job run / pipeline update to finish -> its final state; QueueTimeout at the deadline."""
    kind, run = ref
    while True:
        if kind == "JOB":
            state = w.jobs.get_run(run_id=run).state
            if state and state.life_cycle_state and state.life_cycle_state.value in FINAL_RUN_STATES:
                return state.result_state.value if state.result_state else state.life_cycle_state.value
        else:
            update = w.pipelines.get_update(pipeline_id=run[0], update_id=run[1]).update
            if update and update.state and update.state.value in FINAL_UPDATE_STATES:
                return update.state.value
        if time.time() > deadline:
            raise QueueTimeout(f"{kind.lower()} {run} still running at the deadline")
        time.sleep(POLL_SECS)


def launch(w, entity_type, entity_id, name, queue, deadline):
    """Launch / queue the entity -> (detail, ref of the new run)."""
    if entity_type == "JOB":
        # queue=True: if the job is at max_concurrent_runs, the run waits instead of being skipped
        run = w.jobs.run_now(job_id=int(entity_id), queue=QueueSettings(enabled=True))
        return (f"{'queued' if queue else 'launched'} job '{name}' (job_id={entity_id}, run_id={run.run_id})",
                ("JOB", run.run_id))
    if queue:  # pipelines cannot queue an update: wait for the running one to finish, then start
        _, _, running = run_state(w, entity_type, entity_id)
        if running:
            wait_for(w, ("PIPELINE", (entity_id, running)), deadline)
    update = w.pipelines.start_update(pipeline_id=entity_id)
    return (f"{'queued (after running update) ' if queue else ''}started pipeline '{name}' "
            f"(pipeline_id={entity_id}, update_id={update.update_id})", ("PIPELINE", (entity_id, update.update_id)))


class Handoff:
    """Whether a job's own launch task will launch the downstream entities of a table (cached per job / pipeline)."""

    def __init__(self, w):
        self.w = w
        self._tables, self._job_of_pipeline = {}, {}

    def launch_task_tables(self, job_id, entity_type):
        """Tables passed to --src_tables by the job's job_pipeline_launch.py task(s) that would launch an entity of
        `entity_type`: --dry_run false and --entity_type ALL or that type. Empty if it has no such task."""
        if job_id not in self._tables:
            tasks = []  # (tables, entity_type, dry_run) per launch task
            for t in self.w.jobs.get(job_id=int(job_id)).settings.tasks or []:
                task = t.spark_python_task
                if not task or not (task.python_file or "").endswith(LAUNCH_SCRIPT):
                    continue
                params = task.parameters or []
                values = {}
                for i, p in enumerate(params):
                    for name in ("--src_tables", "--entity_type", "--dry_run"):
                        if p.startswith(name + "="):
                            values[name] = p.split("=", 1)[1]
                        elif p == name and i + 1 < len(params):
                            values[name] = params[i + 1]
                tables = {x.strip() for x in values.get("--src_tables", "").replace(" ", ",").split(",") if x.strip()}
                tasks.append((tables, values.get("--entity_type", "ALL").upper(),
                              str_to_bool(values.get("--dry_run", "true"))))  # the launch task defaults to a dry run
            self._tables[job_id] = tasks
        return set().union(*[tables for tables, etype, dry_run in self._tables[job_id]
                             if not dry_run and etype in ("ALL", entity_type)])

    def job_running_pipeline(self, pipeline_id):
        """Job whose active run has a running task for the pipeline, else None (all active runs in the workspace)."""
        if pipeline_id not in self._job_of_pipeline:
            self._job_of_pipeline[pipeline_id] = next(
                (run.job_id for run in self.w.jobs.list_runs(active_only=True, expand_tasks=True)
                 for t in run.tasks or []
                 if t.pipeline_task and t.pipeline_task.pipeline_id == pipeline_id and t.state
                 and t.state.life_cycle_state and t.state.life_cycle_state.value in ACTIVE_TASK_STATES), None)
        return self._job_of_pipeline[pipeline_id]


def upstream_first(planned, targets_of):
    """Order the plans so an entity comes after the entities (also launched here) that produce its sources.
    Returns (ordered plans, {entity: [(upstream entity, shared tables)]}); edges of a cycle are dropped."""
    entities = list(dict.fromkeys((p.etype, p.name) for p in planned))
    src_of = {e: set().union(*(set(p.tables) for p in planned if (p.etype, p.name) == e)) for e in entities}
    deps = {e: {o: src_of[e] & targets_of.get(o, set()) for o in entities if o != e} for e in entities}
    deps = {e: {o: shared for o, shared in d.items() if shared} for e, d in deps.items()}
    order, remaining = [], list(entities)
    while remaining:
        ready = [e for e in remaining if not set(deps[e]) - set(order)] or remaining[:1]  # cycle: take the first
        order.append(ready[0])
        remaining.remove(ready[0])
    upstreams = {e: [(o, deps[e][o]) for o in order[:order.index(e)] if o in deps[e]] for e in order}
    return sorted(planned, key=lambda p: order.index((p.etype, p.name))), upstreams


class Launcher:
    """Launches the plans; one thread per plan so waits run in parallel under one deadline."""

    def __init__(self, spark, w, refreshed, dry_run, deadline):
        self.spark, self.w, self.refreshed, self.dry_run, self.deadline = spark, w, set(refreshed), dry_run, deadline
        self.handoff = Handoff(w)
        self.launched = {}  # (entity_type, entity_id) -> src_group that launched / queued it in this task
        self.locks = defaultdict(threading.Lock)  # per entity: its groups launch it once; other entities in parallel
        self.locks_lock = threading.Lock()

    def result(self, p, status, detail, ref=None):
        return Result(p.etype, p.name, p.group, status, detail, ref)

    def run(self, p: Plan) -> Result:
        try:
            return self._run(p)
        except QueueTimeout as e:
            return self.result(p, "QUEUE_TIMEOUT", f"{e} (--queue_timeout_mins)")
        except Exception as e:
            return self.result(p, "FAILED", f"launch failed: {e}")

    def _run(self, p: Plan) -> Result:
        waits, covered, notes = [], set(), []  # (what, ref) to wait for; tables whose refresh is awaited
        # 1. upstream entities launched in this task that produce some of this row's sources
        for (uetype, uname), uid, shared, futures in p.upstreams:
            urows = [f.result() for f in futures]  # their launch decision (they are earlier in the order)
            ref = next((r.ref for r in urows if r.ref), None)
            would_launch = any(r.status in ("DRY_RUN_LAUNCH", "DRY_RUN_QUEUE") for r in urows)
            hands_off = uetype == "JOB" and bool(shared & self.handoff.launch_task_tables(uid, p.etype))
            if ref is None and not would_launch:
                if any(r.status == "WAITING_ON_UPSTREAM" for r in urows):
                    # the upstream is refreshed later, by its own upstream's launch task
                    if hands_off:  # ... and its own launch task then launches this entity
                        return self.result(p, "WAITING_ON_UPSTREAM", f"sources {sorted(shared)} are refreshed later "
                                           f"by job '{uname}' (itself waiting on its upstream); its launch task "
                                           f"launches this entity")
                    notes.append(f"{uetype} '{uname}' (refreshes {sorted(shared)}) is waiting on its upstream and "
                                 f"has no launch task for this entity: launched on its current data")
                not_run = sorted({r.status for r in urows} & NOT_REFRESHED)
                if not_run:
                    notes.append(f"{uetype} '{uname}' (refreshes {sorted(shared)}) was not launched here ({', '.join(not_run)}): "
                                 f"launched on its current data")
                continue  # not launched in this task: its current data counts (a run in progress is caught in step 2)
            if hands_off:
                return self.result(p, "WAITING_ON_UPSTREAM", f"sources {sorted(shared)} are being refreshed by job "
                                   f"'{uname}' launched in this task; its launch task launches this entity")
            waits.append((f"{uetype} '{uname}' (refreshes {sorted(shared)})", ref))
            covered |= shared
        # 2. sources whose (triggered) producer pipeline is running now
        checker = FreshnessChecker(self.spark, self.w)
        for t in sorted(set(p.tables) - covered):
            running = checker.producer_update(t)
            if not running:
                continue
            pipeline_id, update_id, cause = running
            ref = ("PIPELINE", (pipeline_id, update_id))
            if any(r == ref for _, r in waits):
                continue
            job_id = self.handoff.job_running_pipeline(pipeline_id) if cause == "JOB_TASK" else None
            if job_id and t in self.handoff.launch_task_tables(job_id, p.etype):
                return self.result(p, "WAITING_ON_UPSTREAM", f"source {t} is being refreshed by job {job_id}, whose "
                                   f"launch task launches this entity")
            waits.append((f"pipeline {pipeline_id} update {update_id} (running, refreshes {t})", ref))

        action, reason = p.action, "; ".join(x for x in [p.reason] + notes if x)
        if self.dry_run:
            what = f"would {action.lower()} {p.etype} id={p.entity_id}"
            if waits:
                what = (f"would wait for {'; '.join(w_ for w_, _ in waits)}, re-check the sources, then "
                        f"{action.lower()} {p.etype} id={p.entity_id}")
            return self._once(p, lambda: self.result(p, f"DRY_RUN_{action}", what + (f" ({reason})" if reason else "")))

        # 3. wait for the upstream runs, then re-check the sources on their new data
        for what, ref in waits:
            print(f"[{p.name} / {p.group}] {utc(_utc(time.time() * 1000))} waiting for {what}")
            state = wait_for(self.w, ref, self.deadline)
            if state not in ("SUCCESS", "COMPLETED"):
                return self.result(p, "UPSTREAM_FAILED", f"{what} finished {state}; not launched")

        def go():
            action_, reason_ = action, reason
            if waits:  # re-check on the upstreams' new data; under the entity lock, so another group's run counts
                now = int(time.time() * 1000)
                _, statuses = FreshnessChecker(self.spark, self.w).check(p.tables, p.window, now, p.producers,
                                                                         refreshed_now=self.refreshed)
                stale = [st.table for st in statuses if not st.is_fresh]
                if stale:
                    return self.result(p, "SKIPPED", f"stale sources after waiting for the upstream runs: {stale}")
                action_, reason_ = decide(self.w, p.etype, p.entity_id, max(st.data_refreshed_at for st in statuses))
                if action_ == "SKIP":
                    return self.result(p, "ALREADY_HANDLED", reason_)
                reason_ = "; ".join(x for x in [f"after {'; '.join(w_ for w_, _ in waits)}", reason_] + notes if x)
            detail, ref = launch(self.w, p.etype, p.entity_id, p.name, action_ == "QUEUE", self.deadline)
            return self.result(p, "QUEUED" if action_ == "QUEUE" else "LAUNCHED",
                               detail + (f" — {reason_}" if reason_ else ""), ref)
        # after a wait, decide() in go() tells whether a run launched here for another group includes the new data
        return self._once(p, go, dedupe=not waits)

    def _once(self, p, launch_fn, dedupe=True):
        """Launch the entity at most once per task, even if several of its groups get here."""
        key = (p.etype, p.entity_id)
        with self.locks_lock:
            lock = self.locks[key]
        with lock:
            if dedupe and key in self.launched:
                return self.result(p, "ALREADY_HANDLED", f"covered by the run launched for src_group '{self.launched[key]}'")
            result = launch_fn()
            if result.ref or result.status.startswith("DRY_RUN_"):
                self.launched[key] = p.group
            return result


def launch_all(launcher, planned, targets_of):
    """Launch upstream-first, one thread per row: a row whose sources are about to be refreshed again (a target of
    another entity launched here, or its producer pipeline is running) waits for that run instead of launching on old
    data, unless the upstream job's own launch task will launch it."""
    planned, upstreams = upstream_first(planned, targets_of)
    entity_ids = {(p.etype, p.name): p.entity_id for p in planned}
    futures = {}  # entity -> Futures of its rows
    with ThreadPoolExecutor(max_workers=max(len(planned), 1)) as pool:
        submitted = []
        for p in planned:
            entity = (p.etype, p.name)
            p.upstreams = [(u, entity_ids[u], shared & set(p.tables), list(futures.get(u, [])))
                           for u, shared in upstreams[entity] if shared & set(p.tables)]  # this row's sources only
            future = pool.submit(launcher.run, p)
            futures.setdefault(entity, []).append(future)
            submitted.append(future)
        return [f.result() for f in submitted]


def main():
    args = parse_args()
    dry_run, fail_on_timeout = str_to_bool(args.dry_run), str_to_bool(args.fail_on_queue_timeout)
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
    now = spark.sql("SELECT unix_millis(current_timestamp()) AS ts").collect()[0]["ts"]  # epoch ms: no session time zone
    print(f"Now: {utc(_utc(now))} (epoch ms {now}); all times below are UTC, independent of the session time zone "
          f"({spark.conf.get('spark.sql.session.timeZone')})")
    results = []    # Result per row
    planned = []    # Plan per row that passed the checks

    for r in rows:  # each row (entity + source group) is its own launch rule
        log_tracker_row(r)
        etype, name, group = (r["entity_type"] or "").upper(), r["entity_name"], r["src_group"] or "default"
        skip = set(r["src_tables_to_skip"] or [])
        tables = [t for t in (r["src_tables"] or []) if t not in skip]
        window = r["refresh_window_btn_tables_in_mins"]
        if etype not in ENTITY_TYPES:
            results.append(Result(etype, name, group, "SKIPPED", f"unknown entity_type '{etype}'"))
            continue
        if window is None or not tables:
            results.append(Result(etype, name, group, "SKIPPED", "refresh window is NULL or no source tables to check"))
            continue

        producers = {k: v.asDict() for k, v in (r["src_table_producers"] or {}).items()}
        cutoff, statuses = checker.check(tables, window, now, producers, refreshed_now=set(refreshed))
        print(f"    -> checking {len(tables)} src_table(s) against refresh_window_btn_tables_in_mins={window}:")
        for st in statuses:
            print(f"       {st.describe()}")
        stale = [st.table for st in statuses if not st.is_fresh]
        if stale:
            results.append(Result(etype, name, group, "SKIPPED", f"stale sources: {stale}"))
            continue

        entity_id, error = resolve_entity(w, etype, name)
        if error:
            results.append(Result(etype, name, group, "NOT_FOUND", error))
            continue
        action, reason = decide(w, etype, entity_id, max(st.data_refreshed_at for st in statuses))
        if action == "SKIP":
            results.append(Result(etype, name, group, "ALREADY_HANDLED", reason))
            continue
        no_change = [st.table for st in statuses if st.met_without_row_changes]
        if no_change:
            reason = (reason + "; " if reason else "") + f"met by producer run without row changes: {no_change}"
        planned.append(Plan(etype, name, group, entity_id, action, reason, tables, window, producers))

    targets_of = {(r["entity_type"].upper(), r["entity_name"]): set(r["target_tables"] or []) for r in rows}
    launcher = Launcher(spark, w, refreshed, dry_run, deadline=time.time() + args.queue_timeout_mins * 60)
    results += launch_all(launcher, planned, targets_of)

    print("\nSummary:")
    for r in results:
        print(f"  {r.status:<19} {r.etype:<8} {r.name} [{r.group}]: {r.detail}")
    failed = {"FAILED", "NOT_FOUND"} | ({"QUEUE_TIMEOUT"} if fail_on_timeout else set())
    if any(r.status in failed for r in results):
        raise Exception("One or more entities could not be launched — see summary above.")


if __name__ == "__main__":
    main()
