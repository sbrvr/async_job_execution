import threading
import time
from datetime import datetime
from types import SimpleNamespace as NS

import pytest
from conftest import enum

import job_pipeline_launch as jpl

OLD = datetime(2026, 1, 1)


# ---- run_state / decide with the real code -------------------------------------------------------------------------
class RunsW:
    """jobs.list_runs: `runs` newest first; raises if more than one page would be read."""

    def __init__(self, runs, active=(), pipeline=None):
        self.runs, self.active, self.pipeline = runs, list(active), pipeline
        self.jobs = NS(list_runs=self.list_runs)
        self.pipelines = NS(get=lambda pipeline_id: self.pipeline)

    def list_runs(self, job_id, limit, active_only=False):
        source = self.active if active_only else self.runs
        for i, r in enumerate(source):
            if i >= limit:
                raise AssertionError("read past the first page")
            yield r


def ms(dt):
    return int(dt.replace(tzinfo=__import__("datetime").timezone.utc).timestamp() * 1000)


T_RUN, T_DATA = datetime(2026, 10, 12, 1, 0), datetime(2026, 10, 12, 2, 0)


def test_decide_skip_when_latest_run_started_after_refresh():
    w = RunsW([NS(run_id=7, start_time=ms(T_DATA))])
    action, reason = jpl.decide(w, "JOB", "1", T_RUN)
    assert action == "SKIP" and "run 7" in reason and "UTC" in reason


def test_decide_queue_when_active_run_started_before_refresh():
    run = NS(run_id=7, start_time=ms(T_RUN))
    assert jpl.decide(RunsW([run], active=[run]), "JOB", "1", T_DATA)[0] == "QUEUE"


def test_decide_launch_when_idle_and_older():
    assert jpl.decide(RunsW([NS(run_id=7, start_time=ms(T_RUN))]), "JOB", "1", T_DATA) == ("LAUNCH", "")


def test_decide_launch_when_never_ran():
    assert jpl.decide(RunsW([]), "JOB", "1", T_DATA) == ("LAUNCH", "")


def test_run_state_reads_one_run_per_listing():
    runs = [NS(run_id=i, start_time=ms(T_RUN)) for i in range(50)]  # list_runs raises past the first page
    assert jpl.run_state(RunsW(runs), "JOB", "1") == (False, T_RUN, 0)


def test_run_state_pipeline_offset_converted_to_utc():
    p = NS(state=enum("IDLE"), latest_updates=[NS(creation_time="2026-10-12T07:30:00+05:30", update_id="u1")])
    assert jpl.run_state(RunsW([], pipeline=p), "PIPELINE", "p") == (False, datetime(2026, 10, 12, 2, 0), "u1")


# ---- launcher: waits, handoff, timeouts ----------------------------------------------------------------------------
class FakeW:
    """Runs / updates finish after `duration` seconds with `outcome`."""

    def __init__(self, duration=0.2, outcome="SUCCESS", launch_jobs=None, running=None):
        self.duration, self.outcome = duration, outcome
        self.launch_jobs = launch_jobs or {}   # job_id -> launch task parameters
        self.running = running or {}           # pipeline_id -> (update_id, job_id or None, finishes_at)
        self.started, self.lock, self.n, self.launches = {}, threading.Lock(), 0, []
        self.jobs = NS(run_now=self.run_now, get_run=self.get_run, get=self.get_job, list_runs=self.list_runs)
        self.pipelines = NS(start_update=self.start_update, get_update=self.get_update)

    def _new(self, what):
        with self.lock:
            self.n += 1
            self.started[self.n] = time.time()
            self.launches.append((what, time.time()))
            return self.n

    def run_now(self, job_id, queue):
        return NS(run_id=self._new(("JOB", job_id)))

    def start_update(self, pipeline_id):
        return NS(update_id=self._new(("PIPELINE", pipeline_id)))

    def get_run(self, run_id):
        done = time.time() - self.started[run_id] >= self.duration
        return NS(state=NS(life_cycle_state=enum("TERMINATED" if done else "RUNNING"),
                           result_state=enum(self.outcome) if done else None))

    def get_update(self, pipeline_id, update_id):
        if pipeline_id in self.running and self.running[pipeline_id][0] == update_id:
            done = time.time() >= self.running[pipeline_id][2]
        else:
            done = time.time() - self.started[update_id] >= self.duration
        return NS(update=NS(state=enum(("COMPLETED" if self.outcome == "SUCCESS" else "FAILED") if done else "RUNNING")))

    def get_job(self, job_id):
        params = self.launch_jobs.get(str(job_id))
        task = NS(spark_python_task=NS(python_file="src/job_pipeline_launch.py", parameters=params)) if params \
            else NS(spark_python_task=None)
        return NS(settings=NS(tasks=[task]))

    def list_runs(self, active_only=False, expand_tasks=False, **_):
        return [NS(job_id=jid, tasks=[NS(pipeline_task=NS(pipeline_id=pid), state=NS(life_cycle_state=enum("RUNNING")))])
                for pid, (_, jid, _) in self.running.items() if jid]


def launch_params(tables, entity_type="ALL", dry_run="false"):
    return ["--tracker_table=x", "--src_tables=" + ",".join(tables), f"--entity_type={entity_type}",
            f"--dry_run={dry_run}"]


@pytest.fixture
def fake_checker(monkeypatch):
    """Sources fresh, data refreshed long ago; `stale_after_wait` / `decide_after_wait` steer the post-wait re-check."""
    state = NS(stale_after_wait=False, decide=lambda w, etype, eid, refreshed: ("LAUNCH", ""))

    class FakeChecker:
        def __init__(self, spark, w):
            self.w = w

        def producer_update(self, table):
            for pid, (uid, jid, finishes) in self.w.running.items():
                if table.startswith(pid + ".") and time.time() < finishes:
                    return pid, uid, "JOB_TASK" if jid else "API_CALL"
            return None

        def check(self, tables, window, now, producers, refreshed_now=()):
            return None, [NS(table=t, is_fresh=not state.stale_after_wait, data_refreshed_at=OLD) for t in tables]

    monkeypatch.setattr(jpl, "FreshnessChecker", FakeChecker)
    monkeypatch.setattr(jpl, "decide", lambda *a: state.decide(*a))
    monkeypatch.setattr(jpl, "POLL_SECS", 0.01)
    return state


def plan(etype, name, eid, tables, group="default"):
    return jpl.Plan(etype, name, group, eid, "LAUNCH", "", tables, 20, {})


def run(w, planned, targets_of, dry_run=False, timeout=5):
    launcher = jpl.Launcher(None, w, [], dry_run, deadline=time.time() + timeout)
    return {(r.name, r.group): r for r in jpl.launch_all(launcher, planned, targets_of)}


def launched(w, what):
    return [t for x, t in w.launches if x == what]


def test_waits_for_upstream_pipeline_launched_here(fake_checker):
    w = FakeW()
    res = run(w, [plan("JOB", "B", "2", ["u.t1"]), plan("PIPELINE", "U", "u", ["x.src"])], {("PIPELINE", "U"): {"u.t1"}})
    assert res[("U", "default")].status == res[("B", "default")].status == "LAUNCHED"
    assert launched(w, ("JOB", 2))[0] - launched(w, ("PIPELINE", "u"))[0] >= w.duration


def test_hands_off_to_upstream_job_with_launch_task(fake_checker):
    w = FakeW(launch_jobs={"10": launch_params(["u.t1"])})
    res = run(w, [plan("JOB", "U", "10", ["x.src"]), plan("JOB", "B", "2", ["u.t1"])], {("JOB", "U"): {"u.t1"}})
    assert res[("B", "default")].status == "WAITING_ON_UPSTREAM" and not launched(w, ("JOB", 2))


@pytest.mark.parametrize("params", [launch_params(["u.t1"], dry_run="true"), launch_params(["u.t1"], entity_type="PIPELINE"),
                                    ["--tracker_table=x", "--src_tables=u.t1"]])  # --dry_run defaults to true
def test_no_handoff_to_launch_task_that_would_not_launch(fake_checker, params):
    w = FakeW(launch_jobs={"10": params})
    res = run(w, [plan("JOB", "U", "10", ["x.src"]), plan("JOB", "B", "2", ["u.t1"])], {("JOB", "U"): {"u.t1"}})
    assert res[("B", "default")].status == "LAUNCHED" and "after JOB 'U'" in res[("B", "default")].detail


def test_waits_for_upstream_job_without_launch_task(fake_checker):
    w = FakeW()
    res = run(w, [plan("JOB", "U", "10", ["x.src"]), plan("JOB", "B", "2", ["u.t1"])], {("JOB", "U"): {"u.t1"}})
    assert res[("B", "default")].status == "LAUNCHED" and "after JOB 'U'" in res[("B", "default")].detail


def test_hands_off_to_running_producer_job(fake_checker):
    w = FakeW(launch_jobs={"77": launch_params(["p.t"])}, running={"p": ("upd1", 77, time.time() + 0.2)})
    assert run(w, [plan("JOB", "B", "2", ["p.t"])], {})[("B", "default")].status == "WAITING_ON_UPSTREAM"


def test_waits_for_running_producer_without_handoff(fake_checker):
    w = FakeW(running={"p": ("upd1", None, time.time() + 0.2)})
    t0 = time.time()
    assert run(w, [plan("JOB", "B", "2", ["p.t"])], {})[("B", "default")].status == "LAUNCHED"
    assert time.time() - t0 >= 0.2


def test_timeouts_run_in_parallel(fake_checker):
    w = FakeW(running={"p": ("u1", None, time.time() + 60), "q": ("u2", None, time.time() + 60)})
    t0 = time.time()
    res = run(w, [plan("JOB", "B", "2", ["p.t"]), plan("JOB", "C", "3", ["q.t"])], {}, timeout=0.5)
    assert all(r.status == "QUEUE_TIMEOUT" for r in res.values())
    assert time.time() - t0 < 0.9  # one shared deadline, not one per wait


def test_upstream_failure_not_launched(fake_checker):
    w = FakeW(outcome="FAILED")
    res = run(w, [plan("PIPELINE", "U", "u", ["x.src"]), plan("JOB", "B", "2", ["u.t1"])], {("PIPELINE", "U"): {"u.t1"}})
    assert res[("B", "default")].status == "UPSTREAM_FAILED" and not launched(w, ("JOB", 2))


def test_two_groups_launch_once(fake_checker):
    w = FakeW()
    res = run(w, [plan("JOB", "G", "5", ["a.t"], "daily"), plan("JOB", "G", "5", ["b.t"], "weekly")], {})
    assert sorted(r.status for r in res.values()) == ["ALREADY_HANDLED", "LAUNCHED"] and len(w.launches) == 1


def test_per_row_upstreams(fake_checker):
    w = FakeW(duration=0.3)
    res = run(w, [plan("PIPELINE", "U", "u", ["x.src"]), plan("JOB", "G", "5", ["a.t"], "weekly"),
                  plan("JOB", "G", "5", ["u.t1", "a.t"], "test")], {("PIPELINE", "U"): {"u.t1"}})
    g, u_at = launched(w, ("JOB", 5)), launched(w, ("PIPELINE", "u"))[0]
    assert res[("G", "weekly")].status == res[("G", "test")].status == "LAUNCHED"
    assert len(g) == 2 and g[0] - u_at < 0.2 and g[1] - u_at >= 0.3


def test_dry_run_launches_nothing(fake_checker):
    w = FakeW()
    res = run(w, [plan("PIPELINE", "U", "u", ["x.src"]), plan("JOB", "B", "2", ["u.t1"])], {("PIPELINE", "U"): {"u.t1"}},
              dry_run=True)
    assert not w.launches and "would wait for" in res[("B", "default")].detail


def test_cycle_completes(fake_checker):
    res = run(FakeW(), [plan("JOB", "A", "1", ["b.t"]), plan("JOB", "B", "2", ["a.t"])],
              {("JOB", "A"): {"a.t"}, ("JOB", "B"): {"b.t"}})
    assert len(res) == 2


def test_stale_after_wait_skipped(fake_checker):
    fake_checker.stale_after_wait = True
    w = FakeW()
    res = run(w, [plan("PIPELINE", "U", "u", ["x.src"]), plan("JOB", "B", "2", ["u.t1"])], {("PIPELINE", "U"): {"u.t1"}})
    assert res[("B", "default")].status == "SKIPPED" and "after waiting" in res[("B", "default")].detail


def test_already_handled_after_wait(fake_checker):
    w = FakeW()
    calls = []

    def decide(w_, etype, eid, refreshed):
        calls.append(eid)
        return ("SKIP", "run 9 started after the newest source refresh") if eid == "2" else ("LAUNCH", "")
    fake_checker.decide = decide
    res = run(w, [plan("PIPELINE", "U", "u", ["x.src"]), plan("JOB", "B", "2", ["u.t1"])], {("PIPELINE", "U"): {"u.t1"}})
    assert res[("B", "default")].status == "ALREADY_HANDLED" and not launched(w, ("JOB", 2)) and "2" in calls


# ---- an upstream that is itself WAITING_ON_UPSTREAM (Isaac Review finding) -----------------------------------------
def test_downstream_of_waiting_job_with_launch_task_waits(fake_checker):
    # X (job with launch task for x.t) is launched; A reads x.t -> WAITING (handoff to X);
    # B reads A's target a.t, and A's own launch task covers a.t -> B waits too
    w = FakeW(launch_jobs={"1": launch_params(["x.t"]), "2": launch_params(["a.t"])})
    res = run(w, [plan("JOB", "X", "1", ["s.t"]), plan("JOB", "A", "2", ["x.t"]), plan("JOB", "B", "3", ["a.t"])],
              {("JOB", "X"): {"x.t"}, ("JOB", "A"): {"a.t"}})
    assert res[("A", "default")].status == "WAITING_ON_UPSTREAM"
    assert res[("B", "default")].status == "WAITING_ON_UPSTREAM" and "itself waiting" in res[("B", "default")].detail
    assert not launched(w, ("JOB", 3))


def test_downstream_of_waiting_upstream_without_handoff_launches_with_note(fake_checker):
    # same, but A is a pipeline (no launch task): B launches on A's current data and says so
    w = FakeW(launch_jobs={"1": launch_params(["x.t"])})
    res = run(w, [plan("JOB", "X", "1", ["s.t"]), plan("PIPELINE", "A", "a", ["x.t"]), plan("JOB", "B", "3", ["a.t"])],
              {("JOB", "X"): {"x.t"}, ("PIPELINE", "A"): {"a.t"}})
    assert res[("A", "default")].status == "WAITING_ON_UPSTREAM"
    assert res[("B", "default")].status == "LAUNCHED" and "waiting on its upstream" in res[("B", "default")].detail


# ---- second Isaac Review pass ----------------------------------------------------------------------------------------
def test_two_groups_waiting_on_the_same_upstream_launch_once(fake_checker):
    # both groups of G wait for U; the post-wait decide() runs under G's lock, so the second sees the first's run
    w = FakeW(duration=0.2)
    fake_checker.decide = lambda w_, etype, eid, refreshed: (
        ("SKIP", "run started after the newest source refresh") if launched(w, ("JOB", int(eid))) else ("LAUNCH", ""))
    res = run(w, [plan("PIPELINE", "U", "u", ["x.src"]), plan("JOB", "G", "5", ["u.t1"], "daily"),
                  plan("JOB", "G", "5", ["u.t1"], "weekly")], {("PIPELINE", "U"): {"u.t1"}})
    assert len(launched(w, ("JOB", 5))) == 1
    assert sorted(res[("G", g)].status for g in ("daily", "weekly")) == ["ALREADY_HANDLED", "LAUNCHED"]


def test_note_when_upstream_was_not_launched(fake_checker):
    # U's launch fails (start_update raises): B launches on U's current data and says so
    w = FakeW()

    def boom(pipeline_id):
        raise RuntimeError("no permission")
    w.pipelines.start_update = boom
    res = run(w, [plan("PIPELINE", "U", "u", ["x.src"]), plan("JOB", "B", "2", ["u.t1"])], {("PIPELINE", "U"): {"u.t1"}})
    assert res[("U", "default")].status == "FAILED"
    assert res[("B", "default")].status == "LAUNCHED" and "was not launched here (FAILED)" in res[("B", "default")].detail


@pytest.mark.parametrize("flag, raises", [(None, True), ("true", True), ("false", False)])
def test_fail_on_queue_timeout(monkeypatch, flag, raises):
    import sys
    from conftest import Row

    class Spark:
        conf = NS(get=lambda key: "UTC")

        def sql(self, query, args=None):
            return NS(collect=lambda: [Row(ts=1791800000000)] if "unix_millis" in query else [])
    monkeypatch.setattr(jpl, "get_spark", lambda: Spark())
    monkeypatch.setattr(jpl, "get_workspace_client", lambda: None)
    monkeypatch.setattr(jpl, "FreshnessChecker", lambda spark, w: None)
    monkeypatch.setattr(jpl, "launch_all", lambda launcher, planned, targets_of: [
        jpl.Result("JOB", "B", "default", "QUEUE_TIMEOUT", "still running at the deadline")])
    monkeypatch.setattr(sys, "argv", ["x", "--tracker_table=t", "--src_tables=a.t"]
                        + ([f"--fail_on_queue_timeout={flag}"] if flag else []))
    if raises:
        with pytest.raises(Exception, match="could not be launched"):
            jpl.main()
    else:
        jpl.main()
