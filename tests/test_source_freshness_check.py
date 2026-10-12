import sys
from datetime import datetime, timezone
from types import SimpleNamespace as NS

import pytest
from conftest import Row, enum

import source_freshness_check as sfc

REFRESH = datetime(2026, 10, 12, 2, 0)


def ms(dt):
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def job_run(run_id, start, result, life="TERMINATED"):
    return NS(run_id=run_id, start_time=ms(start), state=NS(result_state=enum(result), life_cycle_state=enum(life)))


class FakeW:
    def __init__(self, runs=(), updates=()):
        self.runs, self.updates, self.cancelled = list(runs), list(updates), []
        self.jobs = NS(list_runs=lambda job_id, limit: iter(self.runs), cancel_run=lambda run_id: self.cancelled.append(run_id),
                       list=lambda name: [NS(job_id=1, settings=NS(name=name))])
        self.pipelines = NS(get=lambda pipeline_id: NS(latest_updates=self.updates))


AFTER, BEFORE = datetime(2026, 10, 12, 3, 0), datetime(2026, 10, 12, 1, 0)


# ---- previous_run -------------------------------------------------------------------------------------------------
def test_previous_run_skips_current_and_cancelled_runs():
    w = FakeW([job_run(100, AFTER, None, "RUNNING"),       # this job run
               job_run(99, AFTER, "CANCELED"),             # a tick cancelled by this check
               job_run(98, AFTER, "FAILED"),
               job_run(97, AFTER, "SUCCESS")])
    assert sfc.previous_run(w, "JOB", "1", "100")[1] == 97


def test_previous_run_counts_a_running_run():
    w = FakeW([job_run(100, AFTER, None, "RUNNING"), job_run(99, AFTER, None, "RUNNING")])
    assert sfc.previous_run(w, "JOB", "1", "100")[1] == 99


def test_previous_run_skips_ended_runs_without_result():
    w = FakeW([job_run(99, AFTER, None, "SKIPPED"), job_run(98, AFTER, None, "INTERNAL_ERROR"),
               job_run(97, BEFORE, "SUCCESS")])
    assert sfc.previous_run(w, "JOB", "1", "100")[1] == 97


def test_previous_run_none_without_success():
    assert sfc.previous_run(FakeW([job_run(9, AFTER, "FAILED")]), "JOB", "1", "100") is None


def test_previous_run_pipeline_skips_failed_updates():
    ups = [NS(update_id="u2", creation_time="2026-10-12T03:00:00Z", state=enum("FAILED")),
           NS(update_id="u1", creation_time="2026-10-12T02:30:00Z", state=enum("COMPLETED"))]
    assert sfc.previous_run(FakeW(updates=ups), "PIPELINE", "p", "100")[1] == "u1"


# ---- main: cancel only when already processed ---------------------------------------------------------------------
class FakeSpark:
    conf = NS(get=lambda key: "UTC")

    def sql(self, query, args=None):
        if "unix_millis" in query:
            return NS(collect=lambda: [Row(ts=ms(datetime(2026, 10, 12, 4, 0)))])
        return NS(collect=lambda: [Row(entity_name="J", entity_type="JOB", src_group="default", src_tables=["c.s.t"],
                                       src_tables_to_skip=[], src_table_producers={}, refresh_window_btn_tables_in_mins=600)])


def run_main(monkeypatch, w, flag=True, job_run_id=100):
    class FakeChecker:
        def __init__(self, spark, w_):
            pass

        def check(self, tables, window, now, producers):
            return None, [NS(table=t, is_fresh=True, data_refreshed_at=REFRESH, met_without_row_changes=False,
                             describe=lambda: "MET") for t in tables]
    monkeypatch.setattr(sfc, "FreshnessChecker", FakeChecker)
    monkeypatch.setattr(sfc, "get_spark", lambda: FakeSpark())
    monkeypatch.setattr(sfc, "get_workspace_client", lambda: w)
    monkeypatch.setattr(sfc, "log_tracker_row", lambda row: None)
    argv = ["x", "--tracker_table=t", "--entity_name=J", f"--job_run_id={job_run_id}"]
    monkeypatch.setattr(sys, "argv", argv + (["--skip_if_already_processed=true"] if flag else []))
    sfc.main()
    return w.cancelled


def test_cancels_when_previous_success_started_after_refresh(monkeypatch):
    assert run_main(monkeypatch, FakeW([job_run(100, AFTER, None, "RUNNING"), job_run(97, AFTER, "SUCCESS")])) == [100]


def test_runs_when_previous_success_started_before_refresh(monkeypatch):
    assert run_main(monkeypatch, FakeW([job_run(100, AFTER, None, "RUNNING"), job_run(97, BEFORE, "SUCCESS")])) == []


def test_does_not_alternate_after_a_cancelled_tick(monkeypatch):
    # tick 100 cancelled itself; the successful run 97 before it still covers the data -> tick 101 cancels too
    runs = [job_run(101, AFTER, None, "RUNNING"), job_run(100, AFTER, "CANCELED"), job_run(97, AFTER, "SUCCESS")]
    assert run_main(monkeypatch, FakeW(runs), job_run_id=101) == [101]


def test_failed_run_after_refresh_does_not_count(monkeypatch):
    runs = [job_run(100, AFTER, None, "RUNNING"), job_run(98, AFTER, "FAILED"), job_run(97, BEFORE, "SUCCESS")]
    assert run_main(monkeypatch, FakeW(runs)) == []


def test_flag_off_never_cancels(monkeypatch):
    assert run_main(monkeypatch, FakeW([job_run(97, AFTER, "SUCCESS")]), flag=False) == []


def test_flag_needs_job_run_id(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["x", "--tracker_table=t", "--entity_name=J", "--skip_if_already_processed=true"])
    with pytest.raises(SystemExit):
        sfc.parse_args()
