from datetime import datetime, timedelta
from types import SimpleNamespace as NS

from conftest import enum

from async_job_execution import freshness
from async_job_execution.freshness import FreshnessChecker, SourceStatus, utc

T1, T2 = datetime(2026, 10, 12, 1, 0), datetime(2026, 10, 12, 2, 0)


def checker(w=None, spark=None):
    return FreshnessChecker(spark, w)


# ---- pipeline_last_success: later of the events table and the event log -----------------------------------------
def test_later_signal_wins(monkeypatch):
    c = checker()
    monkeypatch.setattr(c, "_from_events_table", lambda t: (T1, "events table"))
    monkeypatch.setattr(c, "_from_event_log", lambda t, p: (T2, "event log"))
    monkeypatch.setattr(c, "_from_pipeline_updates", lambda t, p: (datetime(2030, 1, 1), "updates"))
    assert c.pipeline_last_success("c.s.t", "p1") == (T2, "event log")


def test_events_table_wins_when_later(monkeypatch):
    c = checker()
    monkeypatch.setattr(c, "_from_events_table", lambda t: (T2, "events table"))
    monkeypatch.setattr(c, "_from_event_log", lambda t, p: (T1, "event log"))
    assert c.pipeline_last_success("c.s.t", "p1")[1] == "events table"


def test_pipeline_updates_only_when_both_missing(monkeypatch):
    c = checker()
    monkeypatch.setattr(c, "_from_events_table", lambda t: None)
    monkeypatch.setattr(c, "_from_event_log", lambda t, p: None)
    monkeypatch.setattr(c, "_from_pipeline_updates", lambda t, p: (T1, "updates"))
    assert c.pipeline_last_success("c.s.t", "p1") == (T1, "updates")
    c2 = checker()
    monkeypatch.setattr(c2, "_from_events_table", lambda t: None)
    monkeypatch.setattr(c2, "_from_event_log", lambda t, p: None)
    monkeypatch.setattr(c2, "_from_pipeline_updates", lambda t, p: None)
    assert c2.pipeline_last_success("c.s.t", "p1") == (None, "no completed pipeline run found")


# ---- events table error classification ----------------------------------------------------------------------------
class RaisingSpark:
    def __init__(self, message):
        self.message = message

    def sql(self, *a, **k):
        raise Exception(self.message)


def _with_table_id(c):
    c._table_info["c.s.t"] = NS(table_id="tid", pipeline_id="p1", table_type=None)
    return c


def test_events_table_disabled_when_unavailable():
    c = _with_table_id(checker(spark=RaisingSpark("[TABLE_OR_VIEW_NOT_FOUND] system.lakeflow...")))
    assert c._from_events_table("c.s.t") is None
    assert c._events_table_available is False


def test_events_table_kept_after_transient_error():
    c = _with_table_id(checker(spark=RaisingSpark("connection reset / timeout")))
    assert c._from_events_table("c.s.t") is None
    assert c._events_table_available is None  # tried again for the next table


# ---- producer_update ----------------------------------------------------------------------------------------------
def pipelines(get, get_update=None):
    return NS(pipelines=NS(get=get, get_update=get_update or (lambda **k: None)))


def test_producer_update_ignores_continuous():
    w = pipelines(lambda pipeline_id: NS(spec=NS(continuous=True), state=enum("RUNNING"), latest_updates=[]))
    c = _with_table_id(checker(w=w))
    assert c.producer_update("c.s.t") is None


def test_producer_update_ignores_idle():
    w = pipelines(lambda pipeline_id: NS(spec=NS(continuous=False), state=enum("IDLE"), latest_updates=[]))
    assert _with_table_id(checker(w=w)).producer_update("c.s.t") is None


def test_producer_update_returns_active_update_and_cause():
    updates = [NS(update_id="u-done", state=enum("COMPLETED")), NS(update_id="u-run", state=enum("RUNNING"))]
    w = pipelines(lambda pipeline_id: NS(spec=NS(continuous=False), state=enum("RUNNING"), latest_updates=updates),
                  lambda pipeline_id, update_id: NS(update=NS(cause=enum("JOB_TASK"))))
    assert _with_table_id(checker(w=w)).producer_update("c.s.t") == ("p1", "u-run", "JOB_TASK")


# ---- check(): refreshed_now and data_refreshed_at ---------------------------------------------------------------
def _stub_check(monkeypatch, c, last_write, event_log):
    monkeypatch.setattr(c, "last_write", lambda t: (last_write, "MERGE", {}, None))
    monkeypatch.setattr(c, "resolve_producer", lambda t, p: ("PIPELINE", "p1"))
    monkeypatch.setattr(c, "_from_event_log", lambda t, p: event_log)


def test_refreshed_now_counts_as_now_but_keeps_actual_completion(monkeypatch):
    now = datetime(2026, 10, 12, 3, 0)
    c = checker()
    _stub_check(monkeypatch, c, last_write=T1, event_log=(T2, "event log"))
    _, [st] = c.check(["c.s.t"], 20, now, refreshed_now={"c.s.t"})
    assert st.refreshed_at == now and st.is_fresh
    assert st.data_refreshed_at == T2  # max(last write T1, event-log completion T2)


def test_refreshed_now_without_event_log_uses_now(monkeypatch):
    now = datetime(2026, 10, 12, 3, 0)
    c = checker()
    _stub_check(monkeypatch, c, last_write=T1, event_log=None)
    _, [st] = c.check(["c.s.t"], 20, now, refreshed_now={"c.s.t"})
    assert st.data_refreshed_at == now


def test_not_refreshed_now_data_refreshed_at_is_refreshed_at(monkeypatch):
    now = datetime(2026, 10, 12, 3, 0)
    c = checker()
    _stub_check(monkeypatch, c, last_write=T1, event_log=None)
    monkeypatch.setattr(c, "pipeline_last_success", lambda t, p: (T2, "event log"))
    _, [st] = c.check(["c.s.t"], 20, now)
    assert st.refreshed_at == T2 == st.data_refreshed_at
    assert not st.is_fresh  # T2 is an hour before now, window 20 min


def test_epoch_ms_now_is_utc(monkeypatch):
    c = checker()
    _stub_check(monkeypatch, c, last_write=None, event_log=None)
    monkeypatch.setattr(c, "pipeline_last_success", lambda t, p: (None, None))
    cutoff, _ = c.check(["c.s.t"], 60, 1791800000000)  # epoch ms
    assert cutoff == datetime(2026, 10, 12, 10, 13, 20) - timedelta(minutes=60)  # 1791800000000 ms = 10:13:20 UTC


# ---- log format ---------------------------------------------------------------------------------------------------
def test_describe_shows_utc_comparison():
    now = datetime(2026, 10, 12, 2, 10)
    st = SourceStatus("c.s.t", T2, "PIPELINE", "p1", T2, "event log", T2, True, last_write_operation="MERGE",
                      last_write_rows={}, cutoff=now - timedelta(minutes=20), now=now, window_mins=20)
    text = st.describe()
    assert f"refreshed_at {utc(T2)} >= cutoff {utc(now - timedelta(minutes=20))}" in text
    assert utc(now) in text and " UTC" in utc(now)


def test_write_operations():
    for op in ("COPY INTO", "TRUNCATE", "DLT REFRESH", "STREAMING UPDATE", "MERGE"):
        assert op in freshness.WRITE_OPERATIONS
    for op in ("CLONE", "RESTORE", "OPTIMIZE", "DLT SETUP", "SET TBLPROPERTIES", "CREATE TABLE"):
        assert op not in freshness.WRITE_OPERATIONS
