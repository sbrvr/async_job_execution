"""Shared source-freshness logic for Source Freshness Check and Job Pipeline Launch.

A source table counts as refreshed at the later of:
  1. its last data write (DESCRIBE HISTORY, data operations only), and
  2. its producer's last successful run — even if that run changed no rows (e.g. an SCD2 / AUTO CDC update with
     nothing new writes no table version, but the pipeline did process the table):
       PIPELINE producer (pipeline-managed streaming table / materialized view): the later of
         a) system pipeline events table — latest COMPLETED flow_progress for the table's uc_table_id (lags), and
         b) the pipeline's event log, event_log(<pipeline_id>) — latest COMPLETED flow writing the table;
         c) if neither has one: Pipelines API — latest COMPLETED update that covered the table (whole pipeline or selected)
       JOB producer (table written by a job, from the tracker's src_table_producers): last successful job run.
Tables passed as `refreshed_now` (just processed by the upstream job calling the check) count as refreshed now.
Tables with no known producer use the last data write only.

All timestamps are naive UTC. Spark timestamps are read as epoch milliseconds (unix_millis), so the result does not
depend on the session time zone.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import islice
from typing import Optional

# Delta operations that represent an actual write of data (excludes OPTIMIZE, VACUUM, SET TBLPROPERTIES, ...)
WRITE_OPERATIONS = [
    "WRITE", "MERGE", "DELETE", "UPDATE", "STREAMING UPDATE",
    "CREATE TABLE AS SELECT", "REPLACE TABLE AS SELECT",
    "CREATE OR REPLACE TABLE AS SELECT", "COPY INTO", "TRUNCATE",
    "DLT REFRESH",  # SDP full refresh truncates the table first, then reloads it (STREAMING UPDATE / MERGE)
]
# Beta location; moves to system.lakeflow.pipeline_events at GA
PIPELINE_EVENTS_TABLE = "system.lakeflow_pipeline_events_preview.pipeline_events"
# operationMetrics that count rows impacted by a write (WRITE / MERGE / UPDATE / DELETE / STREAMING UPDATE)
ROW_METRICS = ("numOutputRows", "numTargetRowsInserted", "numTargetRowsUpdated", "numTargetRowsDeleted",
               "numUpdatedRows", "numDeletedRows", "numCopiedRows")
REFRESHED_NOW_SIGNAL = "refreshed by calling upstream job"
# How far back to look for a producer's last success (the window check is applied afterwards)
PRODUCER_LOOKBACK = timedelta(days=31)
# Pipeline states with an update in progress
ACTIVE_PIPELINE_STATES = {"RUNNING", "STARTING", "DEPLOYING", "RESETTING"}
# Pipeline update states that are final
FINAL_UPDATE_STATES = {"COMPLETED", "FAILED", "CANCELED"}
# Errors meaning the system events table is not usable at all (vs. a transient failure of one query)
EVENTS_TABLE_UNAVAILABLE = ("TABLE_OR_VIEW_NOT_FOUND", "SCHEMA_NOT_FOUND", "CATALOG_NOT_FOUND", "PERMISSION_DENIED",
                            "INSUFFICIENT_PERMISSIONS")


@dataclass
class SourceStatus:
    table: str
    last_write: Optional[datetime]
    producer_type: str  # PIPELINE | JOB | NONE
    producer_id: Optional[str]
    producer_success: Optional[datetime]
    producer_signal: Optional[str]  # where producer_success came from
    refreshed_at: Optional[datetime]
    is_fresh: bool
    error: Optional[str] = None
    last_write_operation: Optional[str] = None
    last_write_rows: Optional[dict] = None  # non-zero row metrics of the last data write
    cutoff: Optional[datetime] = None
    now: Optional[datetime] = None
    window_mins: Optional[int] = None
    # when the data in the table was last refreshed: refreshed_at, except for tables refreshed by the calling upstream
    # job (refreshed_at = now), where it is the producer's actual completion. A downstream run that started after it
    # already includes the data.
    data_refreshed_at: Optional[datetime] = None

    @property
    def basis(self) -> Optional[str]:
        """What made the source meet the window (None if it did not)."""
        if not self.is_fresh:
            return None
        if self.last_write and self.cutoff and self.last_write >= self.cutoff:
            return "data write"
        if self.last_write_operation == "MATERIALIZED_VIEW":
            return "producer refresh"
        if self.producer_signal == REFRESHED_NOW_SIGNAL:
            return "refreshed by calling upstream job, no rows changed in window"
        return "producer run, no rows changed"

    @property
    def met_without_row_changes(self) -> bool:
        """Met the window only because its producer ran successfully, without writing rows in the window."""
        return bool(self.basis) and "no rows changed" in self.basis

    def describe(self) -> str:
        """Log lines: met / not met (and why), what refreshed it, and the rows impacted by the last write."""
        verdict = f"MET ({self.basis})" if self.is_fresh else "NOT MET"
        age = _age(self.now - self.refreshed_at) + " ago" if self.refreshed_at and self.now else "never"
        compare = (f"refreshed_at {utc(self.refreshed_at)} {'>=' if self.is_fresh else '<'} cutoff {utc(self.cutoff)}"
                   if self.refreshed_at else f"cutoff {utc(self.cutoff)}")
        lines = [f"{verdict} {self.table}: refreshed {age} — {compare} "
                 f"(now {utc(self.now)} − window {self.window_mins} min)"]
        if self.last_write:
            rows = ", ".join(f"{k}={v}" for k, v in (self.last_write_rows or {}).items()) or "0 rows"
            lines.append(f"last write: {self.last_write_operation} at {utc(self.last_write)} ({rows})")
        elif self.last_write_operation == "MATERIALIZED_VIEW":
            lines.append("last write: materialized view — no Delta history; refresh and rows from its producer pipeline")
        else:
            lines.append("last write: none found")
        producer = f"{self.producer_type} {self.producer_id or ''}".rstrip()
        if self.producer_success is None:
            lines.append(f"producer: {producer}" + (f" — {self.producer_signal}" if self.producer_signal else ""))
        elif self.last_write_operation == "MATERIALIZED_VIEW":
            lines.append(f"producer: {producer} refreshed it at {utc(self.producer_success)} [{self.producer_signal}]")
        elif self.cutoff and self.producer_success >= self.cutoff and (not self.last_write or self.last_write < self.cutoff):
            # only the producer run refreshed it: it ran successfully but wrote nothing within the window
            lines.append(f"producer: {producer} ran successfully at {utc(self.producer_success)} "
                         f"[{self.producer_signal}] — no write within the window (run changed no rows)")
        else:
            lines.append(f"producer: {producer} last success {utc(self.producer_success)} [{self.producer_signal}]")
        if self.data_refreshed_at and self.data_refreshed_at != self.refreshed_at:
            lines.append(f"data refreshed at {utc(self.data_refreshed_at)} (producer's actual completion; "
                         f"a downstream run started after it already includes the data)")
        if self.error:
            lines.append(f"error: {self.error}")
        return "\n          ".join(lines)


def utc(value: Optional[datetime]) -> str:
    """Log format of a naive-UTC timestamp, labelled UTC."""
    return f"{value.isoformat(sep=' ', timespec='milliseconds')} UTC" if value else "none"


def _age(delta: timedelta) -> str:
    minutes = delta.total_seconds() / 60
    if minutes < 120:
        return f"{minutes:.1f} min"
    if minutes < 2 * 24 * 60:
        return f"{minutes / 60:.1f} h"
    return f"{minutes / 1440:.1f} days"


def _naive_utc(value) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, (int, float)):  # epoch milliseconds
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc).replace(tzinfo=None)
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return value.astimezone(timezone.utc).replace(tzinfo=None) if value.tzinfo else value


class FreshnessChecker:
    """Caches lookups for one sweep; a source shared by several entities is checked once."""

    def __init__(self, spark, workspace_client, pipeline_events_table: str = PIPELINE_EVENTS_TABLE):
        self.spark = spark
        self.w = workspace_client
        self.pipeline_events_table = pipeline_events_table
        self._events_table_available = None
        self._last_write, self._table_info, self._job_success = {}, {}, {}
        self._pipeline_success, self._event_log = {}, {}
        self._since_ms = int((datetime.now(timezone.utc) - PRODUCER_LOOKBACK).timestamp() * 1000)

    # ---- data writes -------------------------------------------------------------------------------------------
    def last_write(self, table: str):
        """Return (timestamp, operation, row metrics, error) of the table's last data write."""
        if table not in self._last_write:
            info = self.table_info(table)
            if info is not None and info.table_type and info.table_type.value == "MATERIALIZED_VIEW":
                # no Delta history for materialized views: refresh and row counts come from the producer pipeline
                self._last_write[table] = (None, "MATERIALIZED_VIEW", None, None)
                return self._last_write[table]
            try:
                history = self.spark.sql(f"DESCRIBE HISTORY {table}")
                rows = (history.filter(history.operation.isin(WRITE_OPERATIONS))
                        .orderBy(history.timestamp.desc()).limit(1)
                        .selectExpr("unix_millis(timestamp) AS ts", "operation", "operationMetrics").collect())
                if rows:
                    metrics = rows[0]["operationMetrics"] or {}
                    impacted = {k: metrics[k] for k in ROW_METRICS if metrics.get(k) not in (None, "0")}
                    self._last_write[table] = (_naive_utc(rows[0]["ts"]), rows[0]["operation"], impacted, None)
                else:
                    self._last_write[table] = (None, None, None, None)
            except Exception as e:
                first_line = str(e).strip().splitlines()[0] if str(e).strip() else type(e).__name__
                self._last_write[table] = (None, None, None, f"DESCRIBE HISTORY failed: {first_line}")
        return self._last_write[table]

    def table_info(self, table: str):
        if table not in self._table_info:
            try:
                self._table_info[table] = self.w.tables.get(full_name=table)
            except Exception:
                self._table_info[table] = None
        return self._table_info[table]

    # ---- producers ---------------------------------------------------------------------------------------------
    def resolve_producer(self, table: str, producers: dict):
        """Producer from the tracker; pipeline ownership is re-read from Unity Catalog (always current)."""
        info = self.table_info(table)
        if info is not None and info.pipeline_id:
            return "PIPELINE", info.pipeline_id
        p = (producers or {}).get(table)
        if p and p["producer_type"] == "JOB" and p["producer_id"]:
            return "JOB", str(p["producer_id"])
        return "NONE", None

    def job_last_success(self, job_id: str):
        if job_id not in self._job_success:
            result = None
            try:
                # list_runs pages through the whole run history: look at the latest 25 completed runs only
                for run in islice(self.w.jobs.list_runs(job_id=int(job_id), completed_only=True, limit=25), 25):
                    if run.state and run.state.result_state and run.state.result_state.value == "SUCCESS":
                        result = (_naive_utc(run.end_time), f"job run {run.run_id}")
                        break
            except Exception as e:
                result = (None, f"jobs.list_runs failed: {e}")
            self._job_success[job_id] = result or (None, "no successful job run found")
        return self._job_success[job_id]

    def pipeline_last_success(self, table: str, pipeline_id: str):
        """Latest time the pipeline successfully processed `table` (with or without row changes).

        The later of the events system table and the event log: the system table can lag behind the event log, so
        taking the first signal found could report a pipeline run that has just completed as an older one.
        """
        key = (table, pipeline_id)
        if key not in self._pipeline_success:
            signals = [s for s in (self._from_events_table(table), self._from_event_log(table, pipeline_id)) if s]
            self._pipeline_success[key] = (max(signals, key=lambda s: s[0]) if signals
                                           else self._from_pipeline_updates(table, pipeline_id)
                                           or (None, "no completed pipeline run found"))
        return self._pipeline_success[key]

    def _from_events_table(self, table):
        if self._events_table_available is False:
            return None
        info = self.table_info(table)
        if info is None or not info.table_id:
            return None
        try:
            row = self.spark.sql(
                f"""
                SELECT unix_millis(max(event_time)) AS t
                FROM {self.pipeline_events_table}
                WHERE event_type = 'flow_progress'
                  AND variant_get(details, '$.flow_progress.status', 'STRING') = 'COMPLETED'
                  AND origin.uc_table_id = :table_id
                  AND event_time >= timestamp_millis(:since_ms)
                """,
                args={"table_id": info.table_id, "since_ms": self._since_ms},
            ).collect()[0]
            self._events_table_available = True
            return (_naive_utc(row["t"]), "pipeline_events system table") if row["t"] is not None else None
        except Exception as e:
            if any(marker in str(e) for marker in EVENTS_TABLE_UNAVAILABLE):
                self._events_table_available = False  # not enabled / no access — skip it for the rest of the sweep
            return None  # use the event log

    def _from_event_log(self, table, pipeline_id):
        """Latest COMPLETED flow writing `table`, plus the rows that flow run reported (summed flow_progress metrics)."""
        latest = [r for r in self._event_log_by_dataset(pipeline_id)
                  if r["dataset"] == table or table.endswith("." + r["dataset"])]
        if not latest:
            return None
        r = max(latest, key=lambda r: r["t"])
        counts = ", ".join(f"{k}={r[k]}" for k in ("output_rows", "upserted_rows", "deleted_rows") if r[k])
        return _naive_utc(r["t"]), f"pipeline event log, update {r['update_id']}: {counts or '0 rows'}"

    def _event_log_by_dataset(self, pipeline_id):
        """One event_log() query per pipeline: per dataset, its latest COMPLETED update and the rows reported in it."""
        if pipeline_id not in self._event_log:
            rows = []
            if all(c.isalnum() or c == "-" for c in pipeline_id):
                try:
                    rows = self.spark.sql(
                        f"""
                        WITH flows AS (  -- flow -> table it writes (AUTO CDC / append flows are named differently)
                          SELECT DISTINCT origin.flow_name AS flow_name,
                                 replace(details:flow_definition.output_dataset::string, '`', '') AS dataset
                          FROM event_log('{pipeline_id}')
                          WHERE event_type = 'flow_definition'
                        ),
                        progress AS (
                          SELECT f.dataset, e.timestamp, e.origin.update_id AS update_id,
                                 e.details:flow_progress.status::string AS status,
                                 coalesce(e.details:flow_progress.metrics.num_output_rows::bigint, 0) AS output_rows,
                                 coalesce(e.details:flow_progress.metrics.num_upserted_rows::bigint, 0) AS upserted_rows,
                                 coalesce(e.details:flow_progress.metrics.num_deleted_rows::bigint, 0) AS deleted_rows
                          FROM event_log('{pipeline_id}') e
                          JOIN flows f ON e.origin.flow_name = f.flow_name
                          WHERE e.event_type = 'flow_progress'
                            AND e.timestamp >= timestamp_millis(:since_ms)
                        ),
                        last_done AS (
                          SELECT dataset, max_by(update_id, timestamp) AS update_id, max(timestamp) AS t
                          FROM progress WHERE status = 'COMPLETED'
                          GROUP BY dataset
                        )
                        SELECT l.dataset, unix_millis(l.t) AS t, l.update_id, sum(p.output_rows) AS output_rows,
                               sum(p.upserted_rows) AS upserted_rows, sum(p.deleted_rows) AS deleted_rows
                        FROM last_done l LEFT JOIN progress p ON p.dataset = l.dataset AND p.update_id = l.update_id
                        GROUP BY l.dataset, l.t, l.update_id
                        """,
                        args={"since_ms": self._since_ms},
                    ).collect()
                except Exception:
                    pass  # no access to the event log — use the next signal
            self._event_log[pipeline_id] = [r for r in rows if r["t"] is not None and r["dataset"]]
        return self._event_log[pipeline_id]

    def _from_pipeline_updates(self, table, pipeline_id):
        try:
            for u in (self.w.pipelines.get(pipeline_id=pipeline_id).latest_updates or []):
                if not u.state or u.state.value != "COMPLETED":
                    continue
                upd = self.w.pipelines.get_update(pipeline_id=pipeline_id, update_id=u.update_id).update
                selected = set((upd.refresh_selection or []) + (upd.full_refresh_selection or []))
                short = table.split(".")[-1]
                if not selected or table in selected or short in selected:
                    # update start time: conservative (the update finished later)
                    return _naive_utc(u.creation_time), f"pipeline update {u.update_id} (start time)"
        except Exception:
            pass
        return None

    def producer_update(self, table: str):
        """(pipeline_id, update_id, cause) of the update in progress of the table's producing pipeline, else None.

        Continuous pipelines are always running, so they never count: their tables are refreshed continuously and
        their freshness comes from their latest completed flow. Not cached: the state changes while the task waits.
        """
        info = self.table_info(table)
        if info is None or not info.pipeline_id:
            return None
        try:
            p = self.w.pipelines.get(pipeline_id=info.pipeline_id)
            if (p.spec and p.spec.continuous) or not (p.state and p.state.value in ACTIVE_PIPELINE_STATES):
                return None
            for u in p.latest_updates or []:
                if u.state and u.state.value not in FINAL_UPDATE_STATES:
                    update = self.w.pipelines.get_update(pipeline_id=info.pipeline_id, update_id=u.update_id).update
                    return info.pipeline_id, u.update_id, (update.cause.value if update and update.cause else None)
        except Exception:
            pass
        return None

    # ---- check -------------------------------------------------------------------------------------------------
    def check(self, tables, refresh_window_mins: int, now: datetime, producers: dict = None, refreshed_now=()):
        """Return (cutoff, [SourceStatus]) for `tables` against the window ending at `now`.

        `refreshed_now`: tables the calling upstream job has just processed (its earlier tasks completed), counted as
        refreshed at `now` even if no rows changed and the job's run has not finished yet.
        """
        now = _naive_utc(now)
        cutoff = now - timedelta(minutes=refresh_window_mins)
        statuses = []
        for t in tables:
            last_write, operation, impacted, error = self.last_write(t)
            producer_type, producer_id = self.resolve_producer(t, producers)
            success, signal, completed = None, None, None
            if t in refreshed_now:
                success, signal = now, REFRESHED_NOW_SIGNAL
                # actual completion from the event log (real time; the events system table lags), else now
                logged = self._from_event_log(t, producer_id) if producer_type == "PIPELINE" else None
                completed = logged[0] if logged else now
            elif producer_type == "PIPELINE":
                success, signal = self.pipeline_last_success(t, producer_id)
            elif producer_type == "JOB":
                success, signal = self.job_last_success(producer_id)
            candidates = [x for x in (last_write, success) if x is not None]
            refreshed_at = max(candidates) if candidates else None
            statuses.append(SourceStatus(
                table=t, last_write=last_write, producer_type=producer_type, producer_id=producer_id,
                producer_success=success, producer_signal=signal, refreshed_at=refreshed_at,
                is_fresh=refreshed_at is not None and refreshed_at >= cutoff, error=error,
                last_write_operation=operation, last_write_rows=impacted, cutoff=cutoff, now=now,
                window_mins=refresh_window_mins,
                data_refreshed_at=max([x for x in (last_write, completed) if x], default=None) if completed
                else refreshed_at))
        return cutoff, statuses
