"""Shared source-freshness logic for Source Freshness Check and Job Pipeline Launch.

A source table counts as refreshed at the later of:
  1. its last data write (DESCRIBE HISTORY, data operations only), and
  2. its producer's last successful run — even if that run changed no rows (e.g. an SCD2 / AUTO CDC update with
     nothing new writes no table version, but the pipeline did process the table):
       PIPELINE producer (pipeline-managed streaming table / materialized view):
         a) system pipeline events table — latest COMPLETED flow_progress for the table's uc_table_id
         b) the pipeline's event log, event_log(<pipeline_id>) — latest COMPLETED flow writing the table
         c) Pipelines API — latest COMPLETED update that covered the table (whole pipeline or selected)
       JOB producer (table written by a job, from the tracker's src_table_producers): last successful job run.
Tables passed as `refreshed_now` (just processed by the upstream job calling the check) count as refreshed now.
Tables with no known producer use the last data write only.

All timestamps are naive UTC.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

# Delta operations that represent an actual write of data (excludes OPTIMIZE, VACUUM, SET TBLPROPERTIES, ...)
WRITE_OPERATIONS = [
    "WRITE", "MERGE", "DELETE", "UPDATE", "STREAMING UPDATE",
    "CREATE TABLE AS SELECT", "REPLACE TABLE AS SELECT",
    "CREATE OR REPLACE TABLE AS SELECT",
]
# Beta location; moves to system.lakeflow.pipeline_events at GA
PIPELINE_EVENTS_TABLE = "system.lakeflow_pipeline_events_preview.pipeline_events"
# operationMetrics that count rows impacted by a write (WRITE / MERGE / UPDATE / DELETE / STREAMING UPDATE)
ROW_METRICS = ("numOutputRows", "numTargetRowsInserted", "numTargetRowsUpdated", "numTargetRowsDeleted",
               "numUpdatedRows", "numDeletedRows", "numCopiedRows")
REFRESHED_NOW_SIGNAL = "refreshed by calling upstream job"
# How far back to look for a producer's last success (the window check is applied afterwards)
PRODUCER_LOOKBACK = timedelta(days=31)


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
        lines = [f"{verdict} {self.table}: refreshed {age} (window {self.window_mins} min, cutoff {self.cutoff})"]
        if self.last_write:
            rows = ", ".join(f"{k}={v}" for k, v in (self.last_write_rows or {}).items()) or "0 rows"
            lines.append(f"last write: {self.last_write_operation} at {self.last_write} ({rows})")
        elif self.last_write_operation == "MATERIALIZED_VIEW":
            lines.append("last write: materialized view — no Delta history; refresh and rows from its producer pipeline")
        else:
            lines.append("last write: none found")
        producer = f"{self.producer_type} {self.producer_id or ''}".rstrip()
        if self.producer_success is None:
            lines.append(f"producer: {producer}" + (f" — {self.producer_signal}" if self.producer_signal else ""))
        elif self.last_write_operation == "MATERIALIZED_VIEW":
            lines.append(f"producer: {producer} refreshed it at {self.producer_success} [{self.producer_signal}]")
        elif self.cutoff and self.producer_success >= self.cutoff and (not self.last_write or self.last_write < self.cutoff):
            # only the producer run refreshed it: it ran successfully but wrote nothing within the window
            lines.append(f"producer: {producer} ran successfully at {self.producer_success} "
                         f"[{self.producer_signal}] — no write within the window (run changed no rows)")
        else:
            lines.append(f"producer: {producer} last success {self.producer_success} [{self.producer_signal}]")
        if self.error:
            lines.append(f"error: {self.error}")
        return "\n          ".join(lines)


def _age(delta: timedelta) -> str:
    minutes = delta.total_seconds() / 60
    if minutes < 120:
        return f"{minutes:.1f} min"
    if minutes < 2 * 24 * 60:
        return f"{minutes / 60:.1f} h"
    return f"{minutes / 1440:.1f} days"


def rows_ok(rows) -> bool:
    return bool(rows) and rows[0]["t"] is not None


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
        self._pipeline_success = {}
        self._since = datetime.now(timezone.utc).replace(tzinfo=None) - PRODUCER_LOOKBACK

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
                        .orderBy(history.timestamp.desc()).limit(1).collect())
                if rows:
                    metrics = rows[0]["operationMetrics"] or {}
                    impacted = {k: metrics[k] for k in ROW_METRICS if metrics.get(k) not in (None, "0")}
                    self._last_write[table] = (_naive_utc(rows[0]["timestamp"]), rows[0]["operation"], impacted, None)
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
                for run in self.w.jobs.list_runs(job_id=int(job_id), completed_only=True, limit=25):
                    if run.state and run.state.result_state and run.state.result_state.value == "SUCCESS":
                        result = (_naive_utc(run.end_time), f"job run {run.run_id}")
                        break
            except Exception as e:
                result = (None, f"jobs.list_runs failed: {e}")
            self._job_success[job_id] = result or (None, "no successful job run found")
        return self._job_success[job_id]

    def pipeline_last_success(self, table: str, pipeline_id: str):
        """Latest time the pipeline successfully processed `table` (with or without row changes)."""
        key = (table, pipeline_id)
        since = self._since
        if key not in self._pipeline_success:
            self._pipeline_success[key] = (self._from_events_table(table, since)
                                           or self._from_event_log(table, pipeline_id, since)
                                           or self._from_pipeline_updates(table, pipeline_id)
                                           or (None, "no completed pipeline run found"))
        return self._pipeline_success[key]

    def _from_events_table(self, table, since):
        if self._events_table_available is False:
            return None
        info = self.table_info(table)
        if info is None or not info.table_id:
            return None
        try:
            row = self.spark.sql(
                f"""
                SELECT max(event_time) AS t
                FROM {self.pipeline_events_table}
                WHERE event_type = 'flow_progress'
                  AND variant_get(details, '$.flow_progress.status', 'STRING') = 'COMPLETED'
                  AND origin.uc_table_id = :table_id
                  AND event_time >= :since
                """,
                args={"table_id": info.table_id, "since": since},
            ).collect()[0]
            self._events_table_available = True
            return (_naive_utc(row["t"]), "pipeline_events system table") if row["t"] else None
        except Exception:
            self._events_table_available = False  # not enabled / no access — use the next signal
            return None

    def _from_event_log(self, table, pipeline_id, since):
        """Latest COMPLETED flow writing `table`, plus the rows that flow run reported (summed flow_progress metrics)."""
        if not all(c.isalnum() or c == "-" for c in pipeline_id):
            return None
        try:
            row = self.spark.sql(
                f"""
                WITH flows AS (  -- flow -> table it writes (AUTO CDC / append flows are named differently)
                  SELECT DISTINCT origin.flow_name AS flow_name,
                         replace(details:flow_definition.output_dataset::string, '`', '') AS dataset
                  FROM event_log('{pipeline_id}')
                  WHERE event_type = 'flow_definition'
                ),
                progress AS (
                  SELECT e.timestamp, e.origin.update_id AS update_id, e.origin.flow_name AS flow_name,
                         e.details:flow_progress.status::string AS status,
                         coalesce(e.details:flow_progress.metrics.num_output_rows::bigint, 0) AS output_rows,
                         coalesce(e.details:flow_progress.metrics.num_upserted_rows::bigint, 0) AS upserted_rows,
                         coalesce(e.details:flow_progress.metrics.num_deleted_rows::bigint, 0) AS deleted_rows
                  FROM event_log('{pipeline_id}') e
                  JOIN flows f ON e.origin.flow_name = f.flow_name
                  WHERE e.event_type = 'flow_progress'
                    AND e.timestamp >= :since
                    AND (f.dataset = :table OR :table LIKE concat('%.', f.dataset))
                ),
                last_done AS (
                  SELECT max_by(update_id, timestamp) AS update_id, max(timestamp) AS t
                  FROM progress WHERE status = 'COMPLETED'
                )
                SELECT l.t, l.update_id, sum(p.output_rows) AS output_rows, sum(p.upserted_rows) AS upserted_rows,
                       sum(p.deleted_rows) AS deleted_rows
                FROM last_done l LEFT JOIN progress p ON p.update_id = l.update_id
                GROUP BY l.t, l.update_id
                """,
                args={"table": table, "since": since},
            ).collect()
            if not rows_ok(row):
                return None
            r = row[0]
            counts = ", ".join(f"{k}={r[k]}" for k in ("output_rows", "upserted_rows", "deleted_rows") if r[k])
            return _naive_utc(r["t"]), f"pipeline event log, update {r['update_id']}: {counts or '0 rows'}"
        except Exception:
            return None  # no access to the event log — use the next signal

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

    def producer_running(self, table: str) -> bool:
        """True if the table's producing pipeline has an update in progress (the table is about to be refreshed)."""
        info = self.table_info(table)
        if info is None or not info.pipeline_id:
            return False
        try:
            state = self.w.pipelines.get(pipeline_id=info.pipeline_id).state
            return bool(state and state.value in ("RUNNING", "STARTING", "DEPLOYING", "RESETTING"))
        except Exception:
            return False

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
            success, signal = None, None
            if t in refreshed_now:
                success, signal = now, REFRESHED_NOW_SIGNAL
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
                window_mins=refresh_window_mins))
        return cutoff, statuses
