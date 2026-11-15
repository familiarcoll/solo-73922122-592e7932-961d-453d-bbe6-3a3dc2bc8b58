"""Lock-wait diagnostics on a single dedicated connection.

For every replay round one background sampler thread snapshots
``pg_stat_activity`` (joined with ``pg_blocking_pids``) for the backends
currently running replay requests, so the on-call engineer can see *who*
blocks the nightly settlement reads: wait event type/name of the waiting
request plus the direct blocker's PID, transaction start and SQL.

Hard guarantees:

* At most ONE extra connection is ever used (never the two pooled worker
  connections).  It is opened lazily, reused across samples, and only
  replaced after the previous one has been closed.
* The sampler runs on its own thread and shares no connection object with
  the engine, so a stuck or broken diagnostic connection can never delay
  the 3s request deadline, round cancellation, state reads, or returning
  worker connections to the pool.
* Every sample is tagged with the in-flight ownership token captured
  *before* the query ran; the engine re-validates that token under the run
  lock when the rows come back, so a late result can never be attributed
  to a different request (connection reuse) or to a later round.
"""

from __future__ import annotations

import threading
import time

import psycopg
from psycopg.rows import dict_row

# How often the sampler queries pg_stat_activity while requests run.
SAMPLE_INTERVAL_SECONDS = 0.2
# While no request is in flight the sampler probes its connection at this
# cadence, so "nothing to sample" stays distinguishable from "diagnostics
# down" (a dead connection turns the run snapshot "unavailable").
IDLE_PROBE_INTERVAL_SECONDS = 1.0
# Pause before reconnecting after a diagnostic failure.
RETRY_INTERVAL_SECONDS = 1.0
# Limits so a hung diagnostic connection fails fast instead of lingering.
CONNECT_TIMEOUT_SECONDS = 2
DIAG_STATEMENT_TIMEOUT_MS = 1500
# If no sample succeeded within this window the run snapshot reports the
# diagnostics as unavailable/stale (covers a silently stuck connection).
STALE_SECONDS = 3.0

# Direct blocker = first PID of pg_blocking_pids(); wait event type/name and
# the blocker's transaction start + last SQL come from pg_stat_activity.
SAMPLE_SQL = """
SELECT
    a.pid                  AS pid,
    clock_timestamp()      AS sampled_at,
    a.wait_event_type      AS wait_event_type,
    a.wait_event           AS wait_event,
    a.state                AS state,
    b.pid                  AS blocker_pid,
    b.xact_start           AS blocker_xact_start,
    b.query                AS blocker_query,
    b.usename              AS blocker_user,
    b.application_name     AS blocker_app,
    b.state                AS blocker_state
FROM pg_stat_activity AS a
LEFT JOIN pg_stat_activity AS b
       ON b.pid = (pg_blocking_pids(a.pid))[1]
WHERE a.pid = ANY(%s)
"""


class LockDiagnoser:
    """Background lock-wait sampler bound to one replay round."""

    def __init__(self, run, dsn: str, connect_factory=None):
        self._run = run
        self._dsn = dsn
        self._connect_factory = connect_factory or psycopg.connect
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._conn = None
        self._last_probe_mono = 0.0

    # ---------------------------------------------------------- lifecycle

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._loop, name="lock-diag", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Ask the sampler to exit; never blocks the engine for long.

        The diagnostic connection is closed by the sampler thread itself
        (its only legal owner).  If the thread is stuck in a socket read we
        simply stop waiting for it: it is a daemon thread and the stale
        connection is cleaned up when the process exits.
        """
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1.5)

    # ------------------------------------------------------------- loop

    def _loop(self) -> None:
        try:
            while not self._stop.is_set():
                # Ownership snapshot: {request_id: (inflight_seq, pid)}.
                targets = self._run.diag_targets()
                if not targets:
                    # Idle: probe the diagnostic connection occasionally so
                    # a dead/stuck connection is reported "unavailable"
                    # instead of silently looking like "nothing to sample".
                    now = time.monotonic()
                    if now - self._last_probe_mono >= IDLE_PROBE_INTERVAL_SECONDS:
                        if not self._attempt(lambda conn: self._sample(conn, [])):
                            self._stop.wait(RETRY_INTERVAL_SECONDS)
                            continue
                        self._last_probe_mono = time.monotonic()
                    self._stop.wait(0.1)
                    continue
                pids = sorted({pid for _, pid in targets.values()})
                rows = []

                def _do(conn):
                    nonlocal rows
                    rows = self._sample(conn, pids)

                if not self._attempt(_do):
                    self._stop.wait(RETRY_INTERVAL_SECONDS)
                    continue
                # Late results are dropped inside if ownership changed.
                self._run.record_diag_samples(targets, rows)
                self._stop.wait(SAMPLE_INTERVAL_SECONDS)
        finally:
            self._close_conn()

    def _attempt(self, fn) -> bool:
        """Run one diagnostic query on the dedicated connection.

        On success the run is marked healthy; on any failure it is marked
        unavailable and the connection is dropped (rebuilt next time).
        Never propagates exceptions: the engine must not feel diagnostics.
        """
        try:
            conn = self._ensure_conn()
            fn(conn)
        except Exception as exc:
            self._run.diag_mark_unavailable(f"{type(exc).__name__}: {exc}")
            self._close_conn()
            return False
        self._run.diag_mark_ok()
        return True

    # -------------------------------------------------------- connection

    def _ensure_conn(self):
        if self._conn is not None and not self._conn.closed:
            return self._conn
        conn = self._connect_factory(
            self._dsn,
            autocommit=True,
            connect_timeout=CONNECT_TIMEOUT_SECONDS,
            row_factory=dict_row,
        )
        try:
            conn.execute(
                f"SET statement_timeout = {DIAG_STATEMENT_TIMEOUT_MS}")
            conn.execute("SET application_name = 'lock-diag'")
        except Exception:
            try:
                conn.close()
            finally:
                raise
        self._conn = conn
        self._run.diag_note_reconnect()
        return conn

    @staticmethod
    def _sample(conn, pids: list[int]):
        with conn.cursor() as cur:
            cur.execute(SAMPLE_SQL, (pids,))
            return cur.fetchall()

    def _close_conn(self) -> None:
        conn, self._conn = self._conn, None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
