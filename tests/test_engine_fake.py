"""Engine concurrency tests with a fake pool / fake connections.

Run with:  python3 -I tests/test_engine_fake.py
No third-party packages are needed: minimal psycopg / psycopg_pool stubs
are injected before importing the real engine module.
"""

from __future__ import annotations

import sys
import threading
import time
import types
from pathlib import Path

# ---- inject minimal psycopg / psycopg_pool stubs -------------------------

psycopg = types.ModuleType("psycopg")
errors = types.ModuleType("psycopg.errors")


class _PgError(Exception):
    sqlstate = None


class QueryCanceled(_PgError):
    sqlstate = "57014"


errors.QueryCanceled = QueryCanceled
psycopg.errors = errors
psycopg.Error = _PgError


class _StandaloneConn:
    """Sibling connection used only to issue pg_cancel_backend()."""

    def cursor(self):
        # No owning fake conn: route through the global registry by pid.
        return FakeCursor(self)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def close(self):
        pass


def _connect(dsn=None, connect_timeout=None, autocommit=False, **kwargs):
    return _StandaloneConn()


psycopg.connect = _connect


# lockdiag.py does `from psycopg.rows import dict_row`; provide the submodule.
rows_mod = types.ModuleType("psycopg.rows")


def _dict_row(*args, **kwargs):
    return None


rows_mod.dict_row = _dict_row
psycopg.rows = rows_mod
sys.modules["psycopg.rows"] = rows_mod


class PoolTimeout(Exception):
    pass


pool_mod = types.ModuleType("psycopg_pool")
pool_mod.PoolTimeout = PoolTimeout
sys.modules["psycopg"] = psycopg
sys.modules["psycopg.errors"] = errors
sys.modules["psycopg_pool"] = pool_mod

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "app"))

import engine  # noqa: E402

TEMPLATES = {
    "fast": "SELECT 1",
    # Blocks until the whole round is canceled (ignores deadline cancel),
    # keeping its connection held past other requests' deadlines.
    "slow": "SELECT pg_sleep_forever",
    # Blocks until conn.cancel() is issued (watchdog timeout or round cancel).
    "slow_cancelable": "SELECT pg_sleep(10)",
    "boom": "SELECT broken",
}


class QueryCanceledExc(Exception):
    sqlstate = "57014"


# Alias to the injected stub class so the engine's except clause catches it.
QueryCanceledExc = errors.QueryCanceled

# Registry of all fake connections keyed by backend pid, used to model
# pg_cancel_backend() issued from a sibling connection.
FAKE_REGISTRY: dict[int, "FakeConn"] = {}
FAKE_DSN = "postgresql://stub/stub"


class FakeInfo:
    def __init__(self, backend_pid):
        self.backend_pid = backend_pid
        self.dsn = FAKE_DSN


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, params=None):
        # The engine's cancel path calls pg_cancel_backend(<pid>) on a sibling
        # connection; model it as signaling the target fake connection.
        if isinstance(sql, str) and sql.startswith("SELECT pg_cancel_backend"):
            pid = params[0]
            target = next((c for c in FAKE_REGISTRY.values()
                           if c.info.backend_pid == pid), None)
            if target is not None:
                target._signal_cancel()
            return
        self.conn.running.set()
        try:
            if sql == TEMPLATES["boom"]:
                raise engine.psycopg.Error("syntax boom")
            if sql == TEMPLATES["slow_cancelable"]:
                # Block until THIS connection's cancel gate fires.
                if not self.conn.cancel_gate.wait(timeout=8):
                    raise AssertionError("slow query was never cancelled")
                raise errors.QueryCanceled(
                    "canceling statement due to user request")
            if sql == TEMPLATES["slow"]:
                # Only a round-level cancellation releases this statement;
                # the watchdog's deadline cancel leaves it holding the conn.
                if not self.conn.round_cancel_gate.wait(timeout=8):
                    raise AssertionError("slow query was never round-canceled")
                raise errors.QueryCanceled(
                    "canceling statement due to round cancel")
            time.sleep(0.01)  # fast query
        finally:
            self.conn.running.clear()

    def fetchall(self):
        return [(1,)]

    def close(self):
        pass


class FakeConn:
    _seq = 0
    _seq_lock = threading.Lock()

    def __init__(self):
        with FakeConn._seq_lock:
            FakeConn._seq += 1
            self.ident = FakeConn._seq
        self.info = FakeInfo(backend_pid=10000 + self.ident)
        self.replay_dsn = FAKE_DSN
        FAKE_REGISTRY[self.info.backend_pid] = self
        self.checked_out = False
        # cancel_gate models conn.cancel() (watchdog timeout OR round cancel).
        self.cancel_gate = threading.Event()
        # round_cancel_gate is set only when the whole round is canceled, so
        # the "slow" statement can be made to ignore deadline-driven cancels
        # and keep holding its connection until the round ends.
        self.round_cancel_gate = threading.Event()
        self.running = threading.Event()
        self.rollback_count = 0
        self.cancels = 0
        self.discarded = False

    def cursor(self):
        return FakeCursor(self)

    def _signal_cancel(self):
        self.cancels += 1
        self.cancel_gate.set()

    def cancel(self):
        self._signal_cancel()

    def rollback(self):
        self.rollback_count += 1

    def reset_for_reuse(self):
        # Fresh gates for the next owner so a stale cancel cannot hit it.
        # Counters are cumulative across reuses on purpose.
        self.cancel_gate = threading.Event()
        self.round_cancel_gate = threading.Event()
        self.running = threading.Event()


class FakePool:
    def __init__(self, size):
        self.size = size
        self._sem = threading.BoundedSemaphore(size)
        self._conns = [FakeConn() for _ in range(size)]
        self.peak_in_use = 0
        self.in_use = 0
        self._lock = threading.Lock()

    def getconn(self, timeout=None):
        if not self._sem.acquire(timeout=timeout):
            raise PoolTimeout("no connection available")
        with self._lock:
            self.in_use += 1
            self.peak_in_use = max(self.peak_in_use, self.in_use)
        conn = next(c for c in self._conns if not c.checked_out)
        conn.checked_out = True
        conn.reset_for_reuse()
        return conn

    def putconn(self, conn, discard=False):
        conn.checked_out = False
        conn.discarded = discard
        with self._lock:
            self.in_use -= 1
        self._sem.release()


def wait_terminal(mgr, timeout=15.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        snap = mgr.snapshot()
        if snap["status"] in ("completed", "canceled"):
            return snap
        time.sleep(0.02)
    raise AssertionError("run did not finish in time")


# ---- fake diagnostic connection (lockdiag.LockDiagnoser) -----------------


class FakeDiagCursor:
    def __init__(self, rows_supplier):
        self._rows_supplier = rows_supplier

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        pass

    def fetchall(self):
        return self._rows_supplier()


class FakeDiagConn:
    """Stand-in for the single dedicated diagnostic connection."""

    def __init__(self, rows_supplier):
        self._rows_supplier = rows_supplier
        self.closed = False
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append(sql)

    def cursor(self):
        return FakeDiagCursor(self._rows_supplier)

    def close(self):
        self.closed = True


def make_diag_factory(rows_supplier, calls=None):
    def _factory(dsn, **kwargs):
        if calls is not None:
            calls.append(dsn)
        return FakeDiagConn(rows_supplier)
    return _factory


def blocked_row(pid, blocker_pid=4242):
    return {
        "pid": pid,
        "sampled_at": "2026-11-15T01:00:00.123456+00:00",
        "wait_event_type": "Lock",
        "wait_event": "relation",
        "state": "active",
        "blocker_pid": blocker_pid,
        "blocker_xact_start": "2026-11-15T00:59:00+00:00",
        "blocker_query": "LOCK TABLE sales IN ACCESS EXCLUSIVE MODE",
        "blocker_user": "replay",
        "blocker_app": "psql",
        "blocker_state": "idle in transaction",
    }


class TestRunManager(engine.RunManager):
    """RunManager whose round cancel also releases the 'slow' test gate."""

    def cancel(self):
        for conn in self._pool._conns:
            conn.round_cancel_gate.set()
        return super().cancel()


def by_id(snap):
    return {r["id"]: r for r in snap["requests"]}


def test_fast_requests_succeed_and_pool_caps_at_two():
    pool = FakePool(2)
    mgr = engine.RunManager(pool, TEMPLATES)
    rows = [{"arrival_ms": i * 50, "template": "fast", "store": str(i)}
            for i in range(6)]
    mgr.start(rows)
    snap = wait_terminal(mgr)
    assert snap["status"] == "completed"
    assert all(r["status"] == "success" for r in snap["requests"]), snap
    assert pool.peak_in_use <= 2, pool.peak_in_use
    assert pool.in_use == 0, "all connections must be returned"
    used = [c for c in pool._conns if c.rollback_count]
    assert used, "at least one connection should have been used"
    assert all(c.rollback_count >= 1 for c in used), "cleanup before return"
    print("OK test_fast_requests_succeed_and_pool_caps_at_two")


def test_queue_timeout_never_executes():
    pool = FakePool(2)
    mgr = TestRunManager(pool, TEMPLATES)
    # Two slow queries hold both connections and do not release them within
    # the 3s window.  The third request can never acquire a connection before
    # its own deadline, so it must expire in the queue and never execute.
    rows = [
        {"arrival_ms": 0, "template": "slow", "store": "1"},
        {"arrival_ms": 0, "template": "slow", "store": "2"},
        {"arrival_ms": 200, "template": "fast", "store": "3"},
    ]
    mgr.start(rows)
    time.sleep(4.0)  # well past request 3's deadline of planned(0.2)+3s
    r3 = by_id(mgr.snapshot())[3]
    assert r3["status"] == "timeout", r3
    assert r3["exec_ms"] in (0.0, None), r3
    assert "queued" in (r3["error"] or "")
    mgr.cancel()
    snap = wait_terminal(mgr)
    assert snap["status"] == "canceled"
    print("OK test_queue_timeout_never_executes")


def test_running_request_stopped_on_deadline_then_conn_cleaned():
    pool = FakePool(2)
    mgr = engine.RunManager(pool, TEMPLATES)
    # slow_cancelable responds to the watchdog's conn.cancel() at the 3s mark.
    mgr.start([{"arrival_ms": 0, "template": "slow_cancelable", "store": "9"}])
    snap = wait_terminal(mgr, timeout=12)
    r1 = by_id(snap)[1]
    assert r1["status"] == "timeout", r1
    assert r1["sqlstate"] == "57014"
    conn = next(c for c in pool._conns if c.cancels)
    assert conn.rollback_count >= 1, "rollback before returning"
    assert not conn.checked_out, "connection returned after cleanup"
    print("OK test_running_request_stopped_on_deadline_then_conn_cleaned")


def test_cancel_stops_inflight_and_blocks_new_starts():
    pool = FakePool(2)
    mgr = TestRunManager(pool, TEMPLATES)
    rows = [
        {"arrival_ms": 0, "template": "slow", "store": "1"},
        # These two are scheduled only AFTER the cancel below, so the cancel
        # sweep must keep them from ever starting.
        {"arrival_ms": 5000, "template": "fast", "store": "2"},
        {"arrival_ms": 5100, "template": "fast", "store": "3"},
    ]
    mgr.start(rows)
    time.sleep(0.3)
    mgr.cancel()
    snap = wait_terminal(mgr, timeout=10)
    assert snap["status"] == "canceled"
    statuses = {r["id"]: r["status"] for r in snap["requests"]}
    assert statuses[1] == "canceled", statuses
    # Not-yet-started requests must never run.
    assert statuses[3] == "canceled", statuses
    assert pool.in_use == 0
    print("OK test_cancel_stops_inflight_and_blocks_new_starts")


def test_late_cancel_does_not_stop_next_request_on_same_conn():
    pool = FakePool(2)
    mgr = TestRunManager(pool, TEMPLATES)
    rows = [
        {"arrival_ms": 0, "template": "slow", "store": "1"},
        {"arrival_ms": 4000, "template": "fast", "store": "2"},
    ]
    mgr.start(rows)
    time.sleep(0.5)
    mgr.cancel()  # stops request 1 before request 2 is dispatched
    snap = wait_terminal(mgr, timeout=12)
    statuses = {r["id"]: r["status"] for r in snap["requests"]}
    assert statuses[1] == "canceled", statuses
    assert statuses[2] == "canceled", statuses  # never started

    # A brand-new round reusing the same pool: a fast query on the previously
    # canceled physical connection must still succeed (no cancel bleed). This
    # is the real guarantee: getconn() resets the cancel gates for the new
    # owner, so the late cancel of request 1 cannot stop the new request.
    mgr2 = engine.RunManager(pool, TEMPLATES)
    mgr2.start([{"arrival_ms": 0, "template": "fast", "store": "2"}])
    snap2 = wait_terminal(mgr2)
    assert by_id(snap2)[1]["status"] == "success", snap2
    print("OK test_late_cancel_does_not_stop_next_request_on_same_conn")


def test_sql_error_reported_and_connection_recovered():
    pool = FakePool(2)
    mgr = engine.RunManager(pool, TEMPLATES)
    rows = [
        {"arrival_ms": 0, "template": "boom", "store": "1"},
        {"arrival_ms": 50, "template": "fast", "store": "2"},
    ]
    mgr.start(rows)
    snap = wait_terminal(mgr)
    reqs = by_id(snap)
    assert reqs[1]["status"] == "error" and "syntax boom" in reqs[1]["error"]
    assert reqs[2]["status"] == "success", reqs
    assert pool.in_use == 0
    print("OK test_sql_error_reported_and_connection_recovered")


def test_burst_of_30_caps_at_two_connections_and_terminates_all():
    pool = FakePool(2)
    mgr = engine.RunManager(pool, TEMPLATES)
    # All 30 requests arrive at t=0.  Only two run at a time; the rest wait
    # on the pool.  Fast queries (~10ms) mean the whole burst drains well
    # inside each request's 3s window, so all should succeed while the pool
    # never exceeds two checked-out connections.
    rows = [{"arrival_ms": 0, "template": "fast", "store": str(i)}
            for i in range(30)]
    mgr.start(rows)
    snap = wait_terminal(mgr, timeout=20)
    assert snap["status"] == "completed", snap["status"]
    assert len(snap["requests"]) == 30
    assert all(r["status"] == "success" for r in snap["requests"]), \
        [r["status"] for r in snap["requests"]]
    assert pool.peak_in_use == 2, pool.peak_in_use
    assert pool.in_use == 0
    # Every request carries a measured (possibly zero) queue duration.
    assert all(r["queue_ms"] is not None for r in snap["requests"])
    print("OK test_burst_of_30_caps_at_two_connections_and_terminates_all")


def test_one_round_at_a_time():
    pool = FakePool(2)
    mgr = TestRunManager(pool, TEMPLATES)
    mgr.start([{"arrival_ms": 0, "template": "slow", "store": "1"}])
    try:
        mgr.start([{"arrival_ms": 0, "template": "fast", "store": "x"}])
        raise AssertionError("expected RunConflict")
    except engine.RunConflict:
        pass
    mgr.cancel()
    wait_terminal(mgr)
    print("OK test_one_round_at_a_time")


def test_diag_samples_attributed_and_kept_after_finish():
    """Blocked samples land on the right request and survive its finish."""
    pool = FakePool(2)

    def rows_supplier():
        # Report a Lock/relation wait for every connection currently busy.
        return [blocked_row(c.info.backend_pid)
                for c in pool._conns if c.running.is_set()]

    mgr = engine.RunManager(pool, TEMPLATES, diag_dsn=FAKE_DSN,
                            diag_connect_factory=make_diag_factory(rows_supplier))
    # slow_cancelable runs until the 3s watchdog cancel -> ~3s of sampling.
    mgr.start([{"arrival_ms": 0, "template": "slow_cancelable", "store": "9"}])
    snap = wait_terminal(mgr, timeout=12)
    r1 = by_id(snap)[1]
    assert r1["status"] == "timeout", r1
    lw = r1["lock_wait"]
    assert lw["state"] == "sampled", lw
    assert lw["sample_count"] >= 3, lw  # ~3s at 0.2s interval
    b = lw["last_blocked"]
    assert b["blocker_pid"] == 4242, b
    assert b["wait_event_type"] == "Lock" and b["wait_event"] == "relation", b
    assert b["blocker_query"] == "LOCK TABLE sales IN ACCESS EXCLUSIVE MODE", b
    assert b["blocker_xact_start"].startswith("2026-11-15T00:59"), b
    assert b["sampled_at"], b
    assert snap["diag"]["available"] is True, snap["diag"]
    assert snap["diag"]["samples_taken"] >= 3, snap["diag"]
    assert pool.in_use == 0, "worker connections returned"
    print("OK test_diag_samples_attributed_and_kept_after_finish")


def test_late_diag_sample_never_attributed_after_reuse():
    """A sample captured for one owner must not leak into the next request
    re-using the same connection, nor into a finished request."""
    pool = FakePool(1)  # single connection: forces reuse
    mgr = engine.RunManager(pool, TEMPLATES)  # no diagnoser: drive manually
    rows = [
        {"arrival_ms": 0, "template": "slow_cancelable", "store": "1"},
        {"arrival_ms": 3300, "template": "slow_cancelable", "store": "2"},
    ]
    mgr.start(rows)
    run = mgr._run

    # Wait until request 1 is executing, then capture the ownership token.
    deadline = time.time() + 5
    while time.time() < deadline:
        targets = run.diag_targets()
        if targets:
            break
        time.sleep(0.02)
    assert targets, "request 1 never went in-flight"
    req1_id, (seq1, pid1) = next(iter(targets.items()))
    assert req1_id == 1

    # Wait until request 2 is executing on the SAME connection (reuse).
    deadline = time.time() + 12
    while time.time() < deadline:
        targets2 = run.diag_targets()
        if 2 in targets2:
            break
        time.sleep(0.02)
    assert 2 in targets2, "request 2 never went in-flight"
    seq2, pid2 = targets2[2]
    assert pid2 == pid1, "expected the same physical connection to be reused"
    assert seq2 != seq1

    # The stale sample (captured for request 1's ownership) arrives late,
    # while request 2 owns the connection: it must be dropped entirely.
    run.record_diag_samples(targets, [blocked_row(pid1)])
    reqs = by_id(mgr.snapshot())
    assert reqs[1]["lock_wait"]["sample_count"] == 0, reqs[1]["lock_wait"]
    assert reqs[2]["lock_wait"]["sample_count"] == 0, reqs[2]["lock_wait"]

    # A fresh sample taken for request 2's ownership DOES land on request 2.
    run.record_diag_samples(targets2, [blocked_row(pid2)])
    reqs = by_id(mgr.snapshot())
    assert reqs[2]["lock_wait"]["sample_count"] == 1, reqs[2]["lock_wait"]
    assert reqs[1]["lock_wait"]["sample_count"] == 0, reqs[1]["lock_wait"]

    snap = wait_terminal(mgr, timeout=15)
    reqs = by_id(snap)
    # Evidence collected for request 2 is kept after it finished.
    assert reqs[2]["lock_wait"]["state"] == "sampled", reqs[2]["lock_wait"]
    assert reqs[2]["lock_wait"]["sample_count"] == 1
    print("OK test_late_diag_sample_never_attributed_after_reuse")


def test_unknown_when_sampler_ran_but_collected_nothing():
    """Diagnostics healthy but no sample for the request -> 'unknown',
    never 'no blocking'."""
    pool = FakePool(2)
    mgr = engine.RunManager(
        pool, TEMPLATES, diag_dsn=FAKE_DSN,
        diag_connect_factory=make_diag_factory(lambda: []))  # empty samples
    mgr.start([{"arrival_ms": 0, "template": "slow_cancelable", "store": "9"}])
    snap = wait_terminal(mgr, timeout=12)
    r1 = by_id(snap)[1]
    assert r1["status"] == "timeout", r1
    lw = r1["lock_wait"]
    assert lw["state"] == "unknown", lw
    assert lw["sample_count"] == 0
    assert snap["diag"]["available"] is True, snap["diag"]
    print("OK test_unknown_when_sampler_ran_but_collected_nothing")


def test_diag_failure_marks_unavailable_without_blocking_engine():
    """A broken diagnostic connection shows 'unavailable' and must not
    delay deadlines, cancels, state reads, or connection returns."""
    pool = FakePool(2)

    def boom_factory(dsn, **kwargs):
        raise RuntimeError("diag connection refused")

    mgr = engine.RunManager(pool, TEMPLATES, diag_dsn=FAKE_DSN,
                            diag_connect_factory=boom_factory)
    rows = [
        {"arrival_ms": 0, "template": "fast", "store": "1"},
        {"arrival_ms": 0, "template": "slow_cancelable", "store": "2"},
    ]
    mgr.start(rows)
    snap = wait_terminal(mgr, timeout=12)
    reqs = by_id(snap)
    # Engine semantics fully intact despite the broken diagnoser.
    assert reqs[1]["status"] == "success", reqs[1]
    assert reqs[2]["status"] == "timeout", reqs[2]  # 3s deadline enforced
    assert reqs[2]["exec_ms"] < 4000, reqs[2]       # not delayed by diag
    assert pool.in_use == 0, "worker connections returned"
    assert snap["diag"]["available"] is False, snap["diag"]
    assert snap["diag"]["note"], snap["diag"]
    # Executed requests with no evidence are 'unavailable', not 'no blocking'.
    assert reqs[1]["lock_wait"]["state"] == "unavailable", reqs[1]["lock_wait"]
    assert reqs[2]["lock_wait"]["state"] == "unavailable", reqs[2]["lock_wait"]
    print("OK test_diag_failure_marks_unavailable_without_blocking_engine")


def test_not_run_and_queued_states_have_no_diag_claim():
    """Requests that never executed must not claim any lock diagnosis."""
    pool = FakePool(2)
    mgr = TestRunManager(pool, TEMPLATES)
    rows = [
        {"arrival_ms": 0, "template": "slow", "store": "1"},
        {"arrival_ms": 0, "template": "slow", "store": "2"},
        {"arrival_ms": 100, "template": "fast", "store": "3"},
    ]
    mgr.start(rows)
    time.sleep(3.5)  # request 3 expires in the queue without executing
    r3 = by_id(mgr.snapshot())[3]
    assert r3["status"] == "timeout", r3
    assert r3["lock_wait"]["state"] == "not_run", r3["lock_wait"]
    mgr.cancel()
    wait_terminal(mgr)
    print("OK test_not_run_and_queued_states_have_no_diag_claim")


if __name__ == "__main__":
    test_fast_requests_succeed_and_pool_caps_at_two()
    test_queue_timeout_never_executes()
    test_running_request_stopped_on_deadline_then_conn_cleaned()
    test_cancel_stops_inflight_and_blocks_new_starts()
    test_late_cancel_does_not_stop_next_request_on_same_conn()
    test_sql_error_reported_and_connection_recovered()
    test_burst_of_30_caps_at_two_connections_and_terminates_all()
    test_one_round_at_a_time()
    test_diag_samples_attributed_and_kept_after_finish()
    test_late_diag_sample_never_attributed_after_reuse()
    test_unknown_when_sampler_ran_but_collected_nothing()
    test_diag_failure_marks_unavailable_without_blocking_engine()
    test_not_run_and_queued_states_have_no_diag_claim()
    print("\nALL ENGINE TESTS PASSED")
