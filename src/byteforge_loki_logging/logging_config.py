# logging_config.py
import atexit
import json
import logging
import logging.handlers
import os
import sys
import threading
import time
import traceback
import weakref
from queue import Queue
from typing import Any, List, Optional, Set, Tuple, Union

import logging_loki
import requests  # type: ignore[import-untyped]
from logging.handlers import BufferingHandler
from logging_loki.emitter import LokiEmitter  # type: ignore[import-untyped]
from logging_loki.handlers import LokiBatchHandler


_atexit_registered = False

#: Every SafeLokiEmitter alive in this process, so a fork can reset them.
#: Weak so an emitter belonging to a discarded handler is not kept alive.
_LIVE_EMITTERS: "weakref.WeakSet[SafeLokiEmitter]" = weakref.WeakSet()


def _reset_emitters_after_fork() -> None:
    """Rebuild per-process emitter state in a freshly forked child.

    The PID-keyed session in SafeLokiEmitter.session handles the inherited
    socket, but it is never reached if the emitter's LOCK is inherited held:
    logging_loki guards its entry points with @with_lock, which does a
    non-blocking acquire and RETURNS WITHOUT POSTING when it fails. Fork copies
    the lock in whatever state it was in, and the thread that owned it does not
    exist in the child, so a fork that lands while the flush timer is mid-POST
    leaves the child silently discarding every batch forever. threading's own
    after-fork fixup does not cover it, because the lock belongs to the emitter
    rather than to a logging Handler.
    """
    for emitter in list(_LIVE_EMITTERS):
        emitter._lock = threading.Lock()
        emitter._session = None
        emitter._session_pid = None


if hasattr(os, "register_at_fork"):      # not available on Windows
    os.register_at_fork(after_in_child=_reset_emitters_after_fork)

#: Seconds any single POST to Loki may take before it is abandoned. Bounded
#: because logging_loki passes no timeout at all (see SafeLokiEmitter).
DEFAULT_PUSH_TIMEOUT = 10.0

#: Retry a push exactly once when the connection was dropped before we got a
#: response. Deliberately hand-rolled rather than a urllib3 Retry adapter, and
#: the reason is worth keeping: urllib3 wraps `RemoteDisconnected` into
#: `ProtocolError`, which `Retry._is_read_error` counts as a READ error, not a
#: connect error. So the intuitive `Retry(connect=1, read=0)` does NOT retry a
#: dropped socket (measured), while the `read=1` that does *also* retries read
#: TIMEOUTS — which doubles the time a black-holed endpoint takes to report
#: (see SafeLokiEmitter's push_timeout) and is the case where the server most
#: plausibly did process the batch, so retrying risks duplicates.
#:
#: requests separates the two for us: a dropped connection raises
#: ConnectionError, a slow one raises ReadTimeout. Retrying only the former is
#: both cheaper and safer, since a server that hung up without answering never
#: sent a response and most likely never committed the batch.
#:
#: The exclusion below is not redundant: ConnectTimeout subclasses BOTH
#: ConnectionError and Timeout, so catching ConnectionError alone would retry a
#: firewall-black-holed endpoint (SYN dropped, no SYN-ACK) and spend a second
#: full push_timeout on it — the very wedge push_timeout exists to bound.
_RETRYABLE_PUSH_ERROR = requests.exceptions.ConnectionError
_NON_RETRYABLE_PUSH_ERROR = requests.exceptions.Timeout


# Standard LogRecord attributes that should not be treated as extra fields
_STANDARD_RECORD_ATTRS: Set[str] = {
    'name', 'msg', 'args', 'created', 'filename', 'funcName', 'levelname',
    'levelno', 'lineno', 'module', 'msecs', 'pathname', 'process',
    'processName', 'relativeCreated', 'thread', 'threadName', 'exc_info',
    'exc_text', 'stack_info', 'message', 'taskName'
}


def _resolve_log_level(level: Union[int, str]) -> int:
    """Convert a log level (int or string name) to its integer value."""
    if isinstance(level, int):
        return level
    return getattr(logging, str(level).upper())


class LokiJsonFormatter(logging.Formatter):
    """JSON formatter for Loki that includes extra fields from log records.

    Outputs log records as JSON with structure:
    {
        "logger": "logger.name",
        "level": "INFO",
        "message": "Log message",
        "extra_field1": "value1",
        "extra_field2": "value2"
    }

    This enables structured queries in Loki like:
    {application="my-app"} | json | client_id="some-value"
    """

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "logger": record.name,
            "level": record.levelname,
            "message": record.getMessage()
        }

        for key, value in record.__dict__.items():
            if key not in _STANDARD_RECORD_ATTRS and not key.startswith('_'):
                try:
                    json.dumps(value)
                    log_data[key] = value
                except (TypeError, ValueError):
                    log_data[key] = str(value)

        return json.dumps(log_data)


class SafeLokiEmitter(LokiEmitter):
    """LokiEmitter that bounds every POST with a timeout.

    logging_loki's LokiEmitter._post_to_loki calls session.post() with no
    `timeout` argument, so requests waits forever. A Loki endpoint that accepts
    the connection and then never answers (black-holed DNS/route, a hung proxy,
    an endpoint cut over mid-flight) therefore parks the caller in that POST
    permanently.

    That is fatal here rather than merely slow, because the flush timer thread
    holds this handler's lock while it POSTs: once it hangs, the QueueListener
    thread blocks on the same lock, the queue grows without bound, and the
    process never ships another log line — with no exception raised anywhere,
    so nothing is printed and nothing recovers short of a restart. Reproduced
    against a black-holing test server: 21 records enqueued, 0 delivered, 0
    bytes on stderr, permanent.
    """

    push_timeout: float = DEFAULT_PUSH_TIMEOUT

    #: Incremented on every POST actually attempted. Upstream wraps the emitter
    #: entry points in @with_lock, which returns WITHOUT posting when the lock is
    #: held, so "emit_batch() did not raise" does not mean "the batch was sent".
    #: SafeLokiHandler compares this across a call to tell the two apart, rather
    #: than scoring a silently-skipped push as a delivery.
    post_attempts: int = 0

    #: PID that built the cached session. See the session property.
    _session_pid: Optional[int] = None

    #: Declared because logging_loki ships no type information, which otherwise
    #: leaves the inherited attribute untyped for the session property below.
    _session: Optional[requests.Session]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        _LIVE_EMITTERS.add(self)

    @property
    def session(self) -> requests.Session:
        """Return the cached session, rebuilding it if it belongs to another PID.

        requests.Session and urllib3's pools are not fork-safe: os.fork()
        duplicates the descriptor, so children of a process that has already
        pushed inherit a session pointing at a LIVE socket and all of them send
        and recv on it at once. The result is protocol chaos and a simultaneous
        RemoteDisconnected in every child — the reported signature was 16
        failures inside ~15ms, one per multiprocessing worker in an Optuna pool
        (ticket ac3c6ccd). Keying the session to the PID is the same guard
        psycopg2, SQLAlchemy and boto3 use.

        _reset_emitters_after_fork covers the case this cannot: an emitter lock
        inherited in the held state, which stops us ever reaching this property.

        The other half of that ticket — retrying a dropped connection — is in
        _post_to_loki, and no retry adapter is mounted here; see
        _RETRYABLE_PUSH_ERROR for why that is deliberate.
        """
        pid = os.getpid()
        if self._session is not None and self._session_pid == pid:
            return self._session

        if self._session is not None:
            # Inherited across a fork. Safe to close: fork duplicated the
            # descriptor, so closing our copy does not FIN the parent's socket.
            try:
                self._session.close()
            except Exception:
                pass

        session = self.session_class()
        session.auth = self.auth or None
        session.verify = self.verify
        self._session = session
        self._session_pid = pid
        return session

    def _post_to_loki(self, payload: dict) -> None:
        """POST the payload, retrying once if the connection was dropped.

        The retry covers the reported RemoteDisconnected bursts: a pooled
        keep-alive socket that the server closes as the request goes out. Note
        urllib3 already discards a socket that was closed *cleanly* while idle,
        so this is not "the connection went stale" — it is the race where the
        socket still looks alive when urllib3 checks it, the request is written,
        and only then does the server hang up. Without a retry that batch is
        simply lost.

        Timeouts are deliberately NOT retried — see _RETRYABLE_PUSH_ERROR.
        """
        try:
            self._post_once(payload)
        except _RETRYABLE_PUSH_ERROR as exc:
            if isinstance(exc, _NON_RETRYABLE_PUSH_ERROR):
                # ConnectTimeout is both. See _RETRYABLE_PUSH_ERROR.
                raise
            # Drop the pool so the retry gets a genuinely fresh connection
            # rather than the socket that just failed.
            self.close()
            self._post_once(payload)

    def _post_once(self, payload: dict) -> None:
        """One POST attempt, bounded by push_timeout."""
        self.post_attempts += 1
        resp = self.session.post(
            self.url, json=payload, headers=self.headers, timeout=self.push_timeout
        )
        if resp.status_code != self.success_response_code:
            raise ValueError(
                f"Unexpected Loki API response status code: {resp.status_code}"
            )


class SafeLokiHandler(logging_loki.LokiHandler):
    """LokiHandler wrapper that prevents recursive logging loops.

    Overrides handleError to print errors directly to stderr instead of using
    the logging system, which would cause infinite recursion.

    Also makes steady-state push failures visible and recoverable. Upstream
    LokiHandler.emit_batch catches every exception and calls handleError, after
    which LokiBatchHandler.flush() clears the buffer regardless — so a failing
    push silently discards its records. This subclass counts failures, prints a
    rate-limited banner naming how many records were lost, and closes the
    emitter session so the next attempt reconnects instead of reusing a socket
    that may itself be the problem.
    """

    #: Delivery counters, declared at class level so they read correctly even on
    #: an instance whose __init__ was bypassed (logging internals and tests both
    #: build handlers that way).
    consecutive_failures: int = 0
    push_success_count: int = 0
    records_dropped: int = 0

    def __init__(
        self,
        *args: Any,
        push_timeout: float = DEFAULT_PUSH_TIMEOUT,
        **kwargs: Any,
    ) -> None:
        """Build the handler, then bound its emitter's POSTs with push_timeout.

        push_timeout is keyword-only and *args is forwarded verbatim, so the
        inherited LokiHandler(url, tags, ...) positional signature keeps working
        rather than silently binding the URL to push_timeout.
        """
        if push_timeout <= 0:
            raise ValueError(
                f"push_timeout must be > 0, got {push_timeout!r}; an unbounded "
                "POST can wedge the logging pipeline permanently."
            )
        super().__init__(*args, **kwargs)
        self.consecutive_failures = 0
        self.push_success_count = 0
        self.records_dropped = 0
        # LokiHandler builds its own LokiEmitter from a long positional
        # signature that upstream keeps extending. Re-blessing the instance
        # applies the timeout without duplicating (and having to track) that
        # signature here.
        emitter = getattr(self, "emitter", None)
        if isinstance(emitter, LokiEmitter):
            emitter.__class__ = SafeLokiEmitter
            emitter.push_timeout = push_timeout
            # Re-blessing skips SafeLokiEmitter.__init__, so enrol it by hand
            # for the after-fork reset.
            _LIVE_EMITTERS.add(emitter)
        elif emitter is not None and sys.meta_path is not None:
            # Failing open here would silently restore unbounded POSTs — the exact
            # wedge this class exists to prevent, and one that produces no output
            # of its own. Say so loudly rather than pass.
            print(
                "LOKI HANDLER WARNING: could not apply push_timeout — logging_loki "
                f"supplied a {type(emitter).__name__}, not a LokiEmitter. POSTs to "
                "Loki are UNBOUNDED and a hung endpoint can stop log delivery for "
                "the life of this process.",
                file=sys.stderr,
            )

    def _note_push_success(self) -> None:
        """Record a delivered push and clear the failure streak."""
        self.push_success_count += 1
        self.consecutive_failures = 0

    def _should_report_failure(self) -> bool:
        """Report the 1st, 2nd, 4th, 8th ... consecutive failure.

        A wedged endpoint fails on every flush — once per batch_interval, and
        once per process in a multiprocess service. Printing a full banner each
        time buries real errors (16 tracebacks per Optuna pool startup was a
        reported complaint). Backing off on powers of two keeps the first
        failure immediate and the ongoing signal alive but quiet.
        """
        n = self.consecutive_failures
        return n > 0 and (n & (n - 1)) == 0

    def _note_push_failure(self, records_lost: int) -> None:
        """Count a failed push and drop the HTTP session so the next one reconnects.

        Accounting only — it prints nothing, so the two failure paths can each
        report in their own shape without doubling up on banners.
        """
        self.consecutive_failures += 1
        self.records_dropped += records_lost
        # Reconnect on the next attempt: the pooled socket may be the fault
        # (stale keep-alive, endpoint cut over). Upstream did this and our
        # earlier handleError override dropped it, which is why a wedged
        # handler never recovered without a container restart.
        try:
            self.emitter.close()
        except Exception:
            pass

    def _report_batch_failure(self, records_lost: int, reason: str = "") -> None:
        """Print a loud, rate-limited banner for a failed batch push to Loki."""
        if not self._should_report_failure():
            return
        if sys.meta_path is None:
            return

        print("=" * 60, file=sys.stderr)
        print("LOKI HANDLER ERROR: Failed to send log batch to Loki", file=sys.stderr)
        print("=" * 60, file=sys.stderr)
        if reason:
            print(reason, file=sys.stderr)
        print(f"Records dropped this batch: {records_lost}", file=sys.stderr)
        print(
            f"Consecutive failed pushes: {self.consecutive_failures} "
            f"(total records dropped: {self.records_dropped})",
            file=sys.stderr,
        )
        if self.push_success_count == 0:
            print(
                "This handler has NEVER successfully delivered a batch. Logs are "
                "not reaching Loki at all — check LOKI_ENDPOINT is the full push "
                "URL (.../loki/api/v1/push), and check credentials.",
                file=sys.stderr,
            )
        elif self.consecutive_failures >= 4:
            print(
                "Loki delivery has been failing repeatedly. Logs queried from "
                "Loki for this service are INCOMPLETE for this period.",
                file=sys.stderr,
            )
        try:
            ei = sys.exc_info()
            if ei and ei[0]:
                print(f"\nError Type: {ei[0].__name__}", file=sys.stderr)
                print(f"Error Message: {ei[1]}", file=sys.stderr)
                print("\nFull Traceback:", file=sys.stderr)
                traceback.print_exception(*ei, file=sys.stderr)
        except Exception as e:
            print(f"Could not print exception info: {e}", file=sys.stderr)
        print("=" * 60, file=sys.stderr)

    def emit(self, record: logging.LogRecord) -> None:
        """Send log record to Loki.

        Overrides LokiHandler.emit to pass the LogRecord (not the caught
        exception) to handleError, so error reporting can always access
        the original log message.
        """
        try:
            line = self.format(record)
        except Exception:
            # A formatter bug is not a Loki outage: report it, but do not count
            # it against delivery or tear down a healthy HTTP session.
            self.handleError(record)
            return

        try:
            self.emitter(record, line)
        except Exception:
            self._note_push_failure(1)
            # handleError prints this path's banner, so reporting here as well
            # would double every message. Gate it on the same backoff the batch
            # path uses: with batching off, an unreachable endpoint would
            # otherwise print a full banner and traceback per log line.
            if self._should_report_failure():
                self.handleError(record)
        else:
            self._note_push_success()

    def emit_batch(self, records: List[logging.LogRecord]) -> None:
        """Send a batch of records, reporting failures instead of swallowing them.

        Upstream's emit_batch routes the exception into handleError and returns
        normally, so LokiBatchHandler.flush() clears the buffer and the loss is
        invisible. Here the failure is counted and announced.

        Deliberately NOT wrapped in logging_loki's @with_original_stdout, unlike
        the method it overrides. That decorator pins output to the stderr that
        existed at logging_loki import time, so the banner bypasses any later
        redirect — including the one an operator or test harness installed to
        capture it. Our reporting never re-enters the logging system (it is a
        raw print), so the recursion guard it provides buys nothing here, and
        emit()/handleError already honour the live sys.stderr.
        """
        try:
            batch = [(record, self.format(record)) for record in records]
        except Exception:
            # A formatter bug is not a Loki outage (see emit). An empty batch
            # cannot raise here, so records[0] is always present.
            self.handleError(records[0])
            return

        # @with_lock returns without posting when the emitter is busy, so a clean
        # return does not prove delivery. Only SafeLokiEmitter counts attempts;
        # against any other emitter fall back to the return-value assumption.
        count_attempts = isinstance(self.emitter, SafeLokiEmitter)
        attempts_before = self.emitter.post_attempts if count_attempts else 0

        try:
            self.emitter.emit_batch(batch)
        except Exception:
            self._note_push_failure(len(records))
            self._report_batch_failure(len(records))
            return

        if count_attempts and self.emitter.post_attempts == attempts_before:
            self._note_push_failure(len(records))
            self._report_batch_failure(
                len(records),
                reason="Push was SKIPPED without being sent (emitter lock held by "
                       "another thread). The batch buffer is cleared regardless, so "
                       "these records are lost.",
            )
            return

        self._note_push_success()

    def handleError(self, record: Union[logging.LogRecord, BaseException]) -> None:
        """Handle errors during emit() by printing to stderr instead of logging.

        This prevents recursive loops where Loki handler errors would be logged
        through the same Loki handler, causing infinite recursion.

        Accepts an Exception as well as a LogRecord: upstream LokiHandler calls
        handleError(exc) from its own emit/emit_batch, so an inherited code path
        can hand us either. The getattr fallbacks below cover both.
        """
        if sys.meta_path is None:
            return

        print("=" * 60, file=sys.stderr)
        print("LOKI HANDLER ERROR: Failed to send log to Loki", file=sys.stderr)
        print("=" * 60, file=sys.stderr)

        try:
            # getMessage() exists on LogRecord but not on an Exception, and can
            # itself raise on a malformed record (bad %-args).
            get_message = getattr(record, 'getMessage', None)
            try:
                if callable(get_message):
                    message = get_message()
                else:
                    message = str(record)
            except Exception:
                message = getattr(record, 'msg', '<unavailable>')
            print(f"Original log: {message}", file=sys.stderr)
            print(f"Level: {getattr(record, 'levelname', '<unavailable>')}", file=sys.stderr)
            print(f"Logger: {getattr(record, 'name', '<unavailable>')}", file=sys.stderr)
        except Exception as e:
            print(f"Could not get log record details: {e}", file=sys.stderr)

        try:
            ei = sys.exc_info()
            if ei and ei[0]:
                print(f"\nError Type: {ei[0].__name__}", file=sys.stderr)
                print(f"Error Message: {ei[1]}", file=sys.stderr)
                print("\nFull Traceback:", file=sys.stderr)
                traceback.print_exception(*ei, file=sys.stderr)
        except Exception as e:
            print(f"Could not print exception info: {e}", file=sys.stderr)

        print("=" * 60, file=sys.stderr)


class SafeLokiBatchHandler(LokiBatchHandler):
    """LokiBatchHandler that flushes on a real timer and survives dictConfig.

    Fixes two silent-log-drop failures in stdlib's MemoryHandler (our
    grandparent), both of which strand records in the buffer with no
    exception, no log line, completely invisible:

    1. No wall-clock flush. MemoryHandler only flushes inside emit(): each
       record is appended and shouldFlush() is checked *against that record*.
       There is no background timer, so "1-second batching" really means
       "flush on the next emit >=1s after the last flush." In a long-running
       service that logs in occasional bursts (an idle alerting/ingest API),
       the trailing records of each burst sit in the buffer until the *next*
       emit — minutes later, or never if the service stays idle. This class
       runs a daemon thread that calls flush() every `interval` seconds, so
       the batch interval is an actual wall-clock interval. The atexit
       flush_logging() safety net does not help here: a gunicorn worker never
       exits under normal operation, so it never fires.

    2. close() nulling the target. MemoryHandler.close() sets
       `self.target = None`. In services that use logging.config.dictConfig
       (Flask+gunicorn create_app patterns, FastAPI+uvicorn, etc.), dictConfig
       calls `_clearExistingHandlers()` which close()s ALL handlers in the
       module-level `_handlerList`. After that, target is None and every
       flush() short-circuits to a no-op forever. The close() override below
       flushes pending records and marks the handler closed but leaves
       `self.target` intact so subsequent emits/flushes keep working. Note it
       deliberately does NOT stop the flush timer — dictConfig close()s the
       handler while the process keeps running, so the timer must survive too.
       See the diagnostic trail in mcp-gatekeeper v0.5.5 for how this
       manifests in production.
    """

    def __init__(self, interval: float, **kwargs) -> None:
        if interval is None or interval <= 0:
            # wait(0) would hot-spin the timer thread; wait(None) would block it
            # forever (reintroducing the stranded-records bug). Batching needs a
            # real positive interval — to turn batching OFF, pass
            # batch_interval=None to configure_logging(), which skips this class
            # entirely and ships each record through SafeLokiHandler.
            raise ValueError(
                f"SafeLokiBatchHandler interval must be > 0, got {interval!r}; "
                "pass batch_interval=None to configure_logging() to disable batching."
            )
        super().__init__(interval, **kwargs)
        self._timer_stop = threading.Event()
        self._flush_timer = threading.Thread(
            target=self._periodic_flush,
            name="loki-batch-flush",
            daemon=True,
        )
        self._flush_timer.start()

    def _periodic_flush(self) -> None:
        """Flush buffered records every `interval` seconds until stopped.

        Event.wait() returns True the moment stop_timer() sets the event, and
        False on each `interval` timeout — so this loops on the wall clock and
        exits promptly on shutdown. flush() is a no-op when the buffer is empty,
        so idle intervals are cheap.
        """
        while not self._timer_stop.wait(self.interval):
            try:
                self.flush()
            except Exception as e:
                # Push failures are reported by SafeLokiHandler.emit_batch, which
                # is where they actually surface. Anything reaching here is a
                # failure of the flush machinery itself (never seen in practice)
                # and would otherwise be invisible, so say so — but never let the
                # timer thread die, or batching stops for the process lifetime.
                if sys.meta_path is not None:
                    print(
                        f"LOKI HANDLER ERROR: batch flush timer raised "
                        f"{type(e).__name__}: {e}",
                        file=sys.stderr,
                    )

    def stop_timer(self) -> None:
        """Stop the background flush timer. Idempotent and safe after close()."""
        self._timer_stop.set()

    def close(self) -> None:
        try:
            if self.flushOnClose and self.target:
                self.flush()
        finally:
            self.acquire()
            try:
                # Skip MemoryHandler.close() — it would set self.target = None.
                # Go directly to BufferingHandler.close() which just marks the
                # handler closed via Handler.close().
                BufferingHandler.close(self)
            finally:
                self.release()


class SafeLokiQueueHandler(logging.handlers.QueueHandler):
    """Queue-based Loki handler that sends logs asynchronously via a background thread.

    Uses an in-memory queue so that emit() returns in microseconds instead
    of blocking on an HTTP POST to Loki. A QueueListener drains the queue
    in a background thread and forwards records through SafeLokiHandler
    (optionally wrapped in SafeLokiBatchHandler for batched POSTs).

    Tracks the number of enqueued messages for diagnostics via get_diagnostics().
    """

    handler: Union[SafeLokiBatchHandler, SafeLokiHandler]

    def __init__(
        self,
        queue: Queue,
        batch_interval: Optional[float] = None,
        push_timeout: float = DEFAULT_PUSH_TIMEOUT,
        **kwargs,
    ) -> None:
        super().__init__(queue)
        self.enqueued_count: int = 0
        loki_handler = SafeLokiHandler(push_timeout=push_timeout, **kwargs)
        if batch_interval:
            self.handler = SafeLokiBatchHandler(batch_interval, target=loki_handler)
        else:
            self.handler = loki_handler
        self.listener = logging.handlers.QueueListener(self.queue, self.handler)
        self.listener.start()

    def emit(self, record: logging.LogRecord) -> None:
        """Enqueue a log record and increment the diagnostics counter."""
        self.enqueued_count += 1
        super().emit(record)

    def get_diagnostics(self) -> dict:
        """Return diagnostic info about this handler's state."""
        target = self.handler
        if isinstance(target, SafeLokiBatchHandler):
            target = target.target
        return {
            "enqueued_count": self.enqueued_count,
            "queue_size": self.queue.qsize(),
            "push_success_count": getattr(target, "push_success_count", 0),
            "consecutive_failures": getattr(target, "consecutive_failures", 0),
            "records_dropped": getattr(target, "records_dropped", 0),
        }

    def flush(self) -> None:
        super().flush()
        self.handler.flush()

    def __del__(self) -> None:
        try:
            self.listener.stop()
        except Exception:
            pass
        inner = getattr(self, "handler", None)
        if isinstance(inner, SafeLokiBatchHandler):
            inner.stop_timer()


def _collect_loki_queue_handlers() -> List[SafeLokiQueueHandler]:
    """Return every SafeLokiQueueHandler currently attached to the root logger."""
    return [
        h for h in logging.getLogger().handlers
        if isinstance(h, SafeLokiQueueHandler)
    ]


def flush_logging(timeout: float = 5.0) -> bool:
    """Drain the async queue and flush every Loki handler on the root logger.

    Short-lived processes (CLI tools, cron jobs) can exit before the
    background QueueListener thread POSTs the last enqueued records,
    silently dropping the final log lines — exactly the success/failure
    summaries operators need to see. Call this at the end of such programs
    to force a clean drain.

    Args:
        timeout: Maximum seconds to wait for the drain to complete. Bounded
            so a dead Loki endpoint can't hang process exit.

    Returns:
        True if all handlers were fully flushed within `timeout`, or if
        there was nothing to flush (no Loki handler installed, e.g.
        debug_local=True or stdout fallback). False if the timeout elapsed
        before the drain finished.

    Safe to call multiple times — once the listener has been stopped,
    subsequent calls return quickly. configure_logging() also registers
    this as an atexit handler when a Loki handler is installed, so most
    scripts get correct behavior with no code change.
    """
    handlers = _collect_loki_queue_handlers()
    if not handlers:
        return True

    completed = threading.Event()

    def _drain() -> None:
        for h in handlers:
            try:
                h.listener.stop()
            except Exception:
                pass
            inner = getattr(h, "handler", None)
            if isinstance(inner, SafeLokiBatchHandler):
                try:
                    inner.stop_timer()
                except Exception:
                    pass
            try:
                h.flush()
            except Exception:
                pass
        completed.set()

    worker = threading.Thread(target=_drain, daemon=True)
    worker.start()
    return completed.wait(timeout)


def _register_atexit_flush() -> None:
    """Register flush_logging as an atexit handler exactly once per process.

    Guarded by a module-level flag so repeat configure_logging() calls
    (e.g. in tests, or apps that reconfigure on signal) don't stack
    duplicate atexit entries.
    """
    global _atexit_registered
    if _atexit_registered:
        return
    atexit.register(flush_logging)
    _atexit_registered = True


def _test_loki_connection(
    endpoint: str, user: str, password: str, ca_bundle: str
) -> Tuple[bool, str]:
    """Test connection to Loki endpoint before setting up the handler.

    Args:
        endpoint: Loki API endpoint URL
        user: Authentication username
        password: Authentication password
        ca_bundle: Path to CA bundle for SSL verification

    Returns:
        Tuple of (success, error_message)
    """
    try:
        base_url = endpoint.replace("/loki/api/v1/push", "")

        response = requests.get(
            f"{base_url}/ready",
            timeout=3,
            auth=(user, password),
            verify=ca_bundle if ca_bundle and ca_bundle != "false" else False
        )

        if not response.ok:
            return False, f"Loki /ready returned HTTP {response.status_code}"

        return True, ""

    except requests.exceptions.SSLError as e:
        return False, f"SSL certificate verification failed: {str(e)}"
    except requests.exceptions.ConnectionError as e:
        return False, f"Connection failed: {str(e)}"
    except requests.exceptions.Timeout as e:
        return False, f"Connection timeout: {str(e)}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"


def _emit_connection_failure_warning(
    endpoint: str, error_msg: str, application_tag: str
) -> None:
    """Print a banner-style warning to stderr when the Loki connection test fails.

    Production operators must notice that logs are not reaching Loki, so the
    output is deliberately loud and includes the endpoint, error, and a hint
    about the most common misconfiguration (forgetting the /loki/api/v1/push
    path suffix). stderr is flushed so the warning isn't lost behind buffering
    when the process is short-lived or stderr is captured by a container runtime.
    """
    print("=" * 70, file=sys.stderr)
    print("WARNING: byteforge-loki-logging connection test FAILED", file=sys.stderr)
    print("=" * 70, file=sys.stderr)
    print(f"  Endpoint:    {endpoint}", file=sys.stderr)
    print(f"  Error:       {error_msg}", file=sys.stderr)
    print(f"  Application: {application_tag}", file=sys.stderr)
    print("", file=sys.stderr)
    print(
        "  Falling back to stdout logging. Logs WILL NOT reach Loki.",
        file=sys.stderr,
    )
    print("", file=sys.stderr)
    print(
        "  Hint: LOKI_ENDPOINT must be the full push URL, e.g.",
        file=sys.stderr,
    )
    print(
        "        https://loki.example.com/loki/api/v1/push",
        file=sys.stderr,
    )
    print(
        "        (not just the base URL https://loki.example.com).",
        file=sys.stderr,
    )
    print("=" * 70, file=sys.stderr)
    sys.stderr.flush()


def _configure_loki_internal_logger() -> None:
    """Configure the loki library's internal error logger to write to stderr.

    The python-logging-loki library routes all emitter errors to a logger
    named 'waylay.loglog'. By default this logger has no handlers, so errors
    vanish silently. This function attaches a stderr handler so those errors
    become visible.
    """
    loki_internal_logger = logging.getLogger("waylay.loglog")
    loki_internal_logger.setLevel(logging.WARNING)
    loki_internal_logger.propagate = False

    if not loki_internal_logger.handlers:
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setFormatter(
            logging.Formatter("[LOKI-INTERNAL] %(levelname)s - %(message)s")
        )
        loki_internal_logger.addHandler(stderr_handler)


def _configure_stdout_logging(level: Union[int, str]) -> None:
    """Set up the root logger to write to stdout with UTC timestamps."""
    root_logger = logging.getLogger()
    root_logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    formatter.converter = time.gmtime
    handler.setFormatter(formatter)

    root_logger.setLevel(_resolve_log_level(level))
    root_logger.addHandler(handler)


def _create_loki_handler(
    application_tag: str,
    endpoint: str,
    user: str,
    password: str,
    ca_bundle_path: str,
    json_format: bool,
    batch_interval: Optional[float],
    push_timeout: float,
) -> SafeLokiQueueHandler:
    """Create an async queue-based Loki handler with the appropriate formatter."""
    handler = SafeLokiQueueHandler(
        Queue(-1),
        batch_interval=batch_interval,
        push_timeout=push_timeout,
        url=endpoint,
        tags={"application": application_tag},
        auth=(user, password),
        verify=ca_bundle_path
    )

    if json_format:
        formatter = LokiJsonFormatter()
    else:
        formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')

    inner_handler = handler.handler
    if isinstance(inner_handler, LokiBatchHandler):  # SafeLokiBatchHandler is a subclass
        inner_handler.target.setFormatter(formatter)
    else:
        inner_handler.setFormatter(formatter)

    return handler


def _validate_loki_env_vars() -> Tuple[str, str, str, str]:
    """Read and validate required Loki environment variables.

    Returns:
        Tuple of (endpoint, user, password, ca_bundle_path)

    Raises:
        RuntimeError: If any required variables are missing
    """
    env_map = {
        "LOKI_ENDPOINT": os.environ.get("LOKI_ENDPOINT"),
        "LOKI_USER": os.environ.get("LOKI_USER"),
        "LOKI_PASSWORD": os.environ.get("LOKI_PASSWORD"),
        "LOKI_CA_BUNDLE_PATH": os.environ.get("LOKI_CA_BUNDLE_PATH"),
    }

    missing_vars = [name for name, value in env_map.items() if not value]
    if missing_vars:
        raise RuntimeError(
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )

    return env_map["LOKI_ENDPOINT"], env_map["LOKI_USER"], env_map["LOKI_PASSWORD"], env_map["LOKI_CA_BUNDLE_PATH"]


def configure_logging(
    application_tag: str,
    debug_local: bool = False,
    local_level: Union[int, str] = logging.INFO,
    json_format: bool = True,
    batch_interval: Optional[float] = 1.0,
    push_timeout: float = DEFAULT_PUSH_TIMEOUT
) -> Optional[logging.Handler]:
    """Configure logging for the application with Loki integration or local stdout.

    Tests the Loki connection before setting up the handler. If the connection
    test fails, prints a loud banner-style warning to stderr and falls back to
    stdout logging. The application never crashes due to logging issues, but
    operators are explicitly told that logs are not reaching Loki.

    Environment variables (required when debug_local=False):
        LOKI_ENDPOINT: Loki push API URL — must be the full push path,
            e.g. https://loki.example.com/loki/api/v1/push (not just the
            base URL). The base URL alone may pass the /ready probe but
            log POSTs will silently 404.
        LOKI_USER: HTTP Basic Auth username
        LOKI_PASSWORD: HTTP Basic Auth password
        LOKI_CA_BUNDLE_PATH: Path to CA .pem file, or "false" to disable SSL verification

    Args:
        application_tag: Unique identifier for the application in logs
        debug_local: If True, log to stdout instead of Loki (default: False)
        local_level: Logging level for both Loki and local modes (default: logging.INFO)
        json_format: If True, format Loki logs as JSON for structured queries (default: True)
        batch_interval: Seconds between batched POSTs to Loki (default: 1.0). A
            background timer flushes the buffer every `batch_interval` seconds,
            so trailing records ship on the wall clock even when the process
            goes idle. Pass None or 0 to disable batching entirely and ship each
            record immediately — useful for low-volume alerting/ingest services
            where latency matters more than POST count.
        push_timeout: Seconds any single POST to Loki may take before it is
            abandoned (default: 10.0). Must be > 0. logging_loki itself passes
            no timeout, which lets a black-holed endpoint hang the flush thread
            forever and silently wedge the whole pipeline; this bounds it.

    Returns:
        SafeLokiQueueHandler if Loki connection succeeds, None if using stdout fallback

    Raises:
        ValueError: If application_tag is empty, or push_timeout is not > 0
        RuntimeError: If required environment variables are missing (non-debug mode only)
    """
    if not application_tag:
        raise ValueError("application_tag must be set")

    # Validated here rather than only in SafeLokiHandler: the handler is built
    # after the connection test, so a bad value would raise in production (Loki
    # up) but silently take the stdout-fallback path in dev/CI (Loki down) and
    # ship undetected.
    if push_timeout <= 0:
        raise ValueError(
            f"push_timeout must be > 0, got {push_timeout!r}; an unbounded "
            "POST can wedge the logging pipeline permanently."
        )

    # Reconfigure (SIGHUP, per-worker init, tests) may replace an existing Loki
    # handler. Clearing root.handlers alone would orphan the previous handler's
    # QueueListener thread and batch-flush timer — they'd linger until GC. Drain
    # and stop them first; flush_logging bounds this with its own timeout so a
    # dead old endpoint can't hang startup, and it's a no-op on first call.
    flush_logging()

    if debug_local:
        _configure_stdout_logging(local_level)
        return None

    endpoint, user, password, ca_bundle_path = _validate_loki_env_vars()

    connection_ok, error_msg = _test_loki_connection(endpoint, user, password, ca_bundle_path)

    if not connection_ok:
        _emit_connection_failure_warning(endpoint, error_msg, application_tag)
        _configure_stdout_logging(local_level)
        return None

    handler = _create_loki_handler(
        application_tag, endpoint, user, password, ca_bundle_path, json_format,
        batch_interval, push_timeout
    )

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(_resolve_log_level(local_level))
    root_logger.addHandler(handler)

    _configure_loki_internal_logger()
    _register_atexit_flush()

    return handler
