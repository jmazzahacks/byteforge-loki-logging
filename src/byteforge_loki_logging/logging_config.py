# logging_config.py
import json
import logging
import logging.handlers
import os
import sys
import time
import traceback
from queue import Queue
from typing import Optional, Set, Tuple, Union

import logging_loki
from logging.handlers import BufferingHandler
from logging_loki.handlers import LokiBatchHandler


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


class SafeLokiHandler(logging_loki.LokiHandler):
    """LokiHandler wrapper that prevents recursive logging loops.

    Overrides handleError to print errors directly to stderr instead of using
    the logging system, which would cause infinite recursion.
    """

    def emit(self, record: logging.LogRecord) -> None:
        """Send log record to Loki.

        Overrides LokiHandler.emit to pass the LogRecord (not the caught
        exception) to handleError, so error reporting can always access
        the original log message.
        """
        try:
            self.emitter(record, self.format(record))
        except Exception:
            self.handleError(record)

    def handleError(self, record: logging.LogRecord) -> None:
        """Handle errors during emit() by printing to stderr instead of logging.

        This prevents recursive loops where Loki handler errors would be logged
        through the same Loki handler, causing infinite recursion.
        """
        if sys.meta_path is None:
            return

        print("=" * 60, file=sys.stderr)
        print("LOKI HANDLER ERROR: Failed to send log to Loki", file=sys.stderr)
        print("=" * 60, file=sys.stderr)

        try:
            try:
                message = record.getMessage()
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
    """LokiBatchHandler that survives `logging.config.dictConfig` calls.

    Stdlib's MemoryHandler.close() — which we inherit through LokiBatchHandler
    — sets `self.target = None` as part of cleanup. That's fine for one-shot
    programs, but in long-running services that use logging.config.dictConfig
    (Flask+gunicorn create_app patterns, FastAPI+uvicorn, etc.), dictConfig
    calls `_clearExistingHandlers()` which iterates ALL handlers in the
    module-level `_handlerList` and calls close() on each one. After that,
    our LokiBatchHandler.target is None, and every subsequent flush() silently
    does nothing (`if self.target and self.buffer: ...` short-circuits to no-op).
    Records pile up in self.buffer forever and never reach Loki — no exception,
    no log line, completely invisible.

    This override flushes any pending records and marks the handler closed,
    but leaves `self.target` intact so subsequent emits/flushes keep working.
    See the diagnostic trail in mcp-gatekeeper v0.5.5 for how this manifests
    in production.
    """

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

    def __init__(self, queue: Queue, batch_interval: Optional[float] = None, **kwargs) -> None:
        super().__init__(queue)
        self.enqueued_count: int = 0
        loki_handler = SafeLokiHandler(**kwargs)
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
        return {
            "enqueued_count": self.enqueued_count,
            "queue_size": self.queue.qsize(),
        }

    def flush(self) -> None:
        super().flush()
        self.handler.flush()

    def __del__(self) -> None:
        self.listener.stop()


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
        import requests

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
) -> SafeLokiQueueHandler:
    """Create an async queue-based Loki handler with the appropriate formatter."""
    handler = SafeLokiQueueHandler(
        Queue(-1),
        batch_interval=1.0,
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
    json_format: bool = True
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

    Returns:
        SafeLokiQueueHandler if Loki connection succeeds, None if using stdout fallback

    Raises:
        ValueError: If application_tag is empty
        RuntimeError: If required environment variables are missing (non-debug mode only)
    """
    if not application_tag:
        raise ValueError("application_tag must be set")

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
        application_tag, endpoint, user, password, ca_bundle_path, json_format
    )

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(_resolve_log_level(local_level))
    root_logger.addHandler(handler)

    _configure_loki_internal_logger()

    return handler
