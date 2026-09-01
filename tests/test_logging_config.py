import json
import logging
import logging.handlers
import sys
import threading
import time
from io import StringIO
from queue import Queue
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from logging_loki.handlers import LokiBatchHandler

import byteforge_loki_logging.logging_config as cfg_module
from byteforge_loki_logging.logging_config import (
    LokiJsonFormatter,
    SafeLokiBatchHandler,
    SafeLokiEmitter,
    SafeLokiHandler,
    SafeLokiQueueHandler,
    _resolve_log_level,
    _test_loki_connection,
    _configure_loki_internal_logger,
    configure_logging,
    flush_logging,
    _STANDARD_RECORD_ATTRS,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_record(**kwargs: Any) -> logging.LogRecord:
    """Create a LogRecord with sensible defaults, overridable via kwargs."""
    defaults = {
        "name": "test.logger",
        "level": logging.INFO,
        "pathname": "test.py",
        "lineno": 1,
        "msg": "hello %s",
        "args": ("world",),
        "exc_info": None,
    }
    defaults.update(kwargs)
    return logging.LogRecord(**defaults)


def _env_vars() -> dict[str, str]:
    """Return a complete set of Loki environment variables."""
    return {
        "LOKI_ENDPOINT": "https://loki.example.com/loki/api/v1/push",
        "LOKI_USER": "user",
        "LOKI_PASSWORD": "pass",
        "LOKI_CA_BUNDLE_PATH": "/path/to/ca.pem",
    }


# ===========================================================================
# SafeLokiHandler.emit
# ===========================================================================

class TestSafeLokiHandlerEmit:

    @patch("byteforge_loki_logging.logging_config.logging_loki.LokiHandler.__init__", return_value=None)
    def _make_handler(self, mock_init: MagicMock) -> SafeLokiHandler:
        handler = SafeLokiHandler.__new__(SafeLokiHandler)
        return handler

    def test_emit_calls_emitter_with_record(self) -> None:
        handler = self._make_handler()
        handler.emitter = MagicMock()
        handler.format = MagicMock(return_value="formatted")
        record = _make_record()

        handler.emit(record)

        handler.emitter.assert_called_once_with(record, "formatted")

    def test_emit_passes_record_to_handle_error_on_failure(self) -> None:
        handler = self._make_handler()
        handler.emitter = MagicMock(side_effect=ConnectionError("network down"))
        handler.format = MagicMock(return_value="formatted")
        handler.handleError = MagicMock()
        record = _make_record()

        handler.emit(record)

        handler.handleError.assert_called_once_with(record)

    def test_emit_does_not_pass_exception_to_handle_error(self) -> None:
        """Verify we fixed the upstream bug where exc was passed instead of record."""
        handler = self._make_handler()
        handler.emitter = MagicMock(side_effect=ConnectionError("network down"))
        handler.format = MagicMock(return_value="formatted")

        captured_args = []
        def capture_handle_error(arg: Any) -> None:
            captured_args.append(arg)

        handler.handleError = capture_handle_error
        record = _make_record()

        handler.emit(record)

        assert len(captured_args) == 1
        assert isinstance(captured_args[0], logging.LogRecord)
        assert not isinstance(captured_args[0], Exception)


# ===========================================================================
# SafeLokiHandler.handleError
# ===========================================================================

class TestSafeLokiHandleError:

    @patch("byteforge_loki_logging.logging_config.logging_loki.LokiHandler.__init__", return_value=None)
    def _make_handler(self, mock_init: MagicMock) -> SafeLokiHandler:
        handler = SafeLokiHandler.__new__(SafeLokiHandler)
        return handler

    def test_normal_record_prints_details(self) -> None:
        handler = self._make_handler()
        record = _make_record()
        captured = StringIO()

        with patch("sys.stderr", captured):
            handler.handleError(record)

        output = captured.getvalue()
        assert "LOKI HANDLER ERROR" in output
        assert "hello world" in output
        assert "INFO" in output
        assert "test.logger" in output

    def test_malformed_record_falls_back_to_msg(self) -> None:
        handler = self._make_handler()
        record = _make_record()
        record.msg = "bad format %d"
        record.args = ("not_a_number",)
        captured = StringIO()

        with patch("sys.stderr", captured):
            handler.handleError(record)

        output = captured.getvalue()
        assert "LOKI HANDLER ERROR" in output
        assert "bad format %d" in output

    def test_record_missing_attributes_uses_defaults(self) -> None:
        handler = self._make_handler()
        record = MagicMock(spec=[])
        record.getMessage = MagicMock(side_effect=Exception("boom"))
        captured = StringIO()

        with patch("sys.stderr", captured):
            handler.handleError(record)

        output = captured.getvalue()
        assert "LOKI HANDLER ERROR" in output
        assert "<unavailable>" in output

    def test_python_shutdown_silently_returns(self) -> None:
        handler = self._make_handler()
        record = _make_record()
        captured = StringIO()

        with patch("sys.stderr", captured), patch.object(sys, "meta_path", None):
            handler.handleError(record)

        assert captured.getvalue() == ""

    def test_exception_info_printed_when_available(self) -> None:
        handler = self._make_handler()
        record = _make_record()
        captured = StringIO()

        try:
            raise ValueError("test error")
        except ValueError:
            with patch("sys.stderr", captured):
                handler.handleError(record)

        output = captured.getvalue()
        assert "ValueError" in output
        assert "test error" in output
        assert "Full Traceback" in output


# ===========================================================================
# SafeLokiQueueHandler
# ===========================================================================

class TestSafeLokiQueueHandler:

    @patch("byteforge_loki_logging.logging_config.logging_loki.LokiHandler.__init__", return_value=None)
    def test_construction_starts_listener(self, mock_init: MagicMock) -> None:
        queue = Queue(-1)
        handler = SafeLokiQueueHandler(queue, url="http://loki", tags={}, auth=("u", "p"))
        try:
            assert isinstance(handler, logging.handlers.QueueHandler)
            assert handler.listener is not None
            assert handler.listener._thread is not None
            assert handler.listener._thread.is_alive()
        finally:
            handler.listener.stop()

    @patch("byteforge_loki_logging.logging_config.logging_loki.LokiHandler.__init__", return_value=None)
    def test_inner_handler_is_safe_loki_handler_without_batch(self, mock_init: MagicMock) -> None:
        queue = Queue(-1)
        handler = SafeLokiQueueHandler(queue, url="http://loki", tags={}, auth=("u", "p"))
        try:
            assert isinstance(handler.handler, SafeLokiHandler)
        finally:
            handler.listener.stop()

    @patch("byteforge_loki_logging.logging_config.logging_loki.LokiHandler.__init__", return_value=None)
    def test_inner_handler_is_batch_handler_with_interval(self, mock_init: MagicMock) -> None:
        from logging_loki.handlers import LokiBatchHandler
        queue = Queue(-1)
        handler = SafeLokiQueueHandler(queue, batch_interval=1.0, url="http://loki", tags={}, auth=("u", "p"))
        try:
            assert isinstance(handler.handler, LokiBatchHandler)
            assert isinstance(handler.handler.target, SafeLokiHandler)
        finally:
            handler.listener.stop()

    @patch("byteforge_loki_logging.logging_config.logging_loki.LokiHandler.__init__", return_value=None)
    def test_emit_enqueues_without_blocking(self, mock_init: MagicMock) -> None:
        queue = Queue(-1)
        handler = SafeLokiQueueHandler(queue, url="http://loki", tags={}, auth=("u", "p"))
        try:
            record = _make_record()
            handler.emit(record)
        finally:
            handler.listener.stop()

    @patch("byteforge_loki_logging.logging_config.logging_loki.LokiHandler.__init__", return_value=None)
    def test_emit_increments_enqueued_count(self, mock_init: MagicMock) -> None:
        queue = Queue(-1)
        handler = SafeLokiQueueHandler(queue, url="http://loki", tags={}, auth=("u", "p"))
        try:
            assert handler.enqueued_count == 0
            handler.emit(_make_record())
            handler.emit(_make_record())
            handler.emit(_make_record())
            assert handler.enqueued_count == 3
        finally:
            handler.listener.stop()

    @patch("byteforge_loki_logging.logging_config.logging_loki.LokiHandler.__init__", return_value=None)
    def test_get_diagnostics_returns_counts(self, mock_init: MagicMock) -> None:
        queue = Queue(-1)
        handler = SafeLokiQueueHandler(queue, url="http://loki", tags={}, auth=("u", "p"))
        try:
            handler.emit(_make_record())
            diag = handler.get_diagnostics()
            assert "enqueued_count" in diag
            assert "queue_size" in diag
            assert diag["enqueued_count"] == 1
        finally:
            handler.listener.stop()


# ===========================================================================
# _configure_loki_internal_logger
# ===========================================================================

class TestConfigureLokiInternalLogger:

    def setup_method(self) -> None:
        logger = logging.getLogger("waylay.loglog")
        logger.handlers.clear()
        logger.propagate = True
        logger.setLevel(logging.WARNING)

    def test_attaches_stderr_handler(self) -> None:
        _configure_loki_internal_logger()
        logger = logging.getLogger("waylay.loglog")
        assert len(logger.handlers) == 1
        assert isinstance(logger.handlers[0], logging.StreamHandler)
        assert logger.handlers[0].stream is sys.stderr

    def test_does_not_propagate(self) -> None:
        _configure_loki_internal_logger()
        logger = logging.getLogger("waylay.loglog")
        assert logger.propagate is False

    def test_no_duplicate_handlers_on_repeated_calls(self) -> None:
        _configure_loki_internal_logger()
        _configure_loki_internal_logger()
        logger = logging.getLogger("waylay.loglog")
        assert len(logger.handlers) == 1

    def test_formatter_includes_loki_internal_prefix(self) -> None:
        _configure_loki_internal_logger()
        logger = logging.getLogger("waylay.loglog")
        handler = logger.handlers[0]
        record = logging.LogRecord(
            name="waylay.loglog", level=logging.WARNING, pathname="test.py",
            lineno=1, msg="test error", args=None, exc_info=None,
        )
        output = handler.formatter.format(record)
        assert "[LOKI-INTERNAL]" in output


# ===========================================================================
# LokiJsonFormatter
# ===========================================================================

class TestLokiJsonFormatter:

    def test_basic_format_produces_valid_json(self) -> None:
        formatter = LokiJsonFormatter()
        record = _make_record()
        result = json.loads(formatter.format(record))

        assert result["logger"] == "test.logger"
        assert result["level"] == "INFO"
        assert result["message"] == "hello world"

    def test_extra_fields_included(self) -> None:
        formatter = LokiJsonFormatter()
        record = _make_record()
        record.client_id = "web3-payroll"
        record.request_id = "abc-123"
        result = json.loads(formatter.format(record))

        assert result["client_id"] == "web3-payroll"
        assert result["request_id"] == "abc-123"

    def test_non_serializable_extras_converted_to_string(self) -> None:
        formatter = LokiJsonFormatter()
        record = _make_record()
        record.custom_obj = object()
        result = json.loads(formatter.format(record))

        assert isinstance(result["custom_obj"], str)

    def test_standard_attrs_excluded_from_extras(self) -> None:
        formatter = LokiJsonFormatter()
        record = _make_record()
        result = json.loads(formatter.format(record))

        for attr in ("funcName", "pathname", "lineno", "process", "thread"):
            assert attr not in result


# ===========================================================================
# configure_logging
# ===========================================================================

class TestConfigureLogging:

    def setup_method(self) -> None:
        root = logging.getLogger()
        root.handlers.clear()
        root.setLevel(logging.WARNING)

    def test_empty_application_tag_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="application_tag must be set"):
            configure_logging("")

    def test_missing_env_vars_raises_runtime_error(self) -> None:
        env = _env_vars()
        del env["LOKI_USER"]
        del env["LOKI_CA_BUNDLE_PATH"]

        with patch.dict("os.environ", env, clear=True):
            with pytest.raises(RuntimeError, match="LOKI_USER") as exc_info:
                configure_logging("myapp")
            assert "LOKI_CA_BUNDLE_PATH" in str(exc_info.value)

    @patch("byteforge_loki_logging.logging_config._test_loki_connection", return_value=(False, "connection refused"))
    def test_failed_connection_falls_back_to_stdout(self, mock_conn: MagicMock) -> None:
        with patch.dict("os.environ", _env_vars(), clear=True):
            result = configure_logging("myapp")

        assert result is None
        root = logging.getLogger()
        assert len(root.handlers) == 1
        assert isinstance(root.handlers[0], logging.StreamHandler)

    @patch("byteforge_loki_logging.logging_config._test_loki_connection",
           return_value=(False, "Loki /ready returned HTTP 404"))
    def test_failed_connection_emits_loud_banner_warning(self, mock_conn: MagicMock) -> None:
        captured = StringIO()
        with patch.dict("os.environ", _env_vars(), clear=True), patch("sys.stderr", captured):
            configure_logging("probe")

        output = captured.getvalue()
        # Banner separator must be present so the warning stands out in container logs.
        assert "=" * 70 in output
        assert "connection test FAILED" in output
        # Operator needs to see the endpoint that was tried, the error, and which app is affected.
        assert _env_vars()["LOKI_ENDPOINT"] in output
        assert "HTTP 404" in output
        assert "probe" in output
        # Stdout-fallback warning must be explicit, not buried.
        assert "WILL NOT reach Loki" in output
        # Hint about the most common misconfig (forgetting the push path).
        assert "/loki/api/v1/push" in output

    @patch("byteforge_loki_logging.logging_config._test_loki_connection", return_value=(True, ""))
    @patch("byteforge_loki_logging.logging_config.logging_loki.LokiHandler.__init__", return_value=None)
    def test_successful_connection_creates_queue_handler(
        self, mock_init: MagicMock, mock_conn: MagicMock
    ) -> None:
        with patch.dict("os.environ", _env_vars(), clear=True):
            result = configure_logging("myapp")

        try:
            assert isinstance(result, SafeLokiQueueHandler)
            root = logging.getLogger()
            assert result in root.handlers
        finally:
            if result and hasattr(result, 'listener'):
                result.listener.stop()

    @patch("byteforge_loki_logging.logging_config._test_loki_connection", return_value=(True, ""))
    @patch("byteforge_loki_logging.logging_config.logging_loki.LokiHandler.__init__", return_value=None)
    def test_json_format_true_uses_json_formatter(
        self, mock_init: MagicMock, mock_conn: MagicMock
    ) -> None:
        with patch.dict("os.environ", _env_vars(), clear=True):
            result = configure_logging("myapp", json_format=True)

        try:
            from logging_loki.handlers import LokiBatchHandler
            inner = result.handler
            if isinstance(inner, LokiBatchHandler):
                assert isinstance(inner.target.formatter, LokiJsonFormatter)
            else:
                assert isinstance(inner.formatter, LokiJsonFormatter)
        finally:
            if result and hasattr(result, 'listener'):
                result.listener.stop()

    @patch("byteforge_loki_logging.logging_config._test_loki_connection", return_value=(True, ""))
    @patch("byteforge_loki_logging.logging_config.logging_loki.LokiHandler.__init__", return_value=None)
    def test_json_format_false_uses_plain_formatter(
        self, mock_init: MagicMock, mock_conn: MagicMock
    ) -> None:
        with patch.dict("os.environ", _env_vars(), clear=True):
            result = configure_logging("myapp", json_format=False)

        try:
            from logging_loki.handlers import LokiBatchHandler
            inner = result.handler
            if isinstance(inner, LokiBatchHandler):
                formatter = inner.target.formatter
            else:
                formatter = inner.formatter
            assert isinstance(formatter, logging.Formatter)
            assert not isinstance(formatter, LokiJsonFormatter)
        finally:
            if result and hasattr(result, 'listener'):
                result.listener.stop()

    def test_debug_local_configures_stdout(self) -> None:
        result = configure_logging("myapp", debug_local=True)

        assert result is None
        root = logging.getLogger()
        assert len(root.handlers) >= 1
        handler = root.handlers[0]
        assert isinstance(handler, logging.StreamHandler)
        assert handler.stream is sys.stdout

    @patch("byteforge_loki_logging.logging_config._test_loki_connection", return_value=(True, ""))
    @patch("byteforge_loki_logging.logging_config.logging_loki.LokiHandler.__init__", return_value=None)
    def test_default_batch_interval_uses_batch_handler(
        self, mock_init: MagicMock, mock_conn: MagicMock
    ) -> None:
        with patch.dict("os.environ", _env_vars(), clear=True):
            result = configure_logging("myapp")
        try:
            assert isinstance(result.handler, SafeLokiBatchHandler)
        finally:
            if result and hasattr(result, "listener"):
                result.listener.stop()
                result.handler.stop_timer()

    @patch("byteforge_loki_logging.logging_config._test_loki_connection", return_value=(True, ""))
    @patch("byteforge_loki_logging.logging_config.logging_loki.LokiHandler.__init__", return_value=None)
    def test_batch_interval_none_ships_immediately_without_batching(
        self, mock_init: MagicMock, mock_conn: MagicMock
    ) -> None:
        with patch.dict("os.environ", _env_vars(), clear=True):
            result = configure_logging("myapp", batch_interval=None)
        try:
            # No batch layer at all — records go straight to SafeLokiHandler.
            assert isinstance(result.handler, SafeLokiHandler)
            assert not isinstance(result.handler, LokiBatchHandler)
        finally:
            if result and hasattr(result, "listener"):
                result.listener.stop()

    @patch("byteforge_loki_logging.logging_config._test_loki_connection", return_value=(True, ""))
    @patch("byteforge_loki_logging.logging_config.logging_loki.LokiHandler.__init__", return_value=None)
    def test_reconfigure_stops_previous_handler_threads(
        self, mock_init: MagicMock, mock_conn: MagicMock
    ) -> None:
        # A second configure_logging() must not orphan the first handler's
        # QueueListener + batch-flush timer threads — they must be stopped, not
        # left running until GC.
        with patch.dict("os.environ", _env_vars(), clear=True):
            first = configure_logging("app-v1")
            first_listener_thread = first.listener._thread
            first_timer = first.handler._flush_timer
            assert first_listener_thread.is_alive()
            assert first_timer.is_alive()

            second = configure_logging("app-v2")
        try:
            first_listener_thread.join(timeout=1.0)
            first_timer.join(timeout=1.0)
            assert not first_listener_thread.is_alive()
            assert not first_timer.is_alive()
            assert second in logging.getLogger().handlers
        finally:
            if second and hasattr(second, "listener"):
                try:
                    second.listener.stop()
                except Exception:
                    pass
                second.handler.stop_timer()

    @patch("byteforge_loki_logging.logging_config._test_loki_connection", return_value=(True, ""))
    @patch("byteforge_loki_logging.logging_config.logging_loki.LokiHandler.__init__", return_value=None)
    def test_string_log_level_resolved(
        self, mock_init: MagicMock, mock_conn: MagicMock
    ) -> None:
        with patch.dict("os.environ", _env_vars(), clear=True):
            result = configure_logging("myapp", local_level="WARNING")

        try:
            root = logging.getLogger()
            assert root.level == logging.WARNING
        finally:
            if result and hasattr(result, 'listener'):
                result.listener.stop()


# ===========================================================================
# _resolve_log_level
# ===========================================================================

class TestResolveLogLevel:

    def test_int_passthrough(self) -> None:
        assert _resolve_log_level(logging.DEBUG) == logging.DEBUG
        assert _resolve_log_level(logging.WARNING) == logging.WARNING

    def test_string_resolution(self) -> None:
        assert _resolve_log_level("INFO") == logging.INFO
        assert _resolve_log_level("warning") == logging.WARNING
        assert _resolve_log_level("DEBUG") == logging.DEBUG

    def test_invalid_string_raises(self) -> None:
        with pytest.raises(AttributeError):
            _resolve_log_level("NOTALEVEL")


# ===========================================================================
# _test_loki_connection
# ===========================================================================

class TestLokiConnection:

    _ENDPOINT = "https://loki.example.com/loki/api/v1/push"

    @patch("requests.get")
    def test_successful_response(self, mock_get: MagicMock) -> None:
        mock_get.return_value = MagicMock(status_code=200, ok=True)
        ok, msg = _test_loki_connection(self._ENDPOINT, "user", "pass", "/ca.pem")
        assert ok is True
        assert msg == ""

    @patch("requests.get")
    def test_sends_auth_credentials(self, mock_get: MagicMock) -> None:
        mock_get.return_value = MagicMock(status_code=200, ok=True)
        _test_loki_connection(self._ENDPOINT, "myuser", "mypass", "/ca.pem")
        mock_get.assert_called_once()
        call_kwargs = mock_get.call_args[1]
        assert call_kwargs["auth"] == ("myuser", "mypass")

    @patch("requests.get")
    def test_non_2xx_returns_failure(self, mock_get: MagicMock) -> None:
        mock_get.return_value = MagicMock(status_code=503, ok=False)
        ok, msg = _test_loki_connection(self._ENDPOINT, "user", "pass", "/ca.pem")
        assert ok is False
        assert "503" in msg

    @patch("requests.get")
    def test_ssl_error(self, mock_get: MagicMock) -> None:
        import requests
        mock_get.side_effect = requests.exceptions.SSLError("cert failed")
        ok, msg = _test_loki_connection(self._ENDPOINT, "user", "pass", "/ca.pem")
        assert ok is False
        assert "SSL certificate verification failed" in msg

    @patch("requests.get")
    def test_connection_error(self, mock_get: MagicMock) -> None:
        import requests
        mock_get.side_effect = requests.exceptions.ConnectionError("refused")
        ok, msg = _test_loki_connection(self._ENDPOINT, "user", "pass", "/ca.pem")
        assert ok is False
        assert "Connection failed" in msg

    @patch("requests.get")
    def test_timeout(self, mock_get: MagicMock) -> None:
        import requests
        mock_get.side_effect = requests.exceptions.Timeout("timed out")
        ok, msg = _test_loki_connection(self._ENDPOINT, "user", "pass", "/ca.pem")
        assert ok is False
        assert "Connection timeout" in msg


class TestSafeLokiBatchHandlerSurvivesDictConfig:
    """Regression for the silent-log-drop bug found via mcp-gatekeeper.

    Stdlib's `logging.config.dictConfig` unconditionally calls
    `_clearExistingHandlers()` which closes every handler in
    `_handlerList`. Stock `LokiBatchHandler` inherits MemoryHandler.close,
    which sets `self.target = None` — after that, every subsequent flush()
    is a no-op and records pile up silently. SafeLokiBatchHandler MUST
    override close() to preserve target.
    """

    def test_safe_batch_handler_target_survives_close(self) -> None:
        """close() must NOT clear target — that's the whole point of the subclass."""
        import logging
        from byteforge_loki_logging.logging_config import SafeLokiBatchHandler, SafeLokiHandler

        target = SafeLokiHandler(url="https://example.test/loki/api/v1/push")
        h = SafeLokiBatchHandler(interval=1.0, target=target)
        try:
            assert h.target is target

            h.close()

            assert h.target is target, (
                "SafeLokiBatchHandler.close() cleared self.target — that's the upstream "
                "MemoryHandler.close() behavior we explicitly subclass to prevent."
            )
        finally:
            h.stop_timer()

    def test_safe_batch_handler_target_survives_dictConfig(self) -> None:
        """Full path: create handler, simulate uvicorn's dictConfig, verify target intact."""
        import logging
        import logging.config
        from byteforge_loki_logging.logging_config import SafeLokiBatchHandler, SafeLokiHandler

        target = SafeLokiHandler(url="https://example.test/loki/api/v1/push")
        h = SafeLokiBatchHandler(interval=1.0, target=target)
        logging.getLogger().addHandler(h)

        try:
            # Simulate what uvicorn (or any framework) does when given a
            # log_config — dictConfig non-incrementally calls
            # _clearExistingHandlers() which closes everything in _handlerList.
            logging.config.dictConfig({
                "version": 1,
                "disable_existing_loggers": False,
                "loggers": {
                    "uvicorn": {"handlers": [], "level": "INFO", "propagate": True},
                },
            })

            assert h.target is target, (
                "After dictConfig, SafeLokiBatchHandler.target was cleared — the "
                "regression has returned. Records would silently pile up in buffer "
                "and never reach Loki."
            )
        finally:
            logging.getLogger().removeHandler(h)
            h.stop_timer()


# ===========================================================================
# SafeLokiBatchHandler — background flush timer
# ===========================================================================

class TestSafeLokiBatchHandlerPeriodicFlush:
    """Regression for the stranded-trailing-records bug (idle bursty services).

    Stock MemoryHandler flushes only inside emit(), so the last record(s) of a
    burst sit in the buffer until the next emit — minutes later, or never, in a
    long-running idle service. SafeLokiBatchHandler MUST flush on a wall-clock
    timer so trailing records ship on their own.
    """

    def test_timer_flushes_buffered_records_without_a_later_emit(self) -> None:
        # The whole point: records already in the buffer must reach the target
        # on the timer alone, with NO subsequent emit() to trigger shouldFlush.
        shipped: list = []
        target = MagicMock()
        target.emit_batch.side_effect = lambda buf: shipped.extend(buf)

        h = SafeLokiBatchHandler(interval=0.1, target=target)
        try:
            h.buffer.append(_make_record(msg="STRANDED", args=()))
            # Wait up to ~2s for a timer tick to flush it. No emit() in between.
            for _ in range(40):
                if shipped:
                    break
                time.sleep(0.05)
            assert len(shipped) == 1
            assert shipped[0].getMessage() == "STRANDED"
        finally:
            h.stop_timer()

    def test_stop_timer_halts_the_flush_thread(self) -> None:
        target = MagicMock()
        h = SafeLokiBatchHandler(interval=0.1, target=target)
        assert h._flush_timer.is_alive()

        h.stop_timer()
        h._flush_timer.join(timeout=1.0)
        assert not h._flush_timer.is_alive()

    def test_close_does_not_stop_the_timer(self) -> None:
        # dictConfig close()s handlers while the process keeps running; the
        # timer must survive close() or the bug returns after any dictConfig.
        target = SafeLokiHandler(url="https://example.test/loki/api/v1/push")
        h = SafeLokiBatchHandler(interval=0.1, target=target)
        try:
            h.close()
            assert h._flush_timer.is_alive()
        finally:
            h.stop_timer()

    @pytest.mark.parametrize("bad_interval", [0, 0.0, -1.0, None])
    def test_non_positive_interval_raises_rather_than_spinning(self, bad_interval: Any) -> None:
        # interval=0 would hot-spin wait(0); interval=None would block forever
        # (no flush). Both must fail loud, not silently misbehave.
        target = SafeLokiHandler(url="https://example.test/loki/api/v1/push")
        with pytest.raises(ValueError, match="interval must be > 0"):
            SafeLokiBatchHandler(interval=bad_interval, target=target)


# ===========================================================================
# flush_logging — public flush / graceful-shutdown helper
# ===========================================================================

class TestFlushLogging:

    def setup_method(self) -> None:
        root = logging.getLogger()
        root.handlers.clear()
        root.setLevel(logging.WARNING)
        # Each test starts as if configure_logging had never registered atexit,
        # so the registration assertions below see a clean slate.
        cfg_module._atexit_registered = False

    def test_returns_true_when_no_handlers_installed(self) -> None:
        assert flush_logging() is True

    def test_returns_true_when_only_non_loki_handlers_present(self) -> None:
        logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
        assert flush_logging() is True

    @patch("byteforge_loki_logging.logging_config.logging_loki.LokiHandler.__init__", return_value=None)
    def test_stops_listener_and_calls_flush(self, mock_init: MagicMock) -> None:
        handler = SafeLokiQueueHandler(Queue(-1), url="http://loki", tags={}, auth=("u", "p"))
        logging.getLogger().addHandler(handler)
        try:
            stop_spy = MagicMock(wraps=handler.listener.stop)
            handler.listener.stop = stop_spy
            flush_spy = MagicMock(wraps=handler.flush)
            handler.flush = flush_spy

            assert flush_logging(timeout=2.0) is True
            stop_spy.assert_called_once()
            flush_spy.assert_called_once()
        finally:
            logging.getLogger().removeHandler(handler)
            try:
                handler.listener.stop()
            except Exception:
                pass

    @patch("byteforge_loki_logging.logging_config.logging_loki.LokiHandler.__init__", return_value=None)
    def test_stops_batch_flush_timer(self, mock_init: MagicMock) -> None:
        # A clean shutdown must also stop the batch handler's flush timer, not
        # just the queue listener — otherwise a daemon thread keeps ticking.
        handler = SafeLokiQueueHandler(
            Queue(-1), batch_interval=0.1, url="http://loki", tags={}, auth=("u", "p")
        )
        logging.getLogger().addHandler(handler)
        try:
            assert handler.handler._flush_timer.is_alive()
            assert flush_logging(timeout=2.0) is True
            handler.handler._flush_timer.join(timeout=1.0)
            assert not handler.handler._flush_timer.is_alive()
        finally:
            logging.getLogger().removeHandler(handler)
            handler.handler.stop_timer()

    @patch("byteforge_loki_logging.logging_config.logging_loki.LokiHandler.__init__", return_value=None)
    def test_is_idempotent(self, mock_init: MagicMock) -> None:
        handler = SafeLokiQueueHandler(Queue(-1), url="http://loki", tags={}, auth=("u", "p"))
        logging.getLogger().addHandler(handler)
        try:
            assert flush_logging(timeout=2.0) is True
            # Second invocation must not raise even though the listener is stopped.
            assert flush_logging(timeout=2.0) is True
        finally:
            logging.getLogger().removeHandler(handler)

    def test_returns_false_when_drain_exceeds_timeout(self) -> None:
        # Stand-in handler whose listener.stop() blocks forever — simulates a
        # dead Loki endpoint hanging the final POST. flush_logging must bound
        # this with its timeout and return False rather than hang on exit.
        slow_handler = MagicMock(spec=SafeLokiQueueHandler)
        block_forever = threading.Event()
        slow_handler.listener = MagicMock()
        slow_handler.listener.stop = MagicMock(side_effect=lambda: block_forever.wait())
        slow_handler.flush = MagicMock()

        with patch(
            "byteforge_loki_logging.logging_config._collect_loki_queue_handlers",
            return_value=[slow_handler],
        ):
            assert flush_logging(timeout=0.1) is False
        block_forever.set()  # let the daemon worker thread unblock

    @patch("byteforge_loki_logging.logging_config._test_loki_connection", return_value=(True, ""))
    @patch("byteforge_loki_logging.logging_config.logging_loki.LokiHandler.__init__", return_value=None)
    @patch("byteforge_loki_logging.logging_config.atexit.register")
    def test_configure_logging_registers_atexit_when_loki_active(
        self, mock_register: MagicMock, mock_init: MagicMock, mock_conn: MagicMock
    ) -> None:
        with patch.dict("os.environ", _env_vars(), clear=True):
            result = configure_logging("myapp")
        try:
            mock_register.assert_called_once_with(flush_logging)
        finally:
            if result and hasattr(result, "listener"):
                result.listener.stop()

    @patch("byteforge_loki_logging.logging_config._test_loki_connection", return_value=(True, ""))
    @patch("byteforge_loki_logging.logging_config.logging_loki.LokiHandler.__init__", return_value=None)
    @patch("byteforge_loki_logging.logging_config.atexit.register")
    def test_configure_logging_atexit_registers_only_once(
        self, mock_register: MagicMock, mock_init: MagicMock, mock_conn: MagicMock
    ) -> None:
        results = []
        try:
            with patch.dict("os.environ", _env_vars(), clear=True):
                results.append(configure_logging("myapp"))
                results.append(configure_logging("myapp"))
            assert mock_register.call_count == 1
        finally:
            for r in results:
                if r and hasattr(r, "listener"):
                    # The first handler is already stopped by the second
                    # configure_logging (reconfigure drains old handlers), so
                    # stop() may double-fire — swallow it.
                    try:
                        r.listener.stop()
                    except Exception:
                        pass
                    if isinstance(r.handler, SafeLokiBatchHandler):
                        r.handler.stop_timer()

    @patch("byteforge_loki_logging.logging_config.atexit.register")
    def test_configure_logging_does_not_register_atexit_for_debug_local(
        self, mock_register: MagicMock
    ) -> None:
        configure_logging("myapp", debug_local=True)
        mock_register.assert_not_called()

    @patch("byteforge_loki_logging.logging_config._test_loki_connection",
           return_value=(False, "Loki /ready returned HTTP 404"))
    @patch("byteforge_loki_logging.logging_config.atexit.register")
    def test_configure_logging_does_not_register_atexit_on_fallback(
        self, mock_register: MagicMock, mock_conn: MagicMock
    ) -> None:
        captured = StringIO()
        with patch.dict("os.environ", _env_vars(), clear=True), patch("sys.stderr", captured):
            configure_logging("myapp")
        mock_register.assert_not_called()


# ===========================================================================
# Push timeout — the black-holed-endpoint wedge (ticket 4c196404)
# ===========================================================================

class TestSafeLokiEmitterTimeout:
    """logging_loki's LokiEmitter posts with no timeout, so a black-holed
    endpoint hangs the flush thread forever and silently wedges the whole
    pipeline. SafeLokiEmitter bounds every POST."""

    def _make_emitter(self, timeout: float = 10.0) -> SafeLokiEmitter:
        emitter = SafeLokiEmitter("http://loki/loki/api/v1/push")
        emitter.push_timeout = timeout
        return emitter

    def test_post_passes_timeout_to_session(self) -> None:
        emitter = self._make_emitter(timeout=3.5)
        session = MagicMock()
        session.post.return_value = MagicMock(status_code=204)
        emitter._session = session

        emitter._post_to_loki({"streams": []})

        assert session.post.call_args.kwargs["timeout"] == 3.5

    def test_post_raises_on_non_success_status(self) -> None:
        emitter = self._make_emitter()
        session = MagicMock()
        session.post.return_value = MagicMock(status_code=503)
        emitter._session = session

        with pytest.raises(ValueError, match="503"):
            emitter._post_to_loki({"streams": []})

    @patch("byteforge_loki_logging.logging_config.logging_loki.LokiHandler.__init__", return_value=None)
    def test_handler_rejects_non_positive_timeout(self, mock_init: MagicMock) -> None:
        with pytest.raises(ValueError, match="push_timeout must be > 0"):
            SafeLokiHandler(push_timeout=0, url="http://loki")

    def test_configure_logging_rejects_non_positive_timeout(self) -> None:
        with patch.dict("os.environ", _env_vars(), clear=True), \
                patch("byteforge_loki_logging.logging_config._test_loki_connection",
                      return_value=(True, "")):
            with pytest.raises(ValueError, match="push_timeout must be > 0"):
                configure_logging("myapp", push_timeout=-1)

    def test_configure_logging_rejects_bad_timeout_even_when_loki_is_down(self) -> None:
        """Validated before the connection test, so an invalid value cannot pass
        silently in dev/CI (stdout fallback) and only blow up in production."""
        with patch.dict("os.environ", _env_vars(), clear=True), \
                patch("byteforge_loki_logging.logging_config._test_loki_connection",
                      return_value=(False, "connection refused")), \
                patch("sys.stderr", StringIO()):
            with pytest.raises(ValueError, match="push_timeout must be > 0"):
                configure_logging("myapp", push_timeout=0)


# ===========================================================================
# Push failures must be visible and must not wedge the handler
# ===========================================================================

class TestPushFailureVisibility:

    @patch("byteforge_loki_logging.logging_config.logging_loki.LokiHandler.__init__", return_value=None)
    def _make_handler(self, mock_init: MagicMock) -> SafeLokiHandler:
        handler = SafeLokiHandler.__new__(SafeLokiHandler)
        handler.emitter = MagicMock()
        handler.format = MagicMock(return_value="formatted")
        handler.consecutive_failures = 0
        handler.push_success_count = 0
        handler.records_dropped = 0
        return handler

    def test_failed_batch_prints_banner_naming_records_lost(self) -> None:
        handler = self._make_handler()
        handler.emitter.emit_batch.side_effect = ConnectionError("endpoint down")
        captured = StringIO()

        with patch("sys.stderr", captured):
            handler.emit_batch([_make_record(), _make_record(), _make_record()])

        out = captured.getvalue()
        assert "LOKI HANDLER ERROR" in out
        assert "Records dropped this batch: 3" in out
        assert handler.records_dropped == 3
        assert handler.consecutive_failures == 1

    def test_failed_batch_closes_emitter_so_next_attempt_reconnects(self) -> None:
        """Without this the handler reuses a dead session forever and only a
        container restart recovers it."""
        handler = self._make_handler()
        handler.emitter.emit_batch.side_effect = ConnectionError("endpoint down")

        with patch("sys.stderr", StringIO()):
            handler.emit_batch([_make_record()])

        handler.emitter.close.assert_called_once()

    def test_never_shipped_anything_is_called_out_explicitly(self) -> None:
        handler = self._make_handler()
        handler.emitter.emit_batch.side_effect = ConnectionError("endpoint down")
        captured = StringIO()

        with patch("sys.stderr", captured):
            handler.emit_batch([_make_record()])

        assert "NEVER successfully delivered" in captured.getvalue()

    def test_success_resets_failure_streak(self) -> None:
        handler = self._make_handler()
        handler.consecutive_failures = 7

        handler.emit_batch([_make_record()])

        assert handler.consecutive_failures == 0
        assert handler.push_success_count == 1

    def test_repeated_failures_are_rate_limited_not_one_banner_each(self) -> None:
        """A wedged endpoint fails every interval; unthrottled banners bury
        real errors. Reports land on failures 1, 2, 4, 8, 16."""
        handler = self._make_handler()
        handler.emitter.emit_batch.side_effect = ConnectionError("endpoint down")
        captured = StringIO()

        with patch("sys.stderr", captured):
            for _ in range(20):
                handler.emit_batch([_make_record()])

        assert captured.getvalue().count("LOKI HANDLER ERROR") == 5
        assert handler.consecutive_failures == 20
        assert handler.records_dropped == 20

    def test_sustained_failure_warns_that_loki_data_is_incomplete(self) -> None:
        handler = self._make_handler()
        handler.push_success_count = 1     # has shipped before, so not the "never" case
        handler.emitter.emit_batch.side_effect = ConnectionError("endpoint down")
        captured = StringIO()

        with patch("sys.stderr", captured):
            for _ in range(4):
                handler.emit_batch([_make_record()])

        assert "INCOMPLETE" in captured.getvalue()

    def test_single_record_failure_prints_exactly_one_banner(self) -> None:
        """The non-batching path (batch_interval=None) reports via handleError.
        Counting and reporting are split so it does not also print a batch
        banner — two banners per dropped line is noise, not signal."""
        handler = self._make_handler()
        handler.emitter.side_effect = ConnectionError("endpoint down")
        captured = StringIO()

        with patch("sys.stderr", captured):
            handler.emit(_make_record())

        assert captured.getvalue().count("LOKI HANDLER ERROR") == 1
        # ...and the failure is still accounted for and the session dropped.
        assert handler.consecutive_failures == 1
        assert handler.records_dropped == 1
        handler.emitter.close.assert_called_once()

    def test_single_record_failures_are_rate_limited_too(self) -> None:
        """The batch_interval=None path prints a banner AND a full traceback, so
        an unreachable endpoint there was 10 banners for 10 log lines."""
        handler = self._make_handler()
        handler.emitter.side_effect = ConnectionError("endpoint down")
        captured = StringIO()

        with patch("sys.stderr", captured):
            for _ in range(20):
                handler.emit(_make_record())

        assert captured.getvalue().count("LOKI HANDLER ERROR") == 5
        assert handler.consecutive_failures == 20

    def test_formatter_error_is_not_reported_as_a_loki_outage(self) -> None:
        """A formatter bug must not increment delivery counters or tear down a
        healthy HTTP session — operators would get an outage banner for it."""
        handler = self._make_handler()
        handler.format = MagicMock(side_effect=ValueError("bad format string"))

        with patch("sys.stderr", StringIO()):
            handler.emit(_make_record())
            handler.emit_batch([_make_record(), _make_record()])

        assert handler.consecutive_failures == 0
        assert handler.records_dropped == 0
        handler.emitter.close.assert_not_called()
        handler.emitter.emit_batch.assert_not_called()

    def test_skipped_push_is_not_counted_as_a_delivery(self) -> None:
        """logging_loki wraps the emitter in @with_lock, which returns WITHOUT
        posting when the lock is held. A clean return is not proof of delivery,
        and the batch buffer is cleared either way."""
        handler = SafeLokiHandler(url="http://loki.test/loki/api/v1/push")
        handler.emitter._session = MagicMock()
        captured = StringIO()

        # Hold the emitter lock exactly as a concurrent caller would.
        handler.emitter._lock.acquire()
        try:
            with patch("sys.stderr", captured):
                handler.emit_batch([_make_record(), _make_record(), _make_record()])
        finally:
            handler.emitter._lock.release()

        assert handler.push_success_count == 0
        assert handler.records_dropped == 3
        assert "SKIPPED" in captured.getvalue()

    def test_successful_batch_counts_attempts_and_reports_delivery(self) -> None:
        handler = SafeLokiHandler(url="http://loki.test/loki/api/v1/push")
        session = MagicMock()
        session.post.return_value = MagicMock(status_code=204)
        handler.emitter._session = session

        handler.emit_batch([_make_record(), _make_record()])

        assert handler.push_success_count == 1
        assert handler.records_dropped == 0
        assert handler.emitter.post_attempts == 1

    def test_push_timeout_is_keyword_only_so_a_positional_url_still_works(self) -> None:
        """SafeLokiHandler("https://loki/...") must not land the URL in
        push_timeout and raise an unrelated TypeError."""
        handler = SafeLokiHandler("http://loki.test/loki/api/v1/push")
        assert handler.emitter.url == "http://loki.test/loki/api/v1/push"
        assert handler.emitter.push_timeout == 10.0

    def test_handle_error_accepts_an_exception_not_just_a_record(self) -> None:
        """Upstream LokiHandler calls handleError(exc); inherited paths can
        hand us either shape."""
        handler = self._make_handler()
        captured = StringIO()

        with patch("sys.stderr", captured):
            handler.handleError(ConnectionError("boom"))

        assert "LOKI HANDLER ERROR" in captured.getvalue()


# ===========================================================================
# End-to-end regression: a hung endpoint must not wedge the pipeline
# ===========================================================================

class TestHungEndpointDoesNotWedgePipeline:

    def test_queue_drains_and_handler_recovers_without_restart(self) -> None:
        """Regression for ticket 4c196404: pre-fix this delivered 0 records,
        left the queue growing, printed nothing, and never recovered."""
        import os
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

        captured_bodies: list[str] = []
        healed = threading.Event()

        class Catcher(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args: Any) -> None:
                pass

            def do_GET(self) -> None:
                self.send_response(200)
                self.send_header("Content-Length", "5")
                self.end_headers()
                self.wfile.write(b"ready")

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length).decode()
                if not healed.is_set():
                    healed.wait(10)      # black hole: accept, never answer
                    return
                captured_bodies.append(body)
                self.send_response(204)
                self.send_header("Content-Length", "0")
                self.end_headers()

        server = ThreadingHTTPServer(("127.0.0.1", 0), Catcher)
        port = server.server_address[1]
        threading.Thread(target=server.serve_forever, daemon=True).start()

        env = {
            "LOKI_ENDPOINT": f"http://127.0.0.1:{port}/loki/api/v1/push",
            "LOKI_USER": "u",
            "LOKI_PASSWORD": "p",
            "LOKI_CA_BUNDLE_PATH": "false",
        }
        root = logging.getLogger()
        saved_handlers = list(root.handlers)
        handler = None
        try:
            with patch.dict(os.environ, env, clear=True), patch("sys.stderr", StringIO()):
                handler = configure_logging(
                    "wedge-test", batch_interval=0.2, push_timeout=0.5
                )
                log = logging.getLogger("wedge.test")

                log.info("DEAD-1")
                time.sleep(1.5)          # POST times out instead of hanging forever

                healed.set()
                for i in range(3):
                    log.info(f"ALIVE-{i}")
                time.sleep(1.5)

                diagnostics = handler.get_diagnostics()
        finally:
            if handler is not None:
                flush_logging(timeout=2.0)
            server.shutdown()
            root.handlers = saved_handlers

        # The pipeline kept moving: nothing stranded in the queue...
        assert diagnostics["queue_size"] == 0
        # ...the failure was counted rather than swallowed...
        assert diagnostics["records_dropped"] >= 1
        # ...and it resumed shipping with no restart.
        assert diagnostics["push_success_count"] >= 1
        assert any("ALIVE-" in body for body in captured_bodies)
