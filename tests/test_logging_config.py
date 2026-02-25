import json
import logging
import logging.handlers
import sys
from io import StringIO
from queue import Queue
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from byteforge_loki_logging.logging_config import (
    LokiJsonFormatter,
    SafeLokiHandler,
    SafeLokiQueueHandler,
    _resolve_log_level,
    _test_loki_connection,
    _configure_loki_internal_logger,
    configure_logging,
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
