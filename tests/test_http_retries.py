"""Transient HTTP failures must retain records without spinning or hiding loss."""

import logging
import threading
from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import requests

from byteforge_loki_logging.logging_config import (
    LokiPushError,
    SafeLokiBatchHandler,
    SafeLokiHandler,
    SafeLokiQueueHandler,
    configure_logging,
    flush_logging,
)


def record(message: str = "hello", level: int = logging.INFO) -> logging.LogRecord:
    return logging.LogRecord("retry-test", level, __file__, 1, message, (), None)


@pytest.fixture
def batch() -> Iterator[tuple[SafeLokiBatchHandler, SafeLokiHandler, MagicMock]]:
    target = SafeLokiHandler(url="http://loki.test/loki/api/v1/push")
    session = MagicMock()
    session.post.return_value = MagicMock(status_code=204, text="")
    target.emitter.session_class = MagicMock(return_value=session)
    handler = SafeLokiBatchHandler(60.0, target=target, capacity=2, max_buffer_size=3)
    handler.stop_timer()
    handler._flush_timer.join(timeout=1)
    try:
        yield handler, target, session
    finally:
        handler.buffer.clear()
        handler.close()
        target.close()


@pytest.mark.parametrize("status", [408, 429, 500, 502, 503, 504, 599])
def test_transient_http_retains_then_delivers_same_payload(
    batch: tuple[SafeLokiBatchHandler, SafeLokiHandler, MagicMock],
    status: int,
    capsys: pytest.CaptureFixture[str],
) -> None:
    handler, target, session = batch
    session.post.side_effect = [
        MagicMock(status_code=status, text="empty ring"),
        MagicMock(status_code=204, text=""),
    ]
    handler.handle(record())
    handler.flush()
    assert len(handler.buffer) == 1
    assert target.records_dropped == 0
    assert target.push_success_count == 0
    assert target.consecutive_failures == 1
    output = capsys.readouterr().err
    assert "Records dropped this batch: 0" in output
    assert "Records retained for retry: 1" in output
    assert "empty ring" in output
    assert str(status) in output

    # Manual flush, close/dictConfig, capacity and ERROR must all obey cooldown.
    handler.flush()
    handler.close()
    assert session.post.call_count == 1
    with patch("byteforge_loki_logging.logging_config.time.time", return_value=handler._retry_at):
        handler.flush()
    assert session.post.call_count == 2
    assert (
        session.post.call_args_list[0].kwargs["json"]
        == session.post.call_args_list[1].kwargs["json"]
    )
    assert handler.buffer == []
    assert target.records_dropped == 0
    assert target.push_success_count == 1
    assert target.consecutive_failures == 0


def test_outage_bounds_backlog_and_drops_newest_without_hammering(
    batch: tuple[SafeLokiBatchHandler, SafeLokiHandler, MagicMock],
    capsys: pytest.CaptureFixture[str],
) -> None:
    handler, target, session = batch
    session.post.return_value = MagicMock(status_code=429, text="rate limited")
    for i in range(20):
        handler.handle(record(str(i), level=logging.ERROR))
    assert session.post.call_count == 1
    assert [r.msg for r in handler.buffer] == ["0", "1", "2"]
    assert target.records_dropped == 17
    assert target.consecutive_failures == 1
    assert capsys.readouterr().err.count("batch buffer full") == 5  # 1, 2, 4, 8, 16

    session.post.reset_mock()
    session.post.return_value = MagicMock(status_code=204, text="")
    with patch("byteforge_loki_logging.logging_config.time.time", return_value=handler._retry_at):
        handler.flush()
    payloads = [call.kwargs["json"]["streams"] for call in session.post.call_args_list]
    assert [len(streams) for streams in payloads] == [2, 1]
    assert [stream["values"][0][1] for streams in payloads for stream in streams] == ["0", "1", "2"]
    assert target.records_dropped == 17
    assert handler.buffer == []


@pytest.mark.parametrize("status", [400, 401, 403, 404, 413, 302])
def test_permanent_http_failure_is_dropped_once(
    batch: tuple[SafeLokiBatchHandler, SafeLokiHandler, MagicMock], status: int
) -> None:
    handler, target, session = batch
    session.post.return_value = MagicMock(status_code=status, text="rejected")
    handler.handle(record())
    handler.flush()
    handler.flush()
    assert handler.buffer == []
    assert target.records_dropped == 1
    assert session.post.call_count == 1


@pytest.mark.parametrize(
    "error", [requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout]
)
def test_timeouts_remain_non_retryable(
    batch: tuple[SafeLokiBatchHandler, SafeLokiHandler, MagicMock], error: type[Exception]
) -> None:
    handler, target, session = batch
    session.post.side_effect = error("slow endpoint")
    handler.handle(record())
    handler.flush()
    assert handler.buffer == []
    assert target.records_dropped == 1
    assert session.post.call_count == 1


def test_shutdown_discards_failure_even_when_it_started_before_shutdown(
    batch: tuple[SafeLokiBatchHandler, SafeLokiHandler, MagicMock],
) -> None:
    handler, target, session = batch

    def reject_during_shutdown(*args: Any, **kwargs: Any) -> MagicMock:
        handler.begin_shutdown()
        return MagicMock(status_code=503, text="unavailable")

    session.post.side_effect = reject_during_shutdown
    handler.handle(record())
    handler.flush()
    handler.flush()
    assert handler.buffer == []
    assert target.records_dropped == 1
    assert session.post.call_count == 1


def test_flush_logging_makes_final_attempt_and_does_not_leave_retry_backlog() -> None:
    from queue import Queue

    handler = SafeLokiQueueHandler(
        Queue(), batch_interval=60, max_buffer_size=3, url="http://loki.test"
    )
    inner = handler.handler
    assert isinstance(inner, SafeLokiBatchHandler)
    inner.stop_timer()
    target = inner.target
    session = MagicMock()
    session.post.return_value = MagicMock(status_code=503, text="unavailable")
    target.emitter.session_class = MagicMock(return_value=session)
    root = logging.getLogger()
    try:
        inner.handle(record("retained"))
        inner.flush()
        assert handler.get_diagnostics()["buffered_count"] == 1
        root.addHandler(handler)
        handler.emit(record("queued"))
        assert flush_logging(timeout=2)
        assert inner.buffer == []
        assert handler.get_diagnostics()["queue_size"] == 0
        assert target.records_dropped == 2
        assert session.post.call_count == 2
        assert flush_logging(timeout=2)
        assert session.post.call_count == 2
    finally:
        root.removeHandler(handler)
        inner.begin_shutdown()
        if handler.listener._thread is not None:
            handler.listener.stop()
        handler.close()


def test_timer_recovers_idle_batch_without_another_log_record() -> None:
    target = SafeLokiHandler(url="http://loki.test")
    session = MagicMock()
    delivered = threading.Event()
    attempts = 0

    def post(*args: Any, **kwargs: Any) -> MagicMock:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return MagicMock(status_code=500, text="empty ring")
        delivered.set()
        return MagicMock(status_code=204, text="")

    session.post.side_effect = post
    target.emitter.session_class = MagicMock(return_value=session)
    handler = SafeLokiBatchHandler(0.05, target=target)
    try:
        handler.handle(record())
        assert delivered.wait(2)
    finally:
        handler.stop_timer()
        handler._flush_timer.join(timeout=1)
        handler.close()
        target.close()
    assert attempts == 2
    assert target.records_dropped == 0
    assert target.push_success_count == 1


def test_http_error_excerpt_is_bounded_and_escaped() -> None:
    error = LokiPushError(500, "empty ring\n\x1b[31m" + "x" * 5000)
    assert "empty ring" in str(error)
    assert "\n" not in str(error)
    assert "\x1b" not in str(error)
    assert len(str(error)) < 650


def test_repeated_transient_failures_only_count_a_drop_when_permanently_rejected(
    batch: tuple[SafeLokiBatchHandler, SafeLokiHandler, MagicMock],
    capsys: pytest.CaptureFixture[str],
) -> None:
    handler, target, session = batch
    session.post.return_value = MagicMock(status_code=503, text="empty ring")
    handler.handle(record())
    for _ in range(20):
        with patch(
            "byteforge_loki_logging.logging_config.time.time", return_value=handler._retry_at
        ):
            handler.flush()
    assert target.consecutive_failures == 20
    assert target.records_dropped == 0
    assert capsys.readouterr().err.count("Records retained for retry: 1") == 5
    session.post.return_value = MagicMock(status_code=401, text="unauthorized")
    with patch("byteforge_loki_logging.logging_config.time.time", return_value=handler._retry_at):
        handler.flush()
    assert target.records_dropped == 1
    assert handler.buffer == []


def test_unbatched_http_failure_is_reported_and_dropped() -> None:
    target = SafeLokiHandler(url="http://loki.test")
    session = MagicMock()
    session.post.return_value = MagicMock(status_code=503, text="unavailable")
    target.emitter.session_class = MagicMock(return_value=session)
    try:
        target.emit(record())
        assert target.records_dropped == 1
        assert session.post.call_count == 1
    finally:
        target.close()


@pytest.mark.parametrize("shutdown", [False, True])
@pytest.mark.parametrize("failure", [requests.exceptions.ReadTimeout("slow"), 401, 503])
def test_failed_backlog_flush_stops_after_one_batch(
    batch: tuple[SafeLokiBatchHandler, SafeLokiHandler, MagicMock],
    shutdown: bool,
    failure: Any,
) -> None:
    handler, target, session = batch
    session.post.return_value = MagicMock(status_code=503, text="unavailable")
    for i in range(3):
        handler.handle(record(str(i)))
    assert len(handler.buffer) == 3
    assert target.records_dropped == 0
    if isinstance(failure, Exception):
        session.post.side_effect = failure
    else:
        session.post.return_value = MagicMock(status_code=failure, text="failed")
    session.post.reset_mock()
    if shutdown:
        handler.begin_shutdown()
    with patch("byteforge_loki_logging.logging_config.time.time", return_value=handler._retry_at):
        handler.flush()
        handler.flush()
        handler.handle(record("later", logging.ERROR))
        assert session.post.call_count == 1
    if shutdown:
        assert handler.buffer == []
        assert target.records_dropped == 4
    elif failure == 503:
        assert [r.msg for r in handler.buffer] == ["0", "1", "2"]
        assert target.records_dropped == 1  # newest arrival overflows
    else:
        assert [r.msg for r in handler.buffer] == ["2", "later"]
        assert target.records_dropped == 2  # attempted records are not retried
        session.post.side_effect = None
        session.post.return_value = MagicMock(status_code=204, text="")
        with patch(
            "byteforge_loki_logging.logging_config.time.time", return_value=handler._retry_at
        ):
            handler.flush()
        assert handler.buffer == []
        assert target.records_dropped == 2


def test_real_http_outage_recovers_on_timer() -> None:
    import json
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    bodies: list[bytes] = []
    delivered = threading.Event()

    class Endpoint(BaseHTTPRequestHandler):
        def log_message(self, *args: Any) -> None:
            pass

        def do_POST(self) -> None:
            bodies.append(self.rfile.read(int(self.headers["Content-Length"])))
            failed = len(bodies) == 1
            body = b"empty ring" if failed else b""
            self.send_response(503 if failed else 204)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            if not failed:
                delivered.set()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Endpoint)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    target = SafeLokiHandler(url=f"http://127.0.0.1:{server.server_port}/loki/api/v1/push")
    handler = SafeLokiBatchHandler(0.05, target=target)
    try:
        handler.handle(record("survives empty ring"))
        assert delivered.wait(3)
    finally:
        handler.stop_timer()
        handler._flush_timer.join(timeout=2)
        handler.close()
        target.close()
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)
    assert len(bodies) == 2
    assert json.loads(bodies[0]) == json.loads(bodies[1])
    assert target.push_success_count == 1
    assert target.records_dropped == 0


def test_full_buffer_shutdown_does_not_post_the_triggering_arrival_after_failure(
    batch: tuple[SafeLokiBatchHandler, SafeLokiHandler, MagicMock],
) -> None:
    handler, target, session = batch
    session.post.return_value = MagicMock(status_code=503, text="unavailable")
    for i in range(3):
        handler.handle(record(str(i)))
    session.post.reset_mock()
    handler.begin_shutdown()
    handler.handle(record("trigger flush", logging.ERROR))
    assert session.post.call_count == 1
    assert handler.buffer == []
    assert target.records_dropped == 4


@pytest.mark.parametrize("limit", [0, -1, 1.5, True, None])
def test_invalid_buffer_limit_fails_before_connection_probe(limit: Any) -> None:
    with patch("byteforge_loki_logging.logging_config._test_loki_connection") as probe:
        with pytest.raises(ValueError, match="max_buffer_size"):
            configure_logging("test", max_buffer_size=limit)
    probe.assert_not_called()


def test_configure_logging_forwards_buffer_limit() -> None:
    with (
        patch(
            "byteforge_loki_logging.logging_config._validate_loki_env_vars",
            return_value=("url", "u", "p", "false"),
        ),
        patch(
            "byteforge_loki_logging.logging_config._test_loki_connection", return_value=(True, "")
        ),
    ):
        handler = configure_logging("test", max_buffer_size=7)
    try:
        assert isinstance(handler, SafeLokiQueueHandler)
        assert handler.handler.max_buffer_size == 7
    finally:
        flush_logging()
        logging.getLogger().removeHandler(handler)
