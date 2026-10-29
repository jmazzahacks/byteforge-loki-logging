"""Real local forks with mocked HTTP: no production endpoints are contacted."""
import logging
import os
import signal
import threading
import time
import traceback
from queue import Queue
from typing import Callable, Optional
from unittest.mock import patch

import pytest

from byteforge_loki_logging.logging_config import (
    SafeLokiBatchHandler,
    SafeLokiEmitter,
    SafeLokiQueueHandler,
    configure_logging,
    flush_logging,
)

pytestmark = [
    pytest.mark.skipif(not hasattr(os, "fork"), reason="requires os.fork"),
    pytest.mark.filterwarnings("ignore:This process.*is multi-threaded:DeprecationWarning"),
]


def _record(message: str) -> logging.LogRecord:
    return logging.LogRecord("fork-test", logging.INFO, __file__, 1, message, (), None)


def _run_child(check: Callable[[], None]) -> None:
    """Bound deadlocks and propagate child assertion failures to pytest."""
    pid = os.fork()
    if pid == 0:
        signal.alarm(5)
        try:
            check()
        except BaseException:
            traceback.print_exc()
            os._exit(1)
        os._exit(0)
    deadline = time.time() + 7
    while time.time() < deadline:
        finished, status = os.waitpid(pid, os.WNOHANG)
        if finished:
            assert os.waitstatus_to_exitcode(status) == 0
            return
        time.sleep(0.01)
    os.kill(pid, signal.SIGKILL)
    os.waitpid(pid, 0)
    pytest.fail("forked child hung")


@pytest.mark.parametrize("batch_interval", [None, 0.05])
@pytest.mark.parametrize("reconfigure", [False, True])
def test_child_delivery_and_parent_isolation(
    batch_interval: Optional[float], reconfigure: bool
) -> None:
    sent = []
    delivered = threading.Event()

    def capture_post(self: SafeLokiEmitter, payload: dict) -> None:
        self.post_attempts += 1
        sent.append(payload)
        delivered.set()

    root = logging.getLogger()
    original_handlers = root.handlers[:]
    original_level = root.level
    env = {
        "LOKI_ENDPOINT": "http://unused.invalid/loki/api/v1/push",
        "LOKI_USER": "test", "LOKI_PASSWORD": "test", "LOKI_CA_BUNDLE_PATH": "false",
    }
    with patch.dict(os.environ, env), patch(
        "byteforge_loki_logging.logging_config._test_loki_connection", return_value=(True, "")
    ), patch.object(SafeLokiEmitter, "_post_to_loki", capture_post):
        root.handlers = []
        handler = configure_logging("fork-test", batch_interval=batch_interval)
        assert isinstance(handler, SafeLokiQueueHandler)
        held = threading.Event()
        release = threading.Event()
        batch = handler.handler
        if isinstance(batch, SafeLokiBatchHandler):
            batch.stop_timer()
            batch._flush_timer.join(timeout=1)
            batch.buffer.append(_record("parent-buffered"))

        def hold_inherited_locks() -> None:
            # These ordinary queue/Event locks are NOT reset by logging's hook.
            with handler.queue.mutex:
                if isinstance(batch, SafeLokiBatchHandler):
                    batch.acquire()
                    batch._timer_stop._cond.acquire()
                try:
                    handler.queue._put(_record("parent-queued"))
                    handler.queue.unfinished_tasks += 1
                    handler.queue.not_empty.notify()
                    held.set()
                    release.wait(15)
                finally:
                    if isinstance(batch, SafeLokiBatchHandler):
                        batch._timer_stop._cond.release()
                        batch.release()

        holder = threading.Thread(target=hold_inherited_locks)
        holder.start()
        assert held.wait(2)

        def child_check() -> None:
            assert handler.get_diagnostics()["queue_size"] == 0
            assert handler.get_diagnostics()["buffered_count"] == 0
            handler.handle(_record("child-before-reconfigure"))
            # With batching enabled, this specifically proves the timer restarts.
            assert delivered.wait(2), "child has no live delivery pipeline"
            if reconfigure:
                handler.handle(_record("child-pending-at-reconfigure"))
                configure_logging("worker", batch_interval=batch_interval)
                logging.info("child-after-reconfigure")
            assert flush_logging(timeout=2)
            assert flush_logging(timeout=2), "repeated shutdown must be safe"
            text = str(sent)
            assert text.count("child-before-reconfigure") == 1
            assert "parent-queued" not in text
            assert "parent-buffered" not in text
            if reconfigure:
                assert text.count("child-pending-at-reconfigure") == 1
                assert text.count("child-after-reconfigure") == 1
            logging.shutdown()

        try:
            _run_child(child_check)
        finally:
            release.set()
            holder.join(timeout=2)
            assert flush_logging(timeout=2)
            root.handlers = original_handlers
            root.setLevel(original_level)
        text = str(sent)
        assert text.count("parent-queued") == 1
        assert "child-before-reconfigure" not in text
        if isinstance(batch, SafeLokiBatchHandler):
            assert text.count("parent-buffered") == 1


def test_child_shutdown_without_emitting_does_not_replay_parent() -> None:
    handler = SafeLokiQueueHandler(
        Queue(), batch_interval=60, url="http://unused.invalid/loki/api/v1/push"
    )
    batch = handler.handler
    assert isinstance(batch, SafeLokiBatchHandler)
    batch.buffer.append(_record("parent-only"))
    root = logging.getLogger()
    original = root.handlers[:]
    root.handlers = [handler]
    with patch.object(SafeLokiEmitter, "_post_to_loki") as post:
        def child_check() -> None:
            assert flush_logging(timeout=1)
            assert flush_logging(timeout=1)
            assert handler.queue.empty(), "shutdown must not queue orphan sentinels"
            assert handler.listener._thread is None
            assert not batch._flush_timer.is_alive()
            logging.shutdown()
            post.assert_not_called()
        try:
            _run_child(child_check)
        finally:
            batch.buffer.clear()
            flush_logging(timeout=1)
            root.handlers = original
