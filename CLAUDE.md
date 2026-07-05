# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

This is a `src`-layout library installed into a local virtualenv at the repo root. Always activate it first.

```bash
source bin/activate

pytest                                          # run the full suite
pytest tests/test_logging_config.py::TestFlushLogging   # one class
pytest -k "banner"                              # one test by name substring
pip install --upgrade -e ".[dev]"               # (re)install with dev tools

black src tests                                 # format (line-length 100)
ruff check src tests                            # lint
mypy src                                         # type-check (strict: disallow_untyped_defs)
```

There is no separate build/CI step; the library is consumed directly from GitHub via
`pip install git+https://github.com/jmazzahacks/byteforge-loki-logging.git`.

## Release checklist

This is a library. Before committing, bump the version in **both** `pyproject.toml` and
`src/byteforge_loki_logging/__init__.py` (`__version__`) — they must match. Run `pytest` before deciding to commit.

## Architecture

The entire implementation is one module, `src/byteforge_loki_logging/logging_config.py`. The public
surface (`__init__.py`) is just `configure_logging`, `flush_logging`, and `LokiJsonFormatter`.

### The async handler stack

`configure_logging()` installs a single root-logger handler that is a chain of four cooperating pieces —
understanding why each exists is the key to working here:

```
logger.emit()
  → SafeLokiQueueHandler   (QueueHandler: enqueues in microseconds, never blocks on HTTP)
      → QueueListener       (background thread, drains the queue)
          → SafeLokiBatchHandler   (MemoryHandler subclass: buffers, POSTs on a background wall-clock timer every batch_interval)
              → SafeLokiHandler    (the real logging_loki.LokiHandler: HTTP POST to Loki)
```

The three `Safe*` subclasses each exist to fix a specific, silent production failure. Do not "simplify"
them away without understanding the failure each prevents (all documented in their docstrings):

- **`SafeLokiHandler`** — overrides `handleError`/`emit` to print to `stderr` instead of routing errors
  back through the logging system, which would infinitely recurse (a Loki error logged via Loki...).
- **`SafeLokiBatchHandler`** — fixes two silent MemoryHandler drops. (a) Runs a **background daemon
  timer** that `flush()`es every `interval` seconds; stdlib only flushes inside `emit()`, so without
  this the trailing records of a burst strand in the buffer until the *next* emit (forever, in an idle
  service). (b) Overrides `close()` so it does **not** null out `self.target` (stdlib `MemoryHandler.close()`
  sets `target = None`, and `dictConfig()` calls `close()` on every handler — in Flask/gunicorn/FastAPI
  apps that would leave the handler alive but silently discarding every record). Note `close()`
  deliberately does **not** stop the timer, since `dictConfig` closes handlers while the process keeps
  running. The timer is stopped only via `stop_timer()`, which `flush_logging()` and the queue handler's
  `__del__` call on clean shutdown.
- **`SafeLokiQueueHandler`** — the QueueHandler front end; owns/starts the `QueueListener`, tracks
  `enqueued_count` for `get_diagnostics()`, and is what `flush_logging()` looks for on the root logger.

### configure_logging control flow

`debug_local=True` → plain stdout handler, returns `None`, no Loki, no atexit. Otherwise: validate the
four `LOKI_*` env vars → **connection-test** `/ready` → on failure, print a loud `stderr` banner and fall
back to stdout (returns `None`, app never crashes) → on success, build the async stack and register the
atexit flush.

Gotcha in the connection test: `_test_loki_connection` strips `/loki/api/v1/push` off the endpoint and
probes `<base>/ready`. So a *base-URL-only* `LOKI_ENDPOINT` passes the probe but then 404s on every log
POST. This is why the banner and README both insist `LOKI_ENDPOINT` be the full push URL.

### flush_logging + short-lived processes

The async queue means a process can exit before the listener POSTs the last records, dropping final log
lines. `configure_logging()` auto-registers `flush_logging` via `atexit` (guarded by the module-level
`_atexit_registered` flag so repeat calls don't stack). `flush_logging(timeout=5.0)` drains on a daemon
thread with a bounded `Event.wait()` so a dead Loki endpoint can't hang exit; it is idempotent and a
no-op when no Loki handler is installed.

### Structured JSON

`LokiJsonFormatter` emits `{logger, level, message, ...extras}`. Any non-standard `LogRecord` attribute
(i.e. anything passed via `logger.info(..., extra={...})` and not in `_STANDARD_RECORD_ATTRS`) is folded
in as a top-level JSON key, enabling `{application="x"} | json | field="y"` queries in Grafana.

## Conventions

- Time is always stored/compared as unix timestamps; `datetime`/`time.gmtime` are used only for output
  formatting.
- Full type hints on every function; no lambdas (see the named inner functions like `_drain`).
