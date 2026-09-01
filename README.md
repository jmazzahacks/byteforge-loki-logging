# byteforge-loki-logging

Python logging library with Grafana Loki integration, async queue-based handler, and structured JSON formatting.

## Installation

```bash
pip install git+https://github.com/jmazzahacks/byteforge-loki-logging.git
```

## Quick Start

```python
from byteforge_loki_logging import configure_logging
import logging

# Configure logging with Loki
configure_logging(application_tag="my-service")

# Use standard Python logging
logger = logging.getLogger(__name__)
logger.info("Request processed", extra={"user_id": "123", "latency_ms": 42})
```

## Environment Variables

Set these when running in production (not needed when `debug_local=True`):

| Variable | Description |
|----------|-------------|
| `LOKI_ENDPOINT` | Loki push API URL (e.g. `https://loki.example.com/loki/api/v1/push`) |
| `LOKI_USER` | HTTP Basic Auth username |
| `LOKI_PASSWORD` | HTTP Basic Auth password |
| `LOKI_CA_BUNDLE_PATH` | Path to CA `.pem` file, or `"false"` to disable SSL verification |

### `LOKI_ENDPOINT` must be the full push URL

A common deployment mistake is setting `LOKI_ENDPOINT` to the base URL of your
Loki instance. The library expects the full push API path:

```bash
# Wrong — log POSTs go to / and silently 404:
LOKI_ENDPOINT=https://loki.example.com:8443

# Right:
LOKI_ENDPOINT=https://loki.example.com:8443/loki/api/v1/push
```

## Usage

### Production (Loki)

```python
configure_logging(application_tag="my-service")
```

Requires all four environment variables. Logs are sent asynchronously via a
background thread. Records are batched and POSTed to Loki on a real wall-clock
timer (every `batch_interval` seconds, default `1.0`), so trailing records ship
even when the process goes idle between bursts.

For low-volume alerting or ingest services where per-record latency matters more
than POST count, disable batching so each record ships immediately:

```python
configure_logging(application_tag="my-service", batch_interval=None)
```

### Local Development

```python
configure_logging(application_tag="my-service", debug_local=True)
```

Logs to stdout with a human-readable format. No environment variables needed.

### Structured JSON Logging

When `json_format=True` (default), log records are formatted as JSON:

```json
{"logger": "myapp", "level": "INFO", "message": "Request processed", "user_id": "123", "latency_ms": 42}
```

Query in Grafana: `{application="my-service"} | json | user_id="123"`

### Graceful Fallback

If the Loki connection test fails at startup, logging automatically falls back
to stdout and a loud banner-style warning is printed to stderr (with the
endpoint, error, and a hint about the `/loki/api/v1/push` path). Your
application never crashes due to logging issues, but operators are explicitly
told that logs are not reaching Loki.

### Delivery failures are loud

Startup is not the only way Loki delivery breaks. An endpoint that is healthy at
`configure_logging()` time and fails later — cut over to a new host, black-holed
by a route change, returning 5xx — fails on the *steady-state push* path instead.

Failed pushes print a `LOKI HANDLER ERROR` banner to stderr (rate-limited — see
below) naming how many records were dropped, how long the failure has been going
on, and whether the handler has **ever** successfully delivered a batch (the
"this service has been silent since boot and nobody noticed" case):

```
============================================================
LOKI HANDLER ERROR: Failed to send log batch to Loki
============================================================
Records dropped this batch: 3
Consecutive failed pushes: 4 (total records dropped: 9)
Loki delivery has been failing repeatedly. Logs queried from Loki for this
service are INCOMPLETE for this period.
```

Banners are rate-limited to the 1st, 2nd, 4th, 8th ... consecutive failure — on
both the batched and the `batch_interval=None` path — so a wedged endpoint
reports continuously without burying real errors. The handler closes its HTTP
session after each failure, so it **reconnects and resumes on its own** once the
endpoint recovers — no restart required.

Every POST is bounded by `push_timeout` (default 10s). Note this is `requests`'
timeout, which applies per socket operation rather than as a total deadline, so a
slow-trickle endpoint can still exceed it overall. It matters more than it
sounds: `logging_loki` itself passes no timeout, so an endpoint that accepts the
connection and never answers parks the flush thread inside that POST forever,
blocks the queue listener behind it, and silently stops shipping for the life of
the process.

Counters are available for health checks and diagnostics:

```python
handler = configure_logging(application_tag="my-service")
if handler:
    handler.get_diagnostics()
    # {'enqueued_count': 1043, 'queue_size': 0, 'push_success_count': 87,
    #  'consecutive_failures': 0, 'records_dropped': 0}
```

A non-zero `consecutive_failures`, or a `push_success_count` of `0` on a service
that has been running a while, means logs queried from Loki are incomplete.

### Short-Lived Processes (CLI / cron jobs)

The async queue means a process can exit before the background listener has
POSTed the last enqueued records, silently dropping the final log lines.
`configure_logging()` auto-registers a flush via `atexit` when a Loki handler
is installed, so most scripts get correct behavior with no code change. For
explicit, bounded shutdown — e.g. from a `finally` block — call
`flush_logging()`:

```python
from byteforge_loki_logging import configure_logging, flush_logging

configure_logging(application_tag="refresh-asn-table")
try:
    do_work()
finally:
    flush_logging(timeout=5.0)   # blocks until drained, or returns False on timeout
```

`flush_logging()` is idempotent and a no-op when no Loki handler is configured
(e.g. `debug_local=True`).

## API

### `configure_logging(application_tag, debug_local=False, local_level=logging.INFO, json_format=True, batch_interval=1.0, push_timeout=10.0)`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `application_tag` | `str` | required | Application identifier used as a Loki label |
| `debug_local` | `bool` | `False` | Log to stdout instead of Loki |
| `local_level` | `int \| str` | `logging.INFO` | Logging level (e.g. `logging.DEBUG`, `"WARNING"`) |
| `json_format` | `bool` | `True` | Use JSON formatting for structured queries |
| `batch_interval` | `float \| None` | `1.0` | Seconds between batched POSTs, flushed on a background timer. `None`/`0` disables batching (ship each record immediately) |
| `push_timeout` | `float` | `10.0` | Max seconds any single POST to Loki may take. Must be > 0 — see [Delivery failures](#delivery-failures-are-loud) |

Returns `SafeLokiQueueHandler` on success, `None` on fallback/local mode.

### `flush_logging(timeout=5.0)`

Drains the async queue and flushes every Loki handler on the root logger.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `timeout` | `float` | `5.0` | Max seconds to wait for the drain (so a dead Loki can't hang exit) |

Returns `True` if fully flushed in time (or if no Loki handler is configured),
`False` on timeout. Safe to call multiple times and from `atexit`.

## License

MIT
