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

Requires all four environment variables. Logs are sent asynchronously via a background thread with 1-second batching.

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

## API

### `configure_logging(application_tag, debug_local=False, local_level=logging.INFO, json_format=True)`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `application_tag` | `str` | required | Application identifier used as a Loki label |
| `debug_local` | `bool` | `False` | Log to stdout instead of Loki |
| `local_level` | `int \| str` | `logging.INFO` | Logging level (e.g. `logging.DEBUG`, `"WARNING"`) |
| `json_format` | `bool` | `True` | Use JSON formatting for structured queries |

Returns `SafeLokiQueueHandler` on success, `None` on fallback/local mode.

## License

MIT
