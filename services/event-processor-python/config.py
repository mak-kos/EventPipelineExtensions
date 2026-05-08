"""Environment-driven configuration for the event-processor.

All operational knobs live here so the rest of the code never has to read
``os.environ``. Defaults mirror dev/docker-compose.yml so a fresh checkout works
with no extra setup.
"""

import os


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return float(raw)


# Kafka
KAFKA_BOOTSTRAP = _env("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = _env("KAFKA_TOPIC", "events")
KAFKA_GROUP_ID = _env("KAFKA_GROUP_ID", "event-processor-group")
KAFKA_HEALTH_TIMEOUT = _env_float("KAFKA_HEALTH_TIMEOUT", 2.0)

# Minio
MINIO_ENDPOINT = _env("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = _env("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = _env("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = _env("MINIO_BUCKET", "events")
MINIO_SECURE = _env("MINIO_SECURE", "false").lower() == "true"

# HTTP server
HTTP_HOST = _env("HTTP_HOST", "0.0.0.0")
HTTP_PORT = _env_int("HTTP_PORT", 8081)

# Logging
LOG_LEVEL = _env("LOG_LEVEL", "INFO")
