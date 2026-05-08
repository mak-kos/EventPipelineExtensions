"""Liveness probes that actually talk to Kafka and Minio.

Both checks have explicit short timeouts so the /health endpoint can never
itself become a hang vector for an upstream load balancer.
"""

from __future__ import annotations

import logging
from typing import Tuple

from confluent_kafka.admin import AdminClient


log = logging.getLogger(__name__)


def kafka_reachable(bootstrap: str, timeout_seconds: float) -> Tuple[bool, str]:
    """Returns (ok, detail). ok=True iff a metadata request completes within timeout."""
    try:
        admin = AdminClient({"bootstrap.servers": bootstrap, "socket.timeout.ms": int(timeout_seconds * 1000)})
        # list_topics blocks up to `timeout` seconds; raises KafkaException on failure.
        metadata = admin.list_topics(timeout=timeout_seconds)
        if metadata is None:
            return False, "no metadata returned"
        return True, "ok"
    except Exception as ex:  # confluent_kafka raises generic KafkaException; keep broad here
        log.warning("Kafka health check failed: %s", ex)
        return False, str(ex)


def minio_reachable(minio_client, bucket: str) -> Tuple[bool, str]:
    """Returns (ok, detail). ok=True iff a bucket_exists call returns without error."""
    try:
        # bucket_exists is a HEAD on the bucket and respects the client's timeouts.
        minio_client.bucket_exists(bucket)
        return True, "ok"
    except Exception as ex:
        log.warning("Minio health check failed: %s", ex)
        return False, str(ex)
