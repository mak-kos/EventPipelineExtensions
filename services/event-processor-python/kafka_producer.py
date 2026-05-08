"""Thin wrapper over confluent_kafka.Producer for replay.

Kept separate from consumer.py so the replay path doesn't drag in the consumer
state and so tests can substitute a fake producer easily.
"""

from __future__ import annotations

import logging
from typing import Protocol


log = logging.getLogger(__name__)


class Producer(Protocol):
    def produce(self, topic: str, key: bytes, value: bytes) -> None: ...
    def flush(self, timeout: float = ...) -> int: ...


class KafkaPublisher:
    def __init__(self, producer: Producer, topic: str, flush_timeout_seconds: float = 5.0) -> None:
        self._producer = producer
        self._topic = topic
        self._flush_timeout = flush_timeout_seconds

    def publish(self, key: str, value: bytes) -> None:
        """Publish synchronously (produce + flush). Raises if delivery fails or times out."""
        self._producer.produce(self._topic, key=key.encode("utf-8"), value=value)
        remaining = self._producer.flush(timeout=self._flush_timeout)
        if remaining > 0:
            raise TimeoutError(
                f"Kafka publish did not flush within {self._flush_timeout}s "
                f"({remaining} message(s) still queued)"
            )
