"""Kafka consumer that drains messages and dispatches them to the processor.

Task 4 keeps the consumer single-threaded; Task 6 will replace this loop with
a key-partitioned worker pool. The class structure is what enables that — the
loop is parameterised so swapping the body-handling step is trivial.
"""

from __future__ import annotations

import json
import logging
import threading
from typing import Callable

from confluent_kafka import Consumer, KafkaError


log = logging.getLogger(__name__)


class KafkaConsumerLoop:
    def __init__(
        self,
        *,
        bootstrap: str,
        topic: str,
        group_id: str,
        on_message: Callable[[bytes], None],
        on_first_seen: Callable[[str], None],
        poll_timeout: float = 1.0,
    ) -> None:
        self._bootstrap = bootstrap
        self._topic = topic
        self._group_id = group_id
        self._on_message = on_message
        self._on_first_seen = on_first_seen
        self._poll_timeout = poll_timeout
        self._stop = threading.Event()
        self._consumer: Consumer | None = None

    def run(self) -> None:
        self._consumer = Consumer(
            {
                "bootstrap.servers": self._bootstrap,
                "group.id": self._group_id,
                "auto.offset.reset": "earliest",
                # Manual commit so we never advance past a message we failed to
                # store. With "earliest", at-least-once is preserved on restart.
                "enable.auto.commit": False,
            }
        )
        self._consumer.subscribe([self._topic])
        log.info("Kafka consumer subscribed to %s as group %s", self._topic, self._group_id)
        try:
            while not self._stop.is_set():
                msg = self._consumer.poll(timeout=self._poll_timeout)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    log.error("Consumer error: %s", msg.error())
                    continue
                self._handle(msg)
        finally:
            try:
                self._consumer.close()
            except Exception as ex:
                log.warning("Error closing Kafka consumer: %s", ex)

    def stop(self) -> None:
        self._stop.set()

    def _handle(self, msg) -> None:
        raw = msg.value()
        # Best-effort id extraction so we can mark the event as pending before
        # the (possibly slow) Minio put. If it fails, we still process the
        # message; the processor will fall back to a generated id.
        event_id = _peek_event_id(raw)
        if event_id is not None:
            self._on_first_seen(event_id)
        try:
            self._on_message(raw)
            self._consumer.commit(message=msg, asynchronous=False)
        except Exception as ex:
            log.error("Failed to process message at offset %s: %s", msg.offset(), ex)
            # Do NOT commit — the message will be redelivered on next poll/restart.


def _peek_event_id(raw: bytes) -> str | None:
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    candidate = data.get("id")
    if candidate:
        return str(candidate)
    payload = data.get("payload")
    if isinstance(payload, dict) and payload.get("id"):
        return str(payload.get("id"))
    return None
