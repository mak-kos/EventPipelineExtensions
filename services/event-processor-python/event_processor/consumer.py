"""Kafka consumer with concurrent dispatch, per-key ordering, and clean shutdown.

The consumer thread does three things in each loop tick:

1. Poll Kafka and submit each new message to the worker pool, keyed so that
   any two messages sharing a key land on the same worker (and thus stay in
   FIFO order).
2. Drain the completion queue posted to by workers, advancing the per-partition
   ``OffsetWatermark`` whenever offsets complete contiguously.
3. Periodically commit the watermark async; on shutdown, commit sync.

Backpressure: ``WorkerPool.submit`` blocks when the target worker's queue is
full. While the consumer thread is blocked in ``submit`` it is not polling
Kafka, which throttles the producer side without unbounded memory growth.
"""

from __future__ import annotations

import json
import logging
import queue
import threading
import time
from typing import Callable, Optional

from confluent_kafka import Consumer, KafkaError, TopicPartition

from event_processor.watermark import OffsetWatermark
from event_processor.worker import WorkerPool


log = logging.getLogger(__name__)


class KafkaConsumerLoop:
    def __init__(
        self,
        *,
        bootstrap: str,
        topic: str,
        group_id: str,
        pool: WorkerPool,
        on_message: Callable[[bytes], None],
        on_first_seen: Callable[[str], None],
        poll_timeout: float = 1.0,
        commit_interval_seconds: float = 5.0,
    ) -> None:
        self._bootstrap = bootstrap
        self._topic = topic
        self._group_id = group_id
        self._pool = pool
        self._on_message = on_message
        self._on_first_seen = on_first_seen
        self._poll_timeout = poll_timeout
        self._commit_interval = commit_interval_seconds

        self._stop = threading.Event()
        self._completions: queue.Queue[tuple[tuple[str, int], int, bool]] = queue.Queue()
        self._watermark = OffsetWatermark()
        self._last_committed: dict[tuple[str, int], int] = {}
        self._consumer: Optional[Consumer] = None
        self._exit_code = 0

    @property
    def exit_code(self) -> int:
        return self._exit_code

    def run(self) -> None:
        try:
            self._run_inner()
        except Exception:
            log.exception("KafkaConsumerLoop crashed")
            self._exit_code = 1

    def _run_inner(self) -> None:
        self._consumer = Consumer({
            "bootstrap.servers": self._bootstrap,
            "group.id": self._group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        self._consumer.subscribe([self._topic])
        log.info("Kafka consumer subscribed to '%s' as group '%s'", self._topic, self._group_id)

        last_commit_at = time.monotonic()

        try:
            while not self._stop.is_set():
                msg = self._consumer.poll(timeout=self._poll_timeout)
                if msg is not None:
                    if msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            log.error("Consumer error: %s", msg.error())
                    else:
                        self._dispatch(msg)

                self._drain_completions()
                if time.monotonic() - last_commit_at >= self._commit_interval:
                    self._commit(asynchronous=True)
                    last_commit_at = time.monotonic()
        finally:
            log.info("Consumer loop exiting; final drain + commit")
            # Drain anything that finished while we were stopping.
            self._drain_completions()
            self._commit(asynchronous=False)
            try:
                self._consumer.close()
            except Exception as ex:
                log.warning("Error closing Kafka consumer: %s", ex)

    def stop(self) -> None:
        self._stop.set()

    def _dispatch(self, msg) -> None:
        tp = (msg.topic(), msg.partition())
        offset = msg.offset()
        raw = msg.value()
        key = msg.key()

        # Best-effort id extraction so we can mark "pending" before the worker runs.
        event_id = _peek_event_id(raw)
        if event_id is not None:
            self._on_first_seen(event_id)

        self._watermark.observe(tp, offset)
        # Capture by argument default so the closure binds the *current* values,
        # not whatever they happen to be when the worker thread eventually runs.
        self._pool.submit(
            key,
            work=lambda raw=raw: self._on_message(raw),
            on_done=lambda ok, tp=tp, offset=offset: self._completions.put((tp, offset, ok)),
        )

    def _drain_completions(self) -> None:
        while True:
            try:
                tp, offset, ok = self._completions.get_nowait()
            except queue.Empty:
                return
            if ok:
                self._watermark.complete(tp, offset)
            # On failure we deliberately do NOT advance the watermark: the
            # message stays in the in-flight set, the partition watermark
            # stalls, and the offset is reread on next consumer restart.
            # That preserves at-least-once with the {event_id}.xml idempotency
            # already enforced at the Minio layer.

    def _commit(self, *, asynchronous: bool) -> None:
        offsets_to_commit = []
        for tp, next_offset in self._watermark.snapshot().items():
            if self._last_committed.get(tp) == next_offset - 1:
                continue
            topic, partition = tp
            offsets_to_commit.append(TopicPartition(topic, partition, next_offset))
            self._last_committed[tp] = next_offset - 1
        if not offsets_to_commit:
            return
        try:
            self._consumer.commit(offsets=offsets_to_commit, asynchronous=asynchronous)
            log.debug("Committed offsets: %s", offsets_to_commit)
        except Exception as ex:
            # Log loudly — final commit failures are how we lose progress on a clean shutdown.
            level = log.error if not asynchronous else log.warning
            level("Commit failed (asynchronous=%s): %s", asynchronous, ex)
            if not asynchronous:
                self._exit_code = 1


def _peek_event_id(raw: bytes) -> Optional[str]:
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
