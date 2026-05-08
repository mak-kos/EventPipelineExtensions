"""Key-partitioned worker pool for the event-processor.

Why a custom pool rather than ``concurrent.futures.ThreadPoolExecutor``:
the executor can't guarantee that two messages with the same key go to the
same thread, which is exactly the ordering invariant Kafka asks us to keep
when we parallelise consumption. Fixing that on top of the executor would
need an extra serialisation queue per key — at which point we've rebuilt
this class with more moving parts.

Design:
- ``num_workers`` worker threads, each owning a bounded ``queue.Queue``.
- ``submit(key, work, on_done)`` routes by ``hash(key) % num_workers``, so the
  same key always lands on the same worker → FIFO within a key.
- The queue is **bounded**: when the consumer fills a worker's queue, the
  next ``put`` blocks. That blocks the consumer, which stops polling Kafka,
  which is the backpressure signal we want when Minio is slow.
- ``shutdown(timeout)`` enqueues a sentinel on each worker queue and waits
  for the threads to join. A worker only exits when it has drained its
  queue — in-flight messages are not lost.
- Worker exceptions never propagate out of the worker; they call
  ``on_done(ok=False)`` and continue. The pool keeps running.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from dataclasses import dataclass
from typing import Callable


log = logging.getLogger(__name__)


@dataclass
class _WorkItem:
    work: Callable[[], None]
    on_done: Callable[[bool], None]


_SENTINEL = object()


class WorkerPool:
    def __init__(self, num_workers: int, queue_size: int) -> None:
        if num_workers < 1:
            raise ValueError("num_workers must be >= 1")
        if queue_size < 1:
            raise ValueError("queue_size must be >= 1")
        self._num_workers = num_workers
        self._queues: list[queue.Queue] = [queue.Queue(maxsize=queue_size) for _ in range(num_workers)]
        self._workers: list[threading.Thread] = []
        self._started = False

    def start(self) -> None:
        if self._started:
            raise RuntimeError("WorkerPool already started")
        self._started = True
        for i, q in enumerate(self._queues):
            t = threading.Thread(target=self._loop, args=(i, q),
                                 name=f"event-worker-{i}", daemon=False)
            t.start()
            self._workers.append(t)

    def submit(self, key: bytes | None,
               work: Callable[[], None],
               on_done: Callable[[bool], None]) -> None:
        """Route ``work`` to a worker based on ``key``.

        Blocks if the destination worker's queue is full — that is the
        backpressure path. The consumer thread will stop polling Kafka while
        blocked, which is exactly what we want.
        """
        idx = self._route(key)
        self._queues[idx].put(_WorkItem(work=work, on_done=on_done))

    def shutdown(self, timeout: float) -> bool:
        """Drain every worker queue and join its thread.

        Returns True if all workers stopped within the timeout, False otherwise.
        On False the caller should treat it as an error (exit non-zero).
        """
        for q in self._queues:
            q.put(_SENTINEL)
        deadline = time.monotonic() + max(0.0, timeout)
        for t in self._workers:
            remaining = max(0.0, deadline - time.monotonic())
            t.join(timeout=remaining)
        unfinished = [t for t in self._workers if t.is_alive()]
        if unfinished:
            log.error("WorkerPool shutdown timed out: %d workers still alive", len(unfinished))
            return False
        return True

    def _route(self, key: bytes | None) -> int:
        if key is None or len(key) == 0:
            return 0
        # int.from_bytes is deterministic across runs (unlike Python's str hash
        # which is randomised). Important for "same key -> same worker" across
        # restarts and within a process.
        return int.from_bytes(key, "big") % self._num_workers

    def _loop(self, idx: int, q: queue.Queue) -> None:
        log.info("worker-%d started", idx)
        while True:
            item = q.get()
            if item is _SENTINEL:
                log.info("worker-%d stopping (queue drained)", idx)
                return
            ok = True
            try:
                item.work()
            except Exception:
                log.exception("worker-%d failed processing item", idx)
                ok = False
            finally:
                try:
                    item.on_done(ok)
                except Exception:
                    log.exception("worker-%d on_done callback failed", idx)
                q.task_done()
