"""Tests for WorkerPool: key routing, FIFO per worker, backpressure, isolation."""

from __future__ import annotations

import threading
import time

import pytest

from worker import WorkerPool


def test_same_key_always_lands_on_same_worker():
    pool = WorkerPool(num_workers=4, queue_size=100)
    pool.start()
    try:
        threads_seen: dict[bytes, set[str]] = {}
        lock = threading.Lock()

        def record(key):
            tname = threading.current_thread().name
            with lock:
                threads_seen.setdefault(key, set()).add(tname)

        keys = [b"key-a", b"key-b", b"key-c"]
        completed = threading.Event()
        remaining = {"n": 30}

        def on_done(_ok):
            with lock:
                remaining["n"] -= 1
                if remaining["n"] == 0:
                    completed.set()

        for i in range(30):
            k = keys[i % 3]
            pool.submit(k, work=lambda key=k: record(key), on_done=on_done)

        assert completed.wait(timeout=5)
        # Each key should have been seen on exactly ONE worker thread.
        for k, threads in threads_seen.items():
            assert len(threads) == 1, f"key {k!r} ran on {threads}"
    finally:
        pool.shutdown(timeout=5)


def test_different_keys_can_land_on_different_workers():
    pool = WorkerPool(num_workers=4, queue_size=100)
    pool.start()
    try:
        threads_seen: set[str] = set()
        lock = threading.Lock()
        done = threading.Event()
        remaining = {"n": 16}

        def record():
            with lock:
                threads_seen.add(threading.current_thread().name)

        def on_done(_ok):
            with lock:
                remaining["n"] -= 1
                if remaining["n"] == 0:
                    done.set()

        for i in range(16):
            pool.submit(f"key-{i}".encode(), work=record, on_done=on_done)
        assert done.wait(timeout=5)

        assert len(threads_seen) > 1, "expected multiple workers; got: " + repr(threads_seen)
    finally:
        pool.shutdown(timeout=5)


def test_per_key_messages_processed_in_fifo_order():
    pool = WorkerPool(num_workers=2, queue_size=100)
    pool.start()
    try:
        order: list[int] = []
        lock = threading.Lock()
        done = threading.Event()
        remaining = {"n": 20}

        def record(i):
            with lock:
                order.append(i)

        def on_done(_ok):
            with lock:
                remaining["n"] -= 1
                if remaining["n"] == 0:
                    done.set()

        for i in range(20):
            pool.submit(b"sticky-key", work=lambda i=i: record(i), on_done=on_done)

        assert done.wait(timeout=5)
        assert order == list(range(20))
    finally:
        pool.shutdown(timeout=5)


def test_backpressure_blocks_submit_when_queue_full():
    """A bounded queue + slow worker should block submit so the consumer can't outrun
    the worker. We model "slow worker" by holding the worker until we release it."""
    pool = WorkerPool(num_workers=1, queue_size=2)
    pool.start()
    try:
        proceed = threading.Event()
        finished = threading.Event()

        def slow():
            assert proceed.wait(timeout=5)

        def on_done(_ok):
            finished.set()

        # First submit goes to the worker (which now blocks on `proceed`).
        pool.submit(b"k", work=slow, on_done=on_done)
        # Two more saturate the queue.
        pool.submit(b"k", work=lambda: None, on_done=lambda _: None)
        pool.submit(b"k", work=lambda: None, on_done=lambda _: None)

        # The next submit MUST block — try with a short timeout in a side thread.
        submit_returned = threading.Event()

        def try_submit():
            pool.submit(b"k", work=lambda: None, on_done=lambda _: None)
            submit_returned.set()

        t = threading.Thread(target=try_submit)
        t.start()
        # If backpressure works, submit_returned is NOT set.
        assert not submit_returned.wait(timeout=0.5)

        # Release the slow worker — the queue drains and the blocked submit returns.
        proceed.set()
        assert submit_returned.wait(timeout=5)
        t.join()
    finally:
        pool.shutdown(timeout=5)


def test_worker_exception_does_not_kill_pool_and_signals_failure():
    pool = WorkerPool(num_workers=1, queue_size=4)
    pool.start()
    try:
        outcomes: list[bool] = []
        done = threading.Event()
        remaining = {"n": 3}
        lock = threading.Lock()

        def fail():
            raise RuntimeError("boom")

        def succeed():
            return None

        def on_done(ok):
            with lock:
                outcomes.append(ok)
                remaining["n"] -= 1
                if remaining["n"] == 0:
                    done.set()

        pool.submit(b"k", work=succeed, on_done=on_done)
        pool.submit(b"k", work=fail, on_done=on_done)
        pool.submit(b"k", work=succeed, on_done=on_done)

        assert done.wait(timeout=5)
        assert outcomes == [True, False, True]
    finally:
        pool.shutdown(timeout=5)


def test_shutdown_drains_in_flight_messages():
    pool = WorkerPool(num_workers=2, queue_size=10)
    pool.start()
    counted: list[int] = []
    lock = threading.Lock()

    def slow_count(i):
        time.sleep(0.05)
        with lock:
            counted.append(i)

    for i in range(8):
        pool.submit(b"k", work=lambda i=i: slow_count(i), on_done=lambda _: None)

    # Shutdown should wait for all 8 to finish.
    assert pool.shutdown(timeout=5)
    assert sorted(counted) == list(range(8))


def test_constructor_validates_inputs():
    with pytest.raises(ValueError):
        WorkerPool(num_workers=0, queue_size=10)
    with pytest.raises(ValueError):
        WorkerPool(num_workers=2, queue_size=0)
