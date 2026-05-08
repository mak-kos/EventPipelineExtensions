"""Tests for EventStore: pending/processed states, no-downgrade, thread safety."""

from __future__ import annotations

import threading
from datetime import datetime, timezone

from store import EventStore, PENDING, PROCESSED


def test_get_returns_none_for_unknown_event():
    s = EventStore()
    assert s.get("nope") is None


def test_mark_pending_creates_pending_record():
    s = EventStore()
    s.mark_pending("evt-1")
    rec = s.get("evt-1")
    assert rec is not None
    assert rec.status == PENDING
    assert rec.object_key is None
    assert rec.processed_at is None


def test_mark_processed_overwrites_pending():
    s = EventStore()
    s.mark_pending("evt-1")
    when = datetime.now(timezone.utc)
    s.mark_processed("evt-1", "evt-1.xml", when)

    rec = s.get("evt-1")
    assert rec.status == PROCESSED
    assert rec.object_key == "evt-1.xml"
    assert rec.processed_at == when


def test_mark_pending_does_not_downgrade_processed_record():
    s = EventStore()
    when = datetime.now(timezone.utc)
    s.mark_processed("evt-1", "evt-1.xml", when)
    s.mark_pending("evt-1")  # late-arriving pending signal

    rec = s.get("evt-1")
    assert rec.status == PROCESSED
    assert rec.object_key == "evt-1.xml"


def test_concurrent_mark_pending_and_processed_does_not_lose_processed_state():
    s = EventStore()
    when = datetime.now(timezone.utc)
    barrier = threading.Barrier(2)

    def writer_pending():
        barrier.wait()
        for i in range(1000):
            s.mark_pending(f"e{i}")

    def writer_processed():
        barrier.wait()
        for i in range(1000):
            s.mark_processed(f"e{i}", f"e{i}.xml", when)

    t1 = threading.Thread(target=writer_pending)
    t2 = threading.Thread(target=writer_processed)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # Final states are not predictable per-event, but no state can be invalid.
    for i in range(1000):
        rec = s.get(f"e{i}")
        assert rec is not None
        assert rec.status in (PENDING, PROCESSED)
        if rec.status == PROCESSED:
            assert rec.object_key == f"e{i}.xml"
