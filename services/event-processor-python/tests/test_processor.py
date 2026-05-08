"""Tests for MessageProcessor: id-based key, store update, generated-id fallback."""

from __future__ import annotations

import json

from processor import MessageProcessor
from store import EventStore, PROCESSED


def _envelope(event_id: str, payload: dict | None = None) -> bytes:
    body = {"id": event_id, "payload": payload or {}}
    return json.dumps(body).encode("utf-8")


def test_process_writes_to_minio_under_event_id_key(fake_minio):
    store = EventStore()
    proc = MessageProcessor(fake_minio, "events", store)

    key = proc.process(_envelope("evt-1", {"type": "user.signup"}))

    assert key == "evt-1.xml"
    assert ("events", "evt-1.xml") in fake_minio.objects
    xml_bytes = fake_minio.objects[("events", "evt-1.xml")]
    assert b"<event>" in xml_bytes
    assert b"user.signup" in xml_bytes


def test_process_marks_event_as_processed_in_store(fake_minio):
    store = EventStore()
    proc = MessageProcessor(fake_minio, "events", store)

    proc.process(_envelope("evt-2"))

    record = store.get("evt-2")
    assert record is not None
    assert record.status == PROCESSED
    assert record.object_key == "evt-2.xml"
    assert record.processed_at is not None


def test_process_overwrites_same_key_on_replay(fake_minio):
    store = EventStore()
    proc = MessageProcessor(fake_minio, "events", store)

    proc.process(_envelope("evt-3", {"v": 1}))
    proc.process(_envelope("evt-3", {"v": 2}))

    # Only one object key; the second put overwrote the first.
    keys_for_event = [k for k in fake_minio.objects if k[1] == "evt-3.xml"]
    assert len(keys_for_event) == 1
    xml_bytes = fake_minio.objects[("events", "evt-3.xml")]
    assert b"<v>2</v>" in xml_bytes


def test_process_falls_back_to_generated_id_when_envelope_missing_id(fake_minio):
    store = EventStore()
    proc = MessageProcessor(fake_minio, "events", store)

    # No id field anywhere — the processor must not crash.
    key = proc.process(b'{"type":"raw"}')

    assert key.endswith(".xml")
    assert ("events", key) in fake_minio.objects
