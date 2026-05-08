"""Tests for POST /events/{id}/replay end-to-end against the FastAPI app."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from event_processor import api as api_module
from event_processor import health as health_module
from event_processor.api import build_app
from event_processor.event_api_client import EventNotFound, UpstreamError
from event_processor.store import EventStore


class _FakePublisher:
    def __init__(self) -> None:
        self.published: list[tuple[str, bytes]] = []
        self.fail_with: Exception | None = None

    def publish(self, key: str, value: bytes) -> None:
        if self.fail_with is not None:
            raise self.fail_with
        self.published.append((key, value))


@pytest.fixture
def replay_app(monkeypatch, fake_minio):
    monkeypatch.setattr(health_module, "kafka_reachable", lambda *_a, **_kw: (True, "ok"))
    monkeypatch.setattr(health_module, "minio_reachable", lambda *_a, **_kw: (True, "ok"))

    store = EventStore()
    publisher = _FakePublisher()
    client_mock = MagicMock()

    app = build_app(
        store=store,
        minio_client=fake_minio,
        minio_bucket="events",
        kafka_bootstrap="localhost:9092",
        kafka_health_timeout=1.0,
        event_api_client=client_mock,
        publisher=publisher,
    )
    return TestClient(app), store, publisher, client_mock


def test_replay_happy_path_publishes_to_kafka_and_returns_202(replay_app):
    client, store, publisher, client_mock = replay_app
    client_mock.get_event.return_value = {"id": "abc", "payload": {"type": "user.signup"}}

    response = client.post("/events/abc/replay")

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "accepted"
    assert body["eventId"] == "abc"

    # event-api was queried for this id.
    client_mock.get_event.assert_called_once_with("abc")

    # Exactly one Kafka message was published, keyed by event id, with the
    # standard envelope shape.
    assert len(publisher.published) == 1
    key, value = publisher.published[0]
    assert key == "abc"
    envelope = json.loads(value.decode("utf-8"))
    assert envelope["id"] == "abc"
    assert envelope["payload"] == {"type": "user.signup"}

    # Store reflects pending state after replay was scheduled.
    assert store.get("abc").status == "pending"


def test_replay_missing_event_returns_404(replay_app):
    client, _store, publisher, client_mock = replay_app
    client_mock.get_event.side_effect = EventNotFound("nope")

    response = client.post("/events/missing/replay")

    assert response.status_code == 404
    assert publisher.published == []


def test_replay_event_api_unreachable_returns_502(replay_app):
    client, _store, publisher, client_mock = replay_app
    client_mock.get_event.side_effect = UpstreamError("network")

    response = client.post("/events/abc/replay")

    assert response.status_code == 502
    assert publisher.published == []


def test_replay_kafka_publish_failure_returns_502(replay_app):
    client, store, publisher, client_mock = replay_app
    client_mock.get_event.return_value = {"id": "abc", "payload": {}}
    publisher.fail_with = TimeoutError("kafka stalled")

    response = client.post("/events/abc/replay")

    assert response.status_code == 502
    # Important: we did NOT mark pending if the Kafka publish failed, otherwise
    # /status would lie about the state of an event that nobody is processing.
    assert store.get("abc") is None


def test_replay_uses_event_id_as_kafka_key_so_minio_object_overwrites(replay_app):
    """Two replays for the same event_id must publish under the same Kafka key,
    so the consumer (which writes to {event_id}.xml) overwrites instead of
    creating duplicates."""
    client, _store, publisher, client_mock = replay_app
    client_mock.get_event.return_value = {"id": "abc", "payload": {"v": 1}}

    client.post("/events/abc/replay")
    client.post("/events/abc/replay")

    assert len(publisher.published) == 2
    keys = {k for k, _v in publisher.published}
    assert keys == {"abc"}  # both published under same key
