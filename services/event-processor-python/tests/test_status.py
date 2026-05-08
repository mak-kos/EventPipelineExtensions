"""Tests for /events/{id}/status: 404 for unseen, pending for seen, processed for stored."""

from __future__ import annotations

from datetime import datetime, timezone


def test_status_returns_404_for_unseen_event(client):
    response = client.get("/events/unknown-id/status")
    assert response.status_code == 404


def test_status_returns_pending_for_seen_but_not_stored(client, store):
    store.mark_pending("evt-1")
    response = client.get("/events/evt-1/status")
    assert response.status_code == 200
    assert response.json() == {"status": "pending"}


def test_status_returns_processed_with_object_key_and_timestamp(client, store):
    processed_at = datetime(2026, 1, 15, 12, 30, 0, tzinfo=timezone.utc)
    store.mark_processed("evt-2", "evt-2.xml", processed_at)

    response = client.get("/events/evt-2/status")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "processed"
    assert body["objectKey"] == "evt-2.xml"
    assert body["processedAt"] == processed_at.isoformat()


def test_pending_then_processed_transitions(client, store):
    store.mark_pending("evt-3")
    assert client.get("/events/evt-3/status").json() == {"status": "pending"}

    store.mark_processed("evt-3", "evt-3.xml", datetime(2026, 1, 15, 13, 0, 0, tzinfo=timezone.utc))
    body = client.get("/events/evt-3/status").json()
    assert body["status"] == "processed"
    assert body["objectKey"] == "evt-3.xml"


def test_processed_is_not_downgraded_by_subsequent_pending(client, store):
    processed_at = datetime(2026, 1, 15, 12, 30, 0, tzinfo=timezone.utc)
    store.mark_processed("evt-4", "evt-4.xml", processed_at)
    store.mark_pending("evt-4")  # late-arriving pending signal must not regress state

    body = client.get("/events/evt-4/status").json()
    assert body["status"] == "processed"
    assert body["objectKey"] == "evt-4.xml"
