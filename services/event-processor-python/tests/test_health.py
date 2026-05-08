"""Tests for /health: 200 only when both Kafka and Minio respond."""

from __future__ import annotations

import api as api_module
import health as health_module
import pytest
from fastapi.testclient import TestClient


def _build(monkeypatch, *, kafka_ok=True, minio_ok=True, fake_minio=None, store=None):
    from api import build_app
    from store import EventStore

    if store is None:
        store = EventStore()
    if fake_minio is None:
        from tests.conftest import FakeMinioClient

        fake_minio = FakeMinioClient()
        fake_minio.make_bucket("events")

    monkeypatch.setattr(health_module, "kafka_reachable",
                        lambda *_a, **_kw: (kafka_ok, "ok" if kafka_ok else "fail"))
    monkeypatch.setattr(health_module, "minio_reachable",
                        lambda *_a, **_kw: (minio_ok, "ok" if minio_ok else "fail"))
    # build_app re-imports the symbols at call time, so the monkeypatches above
    # take effect for the lambdas captured inside the route handlers.
    return TestClient(build_app(
        store=store,
        minio_client=fake_minio,
        minio_bucket="events",
        kafka_bootstrap="localhost:9092",
        kafka_health_timeout=1.0,
    ))


def test_health_returns_200_when_both_kafka_and_minio_are_reachable(monkeypatch):
    client = _build(monkeypatch, kafka_ok=True, minio_ok=True)
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "healthy"
    assert body["kafka"]["ok"] is True
    assert body["minio"]["ok"] is True


def test_health_returns_503_when_kafka_is_unreachable(monkeypatch):
    client = _build(monkeypatch, kafka_ok=False, minio_ok=True)
    response = client.get("/health")
    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "unhealthy"
    assert body["kafka"]["ok"] is False
    assert body["minio"]["ok"] is True


def test_health_returns_503_when_minio_is_unreachable(monkeypatch):
    client = _build(monkeypatch, kafka_ok=True, minio_ok=False)
    response = client.get("/health")
    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "unhealthy"
    assert body["kafka"]["ok"] is True
    assert body["minio"]["ok"] is False


def test_health_returns_503_when_both_are_unreachable(monkeypatch):
    client = _build(monkeypatch, kafka_ok=False, minio_ok=False)
    response = client.get("/health")
    assert response.status_code == 503
