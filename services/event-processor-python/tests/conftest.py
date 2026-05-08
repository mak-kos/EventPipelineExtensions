"""Shared fixtures: a dummy Minio client and a FastAPI test client backed by mocks."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from io import BytesIO
from typing import Optional

import pytest
from fastapi.testclient import TestClient


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


class FakeMinioClient:
    """Minimal in-memory stand-in for the Minio SDK surface we touch.

    Tests can flip ``raise_on_bucket_exists`` to simulate Minio outages.
    """

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.buckets: set[str] = set()
        self.raise_on_bucket_exists: Optional[Exception] = None

    def bucket_exists(self, bucket: str) -> bool:
        if self.raise_on_bucket_exists is not None:
            raise self.raise_on_bucket_exists
        return bucket in self.buckets

    def make_bucket(self, bucket: str) -> None:
        self.buckets.add(bucket)

    def put_object(self, bucket: str, name: str, data, length: int) -> None:
        if isinstance(data, BytesIO):
            payload = data.read()
        else:
            payload = bytes(data)
        self.objects[(bucket, name)] = payload


@pytest.fixture
def fake_minio() -> FakeMinioClient:
    client = FakeMinioClient()
    client.make_bucket("events")
    return client


@pytest.fixture
def store():
    from event_processor.store import EventStore

    return EventStore()


@pytest.fixture
def fixed_now() -> datetime:
    return datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def app(monkeypatch, fake_minio, store):
    """Builds the FastAPI app with kafka_reachable / minio_reachable monkey-patched."""
    from event_processor.api import build_app
    from event_processor import health

    monkeypatch.setattr(health, "kafka_reachable", lambda *_a, **_kw: (True, "ok"))
    monkeypatch.setattr(health, "minio_reachable", lambda *_a, **_kw: (True, "ok"))

    return build_app(
        store=store,
        minio_client=fake_minio,
        minio_bucket="events",
        kafka_bootstrap="localhost:9092",
        kafka_health_timeout=1.0,
    )


@pytest.fixture
def client(app) -> TestClient:
    return TestClient(app)
