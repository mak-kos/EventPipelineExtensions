"""Tests for EventApiClient: token cache, 404, 401-refresh-and-retry, transport errors."""

from __future__ import annotations

import httpx
import pytest

from event_api_client import (
    AuthError,
    EventApiClient,
    EventNotFound,
    UpstreamError,
)


def _client(*, transport: httpx.MockTransport) -> EventApiClient:
    httpx_client = httpx.Client(transport=transport, base_url="http://event-api")
    return EventApiClient(
        base_url="http://event-api",
        username="user",
        password="user123",
        connect_timeout=1.0,
        read_timeout=1.0,
        client=httpx_client,
    )


def test_get_event_logs_in_then_returns_payload():
    calls: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        if request.url.path == "/auth/login":
            return httpx.Response(200, json={"token": "tok-123", "expiresAt": "2099-01-01T00:00:00Z"})
        if request.url.path == "/events/abc":
            assert request.headers["Authorization"] == "Bearer tok-123"
            return httpx.Response(200, json={"id": "abc", "payload": {"type": "x"}})
        return httpx.Response(404)

    client = _client(transport=httpx.MockTransport(handler))
    event = client.get_event("abc")

    assert event["id"] == "abc"
    assert event["payload"] == {"type": "x"}
    assert calls == [("POST", "/auth/login"), ("GET", "/events/abc")]


def test_token_is_cached_across_requests():
    login_calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/auth/login":
            login_calls["n"] += 1
            return httpx.Response(200, json={"token": "tok"})
        return httpx.Response(200, json={"id": request.url.path.split("/")[-1], "payload": {}})

    client = _client(transport=httpx.MockTransport(handler))
    client.get_event("a")
    client.get_event("b")
    client.get_event("c")

    assert login_calls["n"] == 1


def test_404_from_event_api_raises_event_not_found():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/auth/login":
            return httpx.Response(200, json={"token": "tok"})
        return httpx.Response(404)

    client = _client(transport=httpx.MockTransport(handler))
    with pytest.raises(EventNotFound):
        client.get_event("missing")


def test_401_triggers_one_relogin_and_retry():
    state = {"login_calls": 0, "get_calls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/auth/login":
            state["login_calls"] += 1
            return httpx.Response(200, json={"token": f"tok-{state['login_calls']}"})
        if request.url.path == "/events/abc":
            state["get_calls"] += 1
            if state["get_calls"] == 1:
                return httpx.Response(401)
            assert request.headers["Authorization"] == "Bearer tok-2"
            return httpx.Response(200, json={"id": "abc", "payload": {}})
        return httpx.Response(404)

    client = _client(transport=httpx.MockTransport(handler))
    event = client.get_event("abc")

    assert event["id"] == "abc"
    assert state["login_calls"] == 2  # original login + refresh
    assert state["get_calls"] == 2  # 401 then 200


def test_persistent_401_raises_auth_error():
    state = {"login_calls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/auth/login":
            state["login_calls"] += 1
            return httpx.Response(200, json={"token": f"tok-{state['login_calls']}"})
        return httpx.Response(401)

    client = _client(transport=httpx.MockTransport(handler))
    with pytest.raises(AuthError):
        client.get_event("abc")


def test_login_with_bad_credentials_raises_auth_error():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/auth/login":
            return httpx.Response(401)
        return httpx.Response(500)

    client = _client(transport=httpx.MockTransport(handler))
    with pytest.raises(AuthError):
        client.get_event("abc")


def test_5xx_from_event_api_raises_upstream_error():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/auth/login":
            return httpx.Response(200, json={"token": "tok"})
        return httpx.Response(503)

    client = _client(transport=httpx.MockTransport(handler))
    with pytest.raises(UpstreamError):
        client.get_event("abc")
