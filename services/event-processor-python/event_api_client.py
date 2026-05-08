"""HTTP client for event-api with token caching and 401-driven refresh.

Used by /replay to re-fetch an event before re-publishing it to Kafka. Failure
modes are explicit so the replay endpoint can map them to clean HTTP responses:

  * EventNotFound: event-api returned 404
  * AuthError:     event-api rejected our credentials (5xx-on-our-side)
  * UpstreamError: any other transport / 5xx
"""

from __future__ import annotations

import logging
import threading
from typing import Optional

import httpx


log = logging.getLogger(__name__)


class EventApiClientError(Exception):
    pass


class EventNotFound(EventApiClientError):
    pass


class AuthError(EventApiClientError):
    pass


class UpstreamError(EventApiClientError):
    pass


class EventApiClient:
    """Thread-safe client that lazily logs in and caches the token.

    The cached token is refreshed on demand: a 401 from event-api triggers a
    single re-login + retry. We don't track ``expiresAt`` proactively because
    clocks drift; the 401 path is always the source of truth.
    """

    def __init__(
        self,
        *,
        base_url: str,
        username: str,
        password: str,
        connect_timeout: float,
        read_timeout: float,
        client: Optional[httpx.Client] = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._username = username
        self._password = password
        self._timeout = httpx.Timeout(connect=connect_timeout, read=read_timeout,
                                      write=read_timeout, pool=connect_timeout)
        self._client = client or httpx.Client(timeout=self._timeout)
        self._token: Optional[str] = None
        self._token_lock = threading.Lock()

    def get_event(self, event_id: str) -> dict:
        """Fetch the event payload from event-api.

        Returns the parsed JSON body. Raises EventNotFound on 404, AuthError
        on persistent 401, or UpstreamError on any other failure.
        """
        token = self._token_or_login()
        response = self._do_get(event_id, token)
        if response.status_code == 401:
            log.info("event-api returned 401; refreshing token and retrying once")
            token = self._login_and_cache()
            response = self._do_get(event_id, token)

        if response.status_code == 404:
            raise EventNotFound(f"Event {event_id} not found in event-api")
        if response.status_code == 401:
            raise AuthError("event-api rejected credentials after token refresh")
        if response.status_code >= 400:
            raise UpstreamError(
                f"event-api returned {response.status_code} for event {event_id}"
            )
        return response.json()

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:  # don't shadow the original error during shutdown
            log.warning("Error closing event-api HTTP client", exc_info=True)

    # ----- internals -----

    def _token_or_login(self) -> str:
        with self._token_lock:
            if self._token is not None:
                return self._token
        return self._login_and_cache()

    def _login_and_cache(self) -> str:
        url = f"{self._base_url}/auth/login"
        try:
            response = self._client.post(
                url,
                json={"username": self._username, "password": self._password},
            )
        except httpx.HTTPError as ex:
            raise UpstreamError(f"event-api login transport error: {ex}") from ex

        if response.status_code == 401:
            raise AuthError("event-api login: invalid credentials")
        if response.status_code >= 400:
            raise UpstreamError(f"event-api login returned {response.status_code}")

        body = response.json()
        token = body.get("token")
        if not token:
            raise UpstreamError("event-api login did not return a token")
        with self._token_lock:
            self._token = token
        return token

    def _do_get(self, event_id: str, token: str) -> httpx.Response:
        url = f"{self._base_url}/events/{event_id}"
        try:
            return self._client.get(url, headers={"Authorization": f"Bearer {token}"})
        except httpx.HTTPError as ex:
            raise UpstreamError(f"event-api GET transport error: {ex}") from ex
