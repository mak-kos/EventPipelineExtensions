"""JSON-to-XML transformation and Minio storage.

The Minio object key is ``{event_id}.xml``: this binds an event id to its
stored object so the status endpoint can answer questions about it, and so
Task 5's replay path is naturally idempotent (a second put for the same
event_id overwrites the same key, instead of producing a duplicate object).
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from io import BytesIO
from typing import Optional

import xmltodict

from store import EventStore


log = logging.getLogger(__name__)


class MessageProcessor:
    def __init__(self, minio_client, bucket: str, store: EventStore) -> None:
        self._minio = minio_client
        self._bucket = bucket
        self._store = store

    def process(self, raw_value: bytes) -> str:
        """Transform a Kafka message body into XML and store it in Minio.

        Returns the object key that was written. Raises on any error so the
        caller can decide whether to commit the offset or retry.
        """
        data = json.loads(raw_value.decode("utf-8"))
        event_id = self._extract_event_id(data)

        xml = xmltodict.unparse({"event": data}, pretty=True)
        body = xml.encode("utf-8")
        object_key = f"{event_id}.xml"

        self._minio.put_object(self._bucket, object_key, BytesIO(body), length=len(body))

        self._store.mark_processed(event_id, object_key, datetime.now(timezone.utc))
        log.info("Processed event %s -> %s/%s", event_id, self._bucket, object_key)
        return object_key

    @staticmethod
    def _extract_event_id(data: dict) -> str:
        """Get the event id from the message envelope.

        event-api wraps its outgoing message as ``{"id": "...", "payload": {...}}``;
        if the message is malformed we fall back to a generated UUID so the
        processor stays unblocked. That fallback breaks the id->object link for
        that one message, which is logged.
        """
        candidate: Optional[str] = None
        if isinstance(data, dict):
            candidate = data.get("id")
            if not candidate:
                payload = data.get("payload")
                if isinstance(payload, dict):
                    candidate = payload.get("id")
        if not candidate:
            generated = str(uuid.uuid4())
            log.warning(
                "Kafka message lacks an id field; storing under generated key %s", generated
            )
            return generated
        return str(candidate)
