"""FastAPI app exposing /health and /events/{id}/status.

The app is intentionally constructed by a factory (`build_app`) so tests can
inject mock dependencies without monkey-patching module globals.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from event_processor import health
from event_processor.event_api_client import (
    AuthError,
    EventApiClient,
    EventNotFound,
    UpstreamError,
)
from event_processor.kafka_producer import KafkaPublisher
from event_processor.store import EventStore, PENDING, PROCESSED


log = logging.getLogger(__name__)


def build_app(
    *,
    store: EventStore,
    minio_client,
    minio_bucket: str,
    kafka_bootstrap: str,
    kafka_health_timeout: float,
    event_api_client: Optional[EventApiClient] = None,
    publisher: Optional[KafkaPublisher] = None,
) -> FastAPI:
    app = FastAPI(title="event-processor")

    @app.get("/health")
    def health_endpoint():
        # Module-attribute access (rather than `from health import ...`) so tests can
        # monkeypatch the reachability checks without rebuilding the app.
        kafka_ok, kafka_detail = health.kafka_reachable(kafka_bootstrap, kafka_health_timeout)
        minio_ok, minio_detail = health.minio_reachable(minio_client, minio_bucket)

        body = {
            "status": "healthy" if (kafka_ok and minio_ok) else "unhealthy",
            "kafka": {"ok": kafka_ok, "detail": kafka_detail},
            "minio": {"ok": minio_ok, "detail": minio_detail},
        }
        if kafka_ok and minio_ok:
            return body
        return JSONResponse(status_code=503, content=body)

    @app.get("/events/{event_id}/status")
    def event_status(event_id: str):
        record = store.get(event_id)
        if record is None:
            raise HTTPException(status_code=404, detail="Event not seen by processor")
        if record.status == PROCESSED:
            return {
                "status": PROCESSED,
                "objectKey": record.object_key,
                "processedAt": record.processed_at.isoformat() if record.processed_at else None,
            }
        return {"status": PENDING}

    @app.post("/events/{event_id}/replay", status_code=202)
    def replay(event_id: str):
        if event_api_client is None or publisher is None:
            # Replay was wired off (e.g. credentials missing). Make the contract explicit.
            raise HTTPException(status_code=503, detail="Replay is not configured on this instance")

        try:
            event = event_api_client.get_event(event_id)
        except EventNotFound:
            raise HTTPException(status_code=404, detail=f"Event {event_id} not found in event-api")
        except AuthError as ex:
            log.error("event-api auth failure during replay: %s", ex)
            raise HTTPException(status_code=502,
                                detail="event-api auth failure; replay cannot proceed")
        except UpstreamError as ex:
            log.error("event-api upstream failure during replay: %s", ex)
            raise HTTPException(status_code=502, detail="event-api unavailable")

        # Wrap the response in the same envelope event-api originally produced
        # so the consumer does not need replay-specific code paths. The consumer
        # will overwrite the existing {event_id}.xml object on Minio, so no
        # duplicate object is created (idempotency by key).
        envelope = json.dumps({"id": event_id, "payload": event.get("payload")}).encode("utf-8")

        try:
            publisher.publish(event_id, envelope)
        except Exception as ex:
            log.error("Kafka publish failed during replay of %s: %s", event_id, ex)
            raise HTTPException(status_code=502, detail="Failed to enqueue replay onto Kafka")

        store.mark_pending(event_id)
        log.info("Replay scheduled for event %s", event_id)
        return {"status": "accepted", "eventId": event_id, "message": "Event re-published to Kafka"}

    return app
