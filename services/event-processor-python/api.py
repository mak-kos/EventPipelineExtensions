"""FastAPI app exposing /health and /events/{id}/status.

The app is intentionally constructed by a factory (`build_app`) so tests can
inject mock dependencies without monkey-patching module globals.
"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

import health
from store import EventStore, PENDING, PROCESSED


def build_app(
    *,
    store: EventStore,
    minio_client,
    minio_bucket: str,
    kafka_bootstrap: str,
    kafka_health_timeout: float,
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

    return app
