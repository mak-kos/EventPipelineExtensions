"""event-processor entrypoint.

Wires up the Kafka consumer (in a daemon thread) and the FastAPI HTTP server
(uvicorn on the main thread). Task 6 replaces the daemon thread with a proper
worker pool plus graceful shutdown.
"""

from __future__ import annotations

import logging
import threading

import urllib3
import uvicorn
from minio import Minio

import config
from api import build_app
from consumer import KafkaConsumerLoop
from processor import MessageProcessor
from store import EventStore


# Bound Minio HTTP client so a stalled connection cannot wedge /health or /replay.
# Connect: 5s. Read: 10s. Bounded retries with backoff.
_MINIO_HTTP_CLIENT = urllib3.PoolManager(
    timeout=urllib3.Timeout(connect=5.0, read=10.0),
    retries=urllib3.Retry(total=2, backoff_factor=0.5,
                          status_forcelist=[502, 503, 504]),
)


def _configure_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def _ensure_bucket(minio_client: Minio, bucket: str) -> None:
    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)
        logging.getLogger(__name__).info("Created Minio bucket %s", bucket)


def main() -> None:
    _configure_logging()
    log = logging.getLogger(__name__)

    store = EventStore()
    minio_client = Minio(
        config.MINIO_ENDPOINT,
        access_key=config.MINIO_ACCESS_KEY,
        secret_key=config.MINIO_SECRET_KEY,
        secure=config.MINIO_SECURE,
        http_client=_MINIO_HTTP_CLIENT,
    )
    _ensure_bucket(minio_client, config.MINIO_BUCKET)

    processor = MessageProcessor(minio_client, config.MINIO_BUCKET, store)

    loop = KafkaConsumerLoop(
        bootstrap=config.KAFKA_BOOTSTRAP,
        topic=config.KAFKA_TOPIC,
        group_id=config.KAFKA_GROUP_ID,
        on_message=processor.process,
        on_first_seen=store.mark_pending,
    )
    consumer_thread = threading.Thread(target=loop.run, name="kafka-consumer", daemon=True)
    consumer_thread.start()

    app = build_app(
        store=store,
        minio_client=minio_client,
        minio_bucket=config.MINIO_BUCKET,
        kafka_bootstrap=config.KAFKA_BOOTSTRAP,
        kafka_health_timeout=config.KAFKA_HEALTH_TIMEOUT,
    )

    log.info("Starting HTTP server on %s:%s", config.HTTP_HOST, config.HTTP_PORT)
    uvicorn.run(app, host=config.HTTP_HOST, port=config.HTTP_PORT, log_level=config.LOG_LEVEL.lower())


if __name__ == "__main__":
    main()
