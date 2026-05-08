"""event-processor entrypoint.

Wires up:
  * EventStore (in-memory tracking for /events/{id}/status)
  * Minio client (with bounded HTTP timeouts)
  * MessageProcessor (JSON -> XML -> Minio under {event_id}.xml)
  * WorkerPool (key-partitioned, bounded queues for backpressure)
  * KafkaConsumerLoop (per-partition watermark commit, manual offset commits)
  * FastAPI app served by uvicorn

Shutdown sequence on SIGTERM/SIGINT:
  1. Consumer stops polling and commits final offsets (sync).
  2. Pool drains every worker's queue.
  3. uvicorn exits.
  4. Process exits 0 if all phases succeeded, non-zero on any error.
"""

from __future__ import annotations

import logging
import sys
import threading

import urllib3
import uvicorn
from minio import Minio

import config
from api import build_app
from consumer import KafkaConsumerLoop
from processor import MessageProcessor
from store import EventStore
from worker import WorkerPool


# Bound Minio HTTP client so a stalled connection cannot wedge /health or /replay.
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


def main() -> int:
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
    pool = WorkerPool(config.WORKER_COUNT, config.WORKER_QUEUE_SIZE)
    pool.start()

    consumer = KafkaConsumerLoop(
        bootstrap=config.KAFKA_BOOTSTRAP,
        topic=config.KAFKA_TOPIC,
        group_id=config.KAFKA_GROUP_ID,
        pool=pool,
        on_message=processor.process,
        on_first_seen=store.mark_pending,
    )
    consumer_thread = threading.Thread(target=consumer.run, name="kafka-consumer", daemon=False)
    consumer_thread.start()

    app = build_app(
        store=store,
        minio_client=minio_client,
        minio_bucket=config.MINIO_BUCKET,
        kafka_bootstrap=config.KAFKA_BOOTSTRAP,
        kafka_health_timeout=config.KAFKA_HEALTH_TIMEOUT,
    )

    uvicorn_config = uvicorn.Config(
        app,
        host=config.HTTP_HOST,
        port=config.HTTP_PORT,
        log_level=config.LOG_LEVEL.lower(),
    )
    server = uvicorn.Server(uvicorn_config)

    exit_code = 0

    log.info("Starting HTTP server on %s:%s (workers=%d, queue=%d)",
             config.HTTP_HOST, config.HTTP_PORT,
             config.WORKER_COUNT, config.WORKER_QUEUE_SIZE)

    # uvicorn.Server.run() owns SIGTERM/SIGINT handling: on signal it sets
    # should_exit and returns. Anything our process needs to do beyond stopping
    # the HTTP server happens AFTER run() returns — that's the deterministic
    # graceful-shutdown path.
    try:
        server.run()
    except Exception:
        log.exception("HTTP server crashed")
        exit_code = 1

    log.info("HTTP server stopped; tearing down Kafka consumer and worker pool")

    # 1. Stop accepting new Kafka work; final commit happens inside the loop.
    consumer.stop()
    consumer_thread.join(timeout=config.SHUTDOWN_TIMEOUT_SECONDS)
    if consumer_thread.is_alive():
        log.error("Consumer thread did not stop within %.1fs", config.SHUTDOWN_TIMEOUT_SECONDS)
        exit_code = 1
    if consumer.exit_code != 0:
        exit_code = consumer.exit_code

    # 2. Drain pool — workers finish in-flight messages; sentinel-based stop.
    if not pool.shutdown(timeout=config.SHUTDOWN_TIMEOUT_SECONDS):
        exit_code = 1

    log.info("event-processor exiting with code %d", exit_code)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
