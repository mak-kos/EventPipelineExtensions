"""
event-processor (Python)

Consumes events from a Kafka topic, transforms each JSON payload into XML,
and stores the result in a Minio bucket.
"""

import json
import sys
import uuid
from io import BytesIO

import xmltodict
from confluent_kafka import Consumer
from minio import Minio

KAFKA_BOOTSTRAP = "localhost:9092"
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

TOPIC = "events"
BUCKET = "events"
GROUP_ID = "event-processor-group"


def ensure_bucket(client):
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
        print(f'Created bucket "{BUCKET}"', flush=True)


def main():
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )
    ensure_bucket(minio_client)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}", file=sys.stderr, flush=True)
            continue

        data = json.loads(msg.value().decode("utf-8"))
        xml = xmltodict.unparse({"event": data}, pretty=True)

        filename = f"{uuid.uuid4()}.xml"
        body = xml.encode("utf-8")
        minio_client.put_object(BUCKET, filename, BytesIO(body), length=len(body))

        print(f"Saved {filename}", flush=True)


if __name__ == "__main__":
    main()
