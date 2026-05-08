"""Thread-safe in-memory tracking of which events the processor has seen.

This is the simplest design that satisfies the README requirement to expose a
status endpoint without introducing a separate persistence dependency. The
trade-off (and one to document in NOTES) is that a process restart loses the
``pending`` view; processed objects can still be discovered via Minio HEAD on
``{event_id}.xml`` if a future caller wants restart-survivability.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


PENDING = "pending"
PROCESSED = "processed"


@dataclass(frozen=True)
class EventRecord:
    event_id: str
    status: str
    object_key: Optional[str] = None
    processed_at: Optional[datetime] = None


class EventStore:
    """Thread-safe map of event_id -> EventRecord."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._records: dict[str, EventRecord] = {}

    def mark_pending(self, event_id: str) -> None:
        with self._lock:
            existing = self._records.get(event_id)
            # Don't downgrade an already-processed entry back to pending.
            if existing is not None and existing.status == PROCESSED:
                return
            self._records[event_id] = EventRecord(event_id=event_id, status=PENDING)

    def mark_processed(self, event_id: str, object_key: str, processed_at: datetime) -> None:
        with self._lock:
            self._records[event_id] = EventRecord(
                event_id=event_id,
                status=PROCESSED,
                object_key=object_key,
                processed_at=processed_at,
            )

    def get(self, event_id: str) -> Optional[EventRecord]:
        with self._lock:
            return self._records.get(event_id)

    def __len__(self) -> int:
        with self._lock:
            return len(self._records)
