"""Per-partition offset watermark for out-of-order completions.

Workers can finish messages in a different order than the consumer read them
(message at offset 5 may finish before the message at offset 3 if 5 routed
to a fast worker). We must commit ``next_offset`` such that every offset
< next_offset has truly completed — otherwise a crash + restart would lose
work. This class tracks per-partition state and advances the watermark only
when offsets complete contiguously.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Hashable, Optional


@dataclass
class _PartitionState:
    next_to_commit: int
    pending_completed: set[int]


class OffsetWatermark:
    """Thread-safe tracker of (topic, partition) -> next-offset-to-commit.

    Methods are concurrent-safe; expected pattern is a single producer
    (consumer thread calling :py:meth:`observe`) and N consumers (workers
    calling :py:meth:`complete`).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state: dict[Hashable, _PartitionState] = {}

    def observe(self, tp: Hashable, offset: int) -> None:
        """Record that the consumer has read this offset (it's now in flight)."""
        with self._lock:
            existing = self._state.get(tp)
            if existing is None:
                self._state[tp] = _PartitionState(next_to_commit=offset,
                                                  pending_completed=set())

    def complete(self, tp: Hashable, offset: int) -> None:
        """Record that a worker has successfully processed this offset."""
        with self._lock:
            state = self._state.get(tp)
            if state is None:
                # Defensive: completion before observe shouldn't happen, but if
                # it does, treat the completed offset as the new starting point.
                self._state[tp] = _PartitionState(next_to_commit=offset + 1,
                                                  pending_completed=set())
                return
            if offset < state.next_to_commit:
                # Late completion for an already-advanced watermark; no-op.
                return
            state.pending_completed.add(offset)
            while state.next_to_commit in state.pending_completed:
                state.pending_completed.remove(state.next_to_commit)
                state.next_to_commit += 1

    def next_to_commit(self, tp: Hashable) -> Optional[int]:
        """Return the next offset to commit for this partition, or None if untouched.

        Kafka's commit semantics: committing offset N means "next message to read is N",
        so the value returned here is the offset directly above the last fully-completed
        contiguous block.
        """
        with self._lock:
            state = self._state.get(tp)
            return state.next_to_commit if state else None

    def snapshot(self) -> dict[Hashable, int]:
        """Return a snapshot of all partition watermarks for commit."""
        with self._lock:
            return {tp: state.next_to_commit for tp, state in self._state.items()}
