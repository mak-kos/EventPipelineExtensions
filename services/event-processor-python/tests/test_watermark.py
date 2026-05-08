"""Tests for OffsetWatermark: in-order, out-of-order, late completions."""

from __future__ import annotations

from watermark import OffsetWatermark


def test_initial_state_is_none():
    w = OffsetWatermark()
    assert w.next_to_commit(("t", 0)) is None
    assert w.snapshot() == {}


def test_in_order_completions_advance_watermark():
    w = OffsetWatermark()
    tp = ("t", 0)
    for offset in [10, 11, 12, 13]:
        w.observe(tp, offset)
        w.complete(tp, offset)
    assert w.next_to_commit(tp) == 14


def test_out_of_order_completions_hold_watermark_until_contiguous():
    w = OffsetWatermark()
    tp = ("t", 0)
    for offset in [10, 11, 12]:
        w.observe(tp, offset)

    # Complete out of order: 12, 11, 10
    w.complete(tp, 12)
    assert w.next_to_commit(tp) == 10  # still waiting for 10

    w.complete(tp, 11)
    assert w.next_to_commit(tp) == 10  # still waiting for 10

    w.complete(tp, 10)
    assert w.next_to_commit(tp) == 13  # all caught up


def test_partial_progress_then_gap():
    w = OffsetWatermark()
    tp = ("t", 0)
    for offset in [5, 6, 7, 8, 9]:
        w.observe(tp, offset)

    w.complete(tp, 5)
    w.complete(tp, 6)
    assert w.next_to_commit(tp) == 7

    # 7 is missing; 8 and 9 complete -> watermark stays at 7
    w.complete(tp, 8)
    w.complete(tp, 9)
    assert w.next_to_commit(tp) == 7

    # 7 finally completes -> jumps to 10
    w.complete(tp, 7)
    assert w.next_to_commit(tp) == 10


def test_late_completion_below_watermark_is_ignored():
    w = OffsetWatermark()
    tp = ("t", 0)
    w.observe(tp, 100)
    w.complete(tp, 100)
    assert w.next_to_commit(tp) == 101

    # Some delayed completion for an offset we already advanced past — must not regress.
    w.complete(tp, 100)
    assert w.next_to_commit(tp) == 101


def test_multiple_partitions_are_independent():
    w = OffsetWatermark()
    a = ("topic", 0)
    b = ("topic", 1)
    w.observe(a, 0)
    w.observe(b, 100)
    w.complete(a, 0)
    assert w.next_to_commit(a) == 1
    assert w.next_to_commit(b) == 100  # b not yet completed

    w.complete(b, 100)
    assert w.next_to_commit(b) == 101


def test_snapshot_returns_all_partitions():
    w = OffsetWatermark()
    a = ("topic", 0)
    b = ("topic", 1)
    w.observe(a, 0)
    w.observe(b, 5)
    w.complete(a, 0)
    snap = w.snapshot()
    assert snap == {a: 1, b: 5}
