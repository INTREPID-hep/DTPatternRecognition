"""Tests for the entry-range view of NTuple and EventList.

An ``entry_range=(begin, end)`` exposes a contiguous window of the chain so a
single file can be split into chunks. The window must be indistinguishable from
the same slice of the full list, entry for entry.
"""

import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(HERE, ".."))
for path in (ROOT_DIR, HERE):
    if path not in sys.path:
        sys.path.insert(0, path)

from event_digest import TEST_FILE, digest_event  # noqa: E402

INPUT = os.path.join(ROOT_DIR, TEST_FILE)


def build(entry_range=None):
    if not os.path.exists(INPUT):
        pytest.skip("Test ROOT file not found: %s" % INPUT)
    from dtpr.base import NTuple

    return NTuple(INPUT, entry_range=entry_range)


@pytest.fixture(scope="module")
def full():
    return build()


def assert_same_events(actual, expected):
    assert len(actual) == len(expected)
    for index, (got, exp) in enumerate(zip(actual, expected)):
        if exp is None:
            assert got is None, "event %d should have been rejected by the selector" % index
            continue
        assert digest_event(got) == digest_event(exp), "event %d differs from the full list" % index


def test_default_range_is_the_whole_chain(full):
    assert len(full.events) == full.tree.GetEntries()


def test_range_len_index_slice_iter(full):
    begin, end = 3, 7
    window = build((begin, end))

    assert len(window.events) == end - begin

    expected = [full.events[index] for index in range(begin, end)]
    assert_same_events(list(window.events), expected)

    assert window.events[0].index == begin, "Event.index keeps the entry number in the tree"
    assert window.events[0].number == full.events[begin].number
    assert window.events[-1].number == full.events[end - 1].number
    assert_same_events(list(window.events[1:3]), expected[1:3])


def test_range_with_open_end(full):
    total = full.tree.GetEntries()
    window = build((total - 2, None))
    assert len(window.events) == 2
    assert window.events[-1].number == full.events[total - 1].number


def test_range_is_clamped_to_the_chain(full):
    total = full.tree.GetEntries()
    empty = build((total + 5, total + 10))
    assert len(empty.events) == 0
    with pytest.raises(IndexError):
        empty.events[0]

    clamped = build((total - 1, total + 10))
    assert len(clamped.events) == 1
    assert clamped.events[0].number == full.events[total - 1].number


def test_index_out_of_range(full):
    window = build((0, 2))
    with pytest.raises(IndexError):
        window.events[2]
    with pytest.raises(IndexError):
        window.events[-3]


def test_get_by_number_respects_the_range(full):
    window = build((3, 7))
    inside = full.events[5].number
    assert digest_event(window.events.get_by_number(inside)) == digest_event(full.events[5])

    outside = full.events[1].number
    with pytest.raises(ValueError):
        window.events.get_by_number(outside)


def test_event_list_accepts_the_range_directly(full):
    from dtpr.base import EventList

    window = EventList(full.tree, CONFIG=full.CONFIG, begin=3, end=5)
    assert len(window) == 2
    assert window[0].number == full.events[3].number
    assert [event.number for event in window] == [full.events[3].number, full.events[4].number]
