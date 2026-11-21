"""Tests for EntryView: the cached, sparse view of one TTree entry."""

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
OTHER = os.path.join(
    ROOT_DIR, "tests", "ntuples", "DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_110.root"
)


def ntuple(path):
    if not os.path.exists(path):
        pytest.skip("Test ROOT file not found: %s" % path)
    from dtpr.base import NTuple

    return NTuple(path)


def view_of(ntuple_object):
    from dtpr.base.entry_view import EntryView

    return EntryView(ntuple_object.tree)


def test_values_are_cached_and_refreshed():
    full = ntuple(INPUT)
    view = view_of(full)

    view.set_entry(3)
    first = view.digi_wheel
    assert view.digi_wheel is first, "the same entry must hand back the same list"
    assert len(first) == len(full.events[3].digis)

    view.set_entry(4)
    assert view.digi_wheel is not first, "a new entry must drop the cached list"


def test_unknown_branch_raises_attribute_error():
    full = ntuple(INPUT)
    view = view_of(full)
    view.set_entry(0)
    with pytest.raises(AttributeError):
        view.this_branch_does_not_exist


def test_out_of_range_entry_raises():
    full = ntuple(INPUT)
    view = view_of(full)
    with pytest.raises(RuntimeError):
        view.set_entry(full.tree.GetEntries() + 5)


def test_chain_across_files_reads_the_right_entry():
    first = ntuple(INPUT)
    second = ntuple(OTHER)
    from dtpr.base import NTuple

    chain = NTuple([INPUT, OTHER])
    assert len(chain.events) == len(first.events) + len(second.events)

    def digest(event):
        return None if event is None else digest_event(event)

    boundary = len(first.events)
    # Jump back and forth across the file boundary: the branch cache of the
    # view has to be invalidated when the chain loads the other tree.
    order = [
        boundary - 1,
        boundary,
        boundary - 2,
        boundary + 1,
        boundary - 3,
        boundary + 2,
    ]
    for index in order:
        if index < boundary:
            expected = first.events[index]
        else:
            expected = second.events[index - boundary]
        assert digest(chain.events[index]) == digest(expected), "entry %d differs" % index
