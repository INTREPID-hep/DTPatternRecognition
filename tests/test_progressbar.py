"""Tests for the coarse progress bar of the serial event loop."""

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(HERE, ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from dtpr.analysis import fill_histograms as fh


class RecordingBar:
    """Stand-in for tqdm that records every update."""

    def __init__(self, **kwargs):
        self.total = kwargs["total"]
        self.n = 0
        self.updates = []

    def update(self, size):
        self.n += size
        self.updates.append(size)

    def close(self):
        pass


def run_with_bar(monkeypatch, events, clock=None):
    """Run the range loop with a recording bar and return it."""
    bar = RecordingBar(total=len(events))

    def factory(**kwargs):
        return bar

    monkeypatch.setattr(fh, "tqdm", factory)
    if clock is not None:
        monkeypatch.setattr(fh, "time", clock)
    fh.fill_histograms_range(events, {}, progress=True)
    return bar


def test_bar_refreshes_ten_times_over_a_range(monkeypatch):
    bar = run_with_bar(monkeypatch, list(range(100)))
    assert len(bar.updates) == 10
    assert bar.n == 100


def test_short_range_updates_every_event(monkeypatch):
    bar = run_with_bar(monkeypatch, list(range(7)))
    assert len(bar.updates) == 7
    assert bar.n == 7


def test_a_block_slower_than_the_gap_is_split(monkeypatch):
    class Clock:
        def __init__(self):
            self.now = 0.0

        def perf_counter(self):
            self.now += 10.0
            return self.now

    bar = run_with_bar(monkeypatch, list(range(100)), clock=Clock())
    assert len(bar.updates) > 10
    assert bar.n == 100
