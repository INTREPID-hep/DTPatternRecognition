"""Tests for the chunk engine: splitting, parallel writers and the merge."""

import os
import sys
from array import array

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(HERE, ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)


def test_split_entries_covers_every_entry_once():
    from dtpr.core.engine import split_entries

    for nentries in (1, 5, 7, 439):
        for nchunks in (1, 2, 3, 4, 8, 1000):
            chunks = split_entries(nentries, nchunks)
            assert chunks[0][0] == 0
            assert chunks[-1][1] == nentries
            assert sum(end - begin for begin, end in chunks) == nentries
            assert all(begin < end for begin, end in chunks)
            for (_, end), (begin, _) in zip(chunks, chunks[1:]):
                assert end == begin
            assert len(chunks) <= min(nchunks, nentries)

    assert split_entries(0, 4) == [(0, 0)]


def test_job_ranges_cover_every_entry_once():
    from dtpr.core.engine import job_range

    for nentries in (0, 1, 7, 20):
        for job_count in (1, 2, 3, 8):
            ranges = [job_range(nentries, job, job_count) for job in range(job_count)]
            assert sum(end - begin for begin, end in ranges) == nentries
            assert ranges[0][0] == 0
            assert ranges[-1][1] == nentries
            for (_, end), (begin, _) in zip(ranges, ranges[1:]):
                assert end == begin

    with pytest.raises(ValueError):
        job_range(10, 2, 2)


def test_part_paths_live_next_to_the_output(tmp_path):
    from dtpr.core.engine import part_paths

    paths = part_paths(str(tmp_path / "histograms.root"), 2)
    assert paths == [str(tmp_path / "histograms_p0.root"), str(tmp_path / "histograms_p1.root")]


def _write_histogram(chunk, path):
    import ROOT

    begin, end = chunk
    histogram = ROOT.TH1D("h", "", 10, 0, 10)
    for index in range(begin, end):
        histogram.Fill(index % 10)
    output = ROOT.TFile.Open(path, "RECREATE")
    histogram.Write()
    output.Close()


def _write_tree(chunk, path):
    import ROOT

    begin, end = chunk
    output = ROOT.TFile.Open(path, "RECREATE")
    tree = ROOT.TTree("Events", "")
    value = array("i", [0])
    tree.Branch("value", value, "value/I")
    for index in range(begin, end):
        value[0] = index
        tree.Fill()
    tree.Write()
    output.Close()


def _histogram_contents(path, name="h"):
    import ROOT

    handle = ROOT.TFile.Open(path)
    histogram = handle.Get(name)
    contents = [histogram.GetBinContent(index) for index in range(1, histogram.GetNbinsX() + 1)]
    handle.Close()
    return contents


def test_serial_and_parallel_histograms_are_identical(tmp_path):
    from dtpr.core.engine import run_chunks, split_entries

    serial = str(tmp_path / "serial.root")
    run_chunks(_write_histogram, split_entries(40, 1), 1, serial)

    parallel = str(tmp_path / "parallel.root")
    run_chunks(_write_histogram, split_entries(40, 4), 4, parallel)

    assert _histogram_contents(serial) == _histogram_contents(parallel)


def test_one_core_still_writes_every_chunk(tmp_path):
    from dtpr.core.engine import run_chunks, split_entries

    single = str(tmp_path / "single.root")
    run_chunks(_write_histogram, split_entries(40, 1), 1, single)

    several = str(tmp_path / "several.root")
    written = run_chunks(_write_histogram, split_entries(40, 4), 1, several)

    assert written == [os.path.abspath(several)]
    assert _histogram_contents(single) == _histogram_contents(several)


def test_no_merge_keeps_the_parts_and_they_merge_by_hand(tmp_path):
    from dtpr.core.engine import merge_parts, run_chunks, split_entries

    serial = str(tmp_path / "serial.root")
    run_chunks(_write_histogram, split_entries(40, 1), 1, serial)

    outpath = str(tmp_path / "parallel.root")
    parts = run_chunks(_write_histogram, split_entries(40, 4), 4, outpath, merge=False)
    assert len(parts) == 4
    assert all(os.path.exists(part) for part in parts)
    assert not os.path.exists(outpath)

    merge_parts(parts, outpath)
    assert _histogram_contents(serial) == _histogram_contents(outpath)


def test_trees_are_merged_by_the_same_call(tmp_path):
    from dtpr.core.engine import run_chunks, split_entries

    import uproot

    outpath = str(tmp_path / "events.root")
    run_chunks(_write_tree, split_entries(40, 4), 4, outpath)

    with uproot.open(outpath) as handle:
        tree = handle["Events"]
        assert tree.num_entries == 40
        values = tree["value"].array(library="np").tolist()

    assert values == list(range(40))
