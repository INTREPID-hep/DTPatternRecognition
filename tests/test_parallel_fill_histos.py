"""End-to-end check of the chunked fill-histos.

The promise of the parallel path is that it produces the same histograms as a
serial run, draw-based ones included: each chunk draws its own entry range and
the merge adds the histograms up.
"""

import os
import subprocess
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(HERE, ".."))
INPUT = os.path.join(
    ROOT_DIR, "tests", "ntuples", "DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root"
)
MAXEVENTS = "20"


def fill_histos(outfolder, extra_args):
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join(
        [ROOT_DIR] + [part for part in environment.get("PYTHONPATH", "").split(os.pathsep) if part]
    )
    command = [
        sys.executable,
        "-m",
        "dtpr.cli",
        "fill-histos",
        "-i",
        INPUT,
        "-o",
        str(outfolder),
        "--maxevents",
        MAXEVENTS,
    ] + extra_args
    result = subprocess.run(command, cwd=ROOT_DIR, env=environment, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr[-2000:]
    return os.path.join(str(outfolder), "histograms")


def histogram_contents(path):
    import ROOT

    handle = ROOT.TFile.Open(path)
    assert handle, "cannot open %s" % path
    contents = {}
    for key in handle.GetListOfKeys():
        histogram = key.ReadObj()
        if histogram.InheritsFrom("TH1"):
            contents[key.GetName()] = (
                histogram.GetEntries(),
                [histogram.GetBinContent(i) for i in range(1, histogram.GetNbinsX() + 1)],
            )
    handle.Close()
    return contents


def test_chunked_run_writes_the_same_histograms_as_a_serial_one(tmp_path):
    pytest.importorskip("ROOT")
    if not os.path.exists(INPUT):
        pytest.skip("Test ROOT file not found: %s" % INPUT)

    serial = histogram_contents(os.path.join(fill_histos(tmp_path / "serial", ["--cores", "1"]), "histograms.root"))
    parallel = histogram_contents(os.path.join(fill_histos(tmp_path / "parallel", ["--cores", "4"]), "histograms.root"))

    assert "gen_pt_draw" in serial, "the draw-based histogram is missing from the serial run"
    assert parallel == serial


def test_no_merge_keeps_the_parts_and_they_merge_to_the_serial_result(tmp_path):
    pytest.importorskip("ROOT")
    if not os.path.exists(INPUT):
        pytest.skip("Test ROOT file not found: %s" % INPUT)
    from dtpr.core.engine import merge_parts

    serial = histogram_contents(os.path.join(fill_histos(tmp_path / "serial", ["--cores", "1"]), "histograms.root"))

    folder = fill_histos(tmp_path / "parts", ["--cores", "4", "--no-merge"])
    parts = [os.path.join(folder, "histograms_p%d.root" % index) for index in range(4)]
    assert all(os.path.exists(part) for part in parts), "the chunk files were not kept"
    assert not os.path.exists(os.path.join(folder, "histograms.root"))

    merged = os.path.join(folder, "merged.root")
    merge_parts(parts, merged)
    assert histogram_contents(merged) == serial
