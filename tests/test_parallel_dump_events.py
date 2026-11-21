"""End-to-end check of the chunked dump-events.

A tree is merged by the same ROOT call that adds histograms, so the chunks of a
dump must merge into exactly the tree the serial run writes.
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


def dump_events(outfolder, extra_args):
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join(
        [ROOT_DIR] + [part for part in environment.get("PYTHONPATH", "").split(os.pathsep) if part]
    )
    command = [
        sys.executable,
        "-m",
        "dtpr.cli",
        "dump-events",
        "-i",
        INPUT,
        "-o",
        str(outfolder),
        "--maxevents",
        MAXEVENTS,
    ] + extra_args
    result = subprocess.run(command, cwd=ROOT_DIR, env=environment, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr[-2000:]
    return os.path.join(str(outfolder), "dtpr_events_dumped.root")


def tree_contents(path):
    import uproot

    with uproot.open(path) as handle:
        tree = handle["dtprDumper/Events"]
        return {
            "entries": tree.num_entries,
            "branches": sorted(tree.keys()),
            "values": {name: tree[name].array().tolist() for name in tree.keys()},
        }


def test_chunked_dump_merges_to_the_serial_tree(tmp_path):
    pytest.importorskip("ROOT")
    if not os.path.exists(INPUT):
        pytest.skip("Test ROOT file not found: %s" % INPUT)

    serial = tree_contents(dump_events(tmp_path / "serial", ["--cores", "1"]))
    parallel = tree_contents(dump_events(tmp_path / "parallel", ["--cores", "2"]))

    assert parallel["entries"] == serial["entries"]
    assert parallel["branches"] == serial["branches"]
    assert parallel["values"] == serial["values"]
