import subprocess
import os
import pytest
import sys

# Paths to config and test ntuple
NTUPLE = os.path.abspath(os.path.join(
    os.path.dirname(__file__),
    "ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root"
    )
)

def test_dtpr_cli_entry_point():
    result = subprocess.run(["dtpr", "--help"], capture_output=True, text=True)
    assert result.returncode == 0, f"Failed: {result.stderr}"

@pytest.mark.parametrize(
    "command, extra_args, expect",
    [
        (
            "fill-histos",
            ["--maxevents", "10"],
            "Done"
        ),
        (
            "inspect-events",
            ["-evn", "0:11"],
            "Done"
        ),
        (
            "plot-dt",
            ["-evn", "9", "-sc", "6", "-st", "3", "--artist-names", "all", "--save"],
            "Done"
        ),
        (
            "plot-dts",
            ["-evn", "9", "--artist-names", "all", "--save"],
            "Done"
        ),
    ]
)
def test_dtpr_cli_analysis_commands(command, extra_args, expect):
    args = [
        "dtpr",
        command,
        "-i", NTUPLE,
    ] + extra_args
    result = subprocess.run(args, capture_output=True, text=True)
    assert result.returncode == 0, f"Failed: {result.stderr}"
    # Check for expected output in stdout or stderr
    assert expect.lower() in (result.stdout + result.stderr).lower()

def test_events_visualizer_command():
    args = [
        "dtpr",
        "events-visualizer",
        "-i", NTUPLE
    ]
    if not os.environ.get("DISPLAY"):
        pytest.skip("No display available for GUI tests")

    env = os.environ.copy()
    env["DTPR_TEST_AUTOCLOSE_GUI"] = "1"

    result = subprocess.run(args, capture_output=True, text=True, env=env)
    del env["DTPR_TEST_AUTOCLOSE_GUI"]
    assert result.returncode == 0, f"Failed: {result.stderr}"