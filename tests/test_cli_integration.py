from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest


YDANA_BIN = os.path.join(os.path.dirname(sys.executable), "ydana")


def _run(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, capture_output=True, text=True)


def test_ydana_cli_entrypoint_help() -> None:
    result = _run([YDANA_BIN, "--help"])

    assert result.returncode == 0
    assert "fill-histos" in result.stdout
    assert "dump-events" in result.stdout


@pytest.mark.parametrize(
    "subcommand",
    ["fill-histos", "dump-events", "merge-histos", "merge-roots", "test-cli"],
)
def test_ydana_subcommand_help(subcommand: str) -> None:
    result = _run([YDANA_BIN, subcommand, "--help"])

    assert result.returncode == 0


def test_ydana_test_cli_smoke(sample_root_file: str, tmp_path: Path) -> None:
    result = _run(
        [
            YDANA_BIN,
            "test-cli",
            "-i",
            sample_root_file,
            "-tr",
            "dtNtupleProducer/DTTREE",
            "--maxfiles",
            "1",
            "-o",
            str(tmp_path / "cli-out"),
        ]
    )

    assert result.returncode == 0, result.stderr
    text = (result.stdout + result.stderr).lower()
    assert "cli argument dump" in text


def test_ydana_fill_histos_smoke(sample_root_file: str, tmp_path: Path) -> None:
    result = _run(
        [
            YDANA_BIN,
            "fill-histos",
            "-i",
            sample_root_file,
            "-tr",
            "dtNtupleProducer/DTTREE",
            "-r",
            "--maxfiles",
            "1",
            "-o",
            str(tmp_path / "fill-out"),
            "--no-verbose",
        ]
    )

    assert result.returncode == 0, result.stderr
    assert (tmp_path / "fill-out" / "histograms").exists()
