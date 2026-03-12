from __future__ import annotations

from pathlib import Path

import pytest


TREE_NAME = "dtNtupleProducer/DTTREE"


@pytest.fixture(scope="session")
def tests_dir() -> Path:
    return Path(__file__).parent


@pytest.fixture(scope="session")
def ntuples_dir(tests_dir: Path) -> Path:
    return tests_dir / "ntuples"


@pytest.fixture(scope="session")
def zprime_files(ntuples_dir: Path) -> list[str]:
    return sorted(str(p) for p in (ntuples_dir / "Zprime").glob("*.root"))


@pytest.fixture(scope="session")
def dy_files(ntuples_dir: Path) -> list[str]:
    return sorted(str(p) for p in (ntuples_dir / "DY").glob("*.root"))


@pytest.fixture(scope="session")
def sample_root_file(zprime_files: list[str]) -> str:
    if not zprime_files:
        raise RuntimeError("No test ROOT files found under tests/ntuples/Zprime")
    return zprime_files[0]


@pytest.fixture
def outdir(tmp_path: Path) -> Path:
    return tmp_path / "out"
