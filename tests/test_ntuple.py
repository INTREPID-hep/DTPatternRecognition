from __future__ import annotations

from pathlib import Path

import dask_awkward as dak
import pytest

from ydana.base.config import Config
from ydana.base.ntuple import NTuple


TREE = "dtNtupleProducer/DTTREE"


def _build_min_config(filesets: dict) -> Config:
    cfg = Config.__new__(Config)
    cfg.__dict__["path"] = str(Path.cwd())
    cfg.__dict__["filesets"] = filesets
    cfg.__dict__["pre-steps"] = {}
    return cfg


def test_ntuple_root_single_file_loads_lazy_array(sample_root_file: str) -> None:
    ntuple = NTuple(inputs=sample_root_file, tree_name=TREE, root=True, parquet=False, verbose=False)

    assert isinstance(ntuple.events, dak.Array)
    assert ntuple.events.npartitions == 1


def test_ntuple_root_glob_loads_all_files(zprime_files: list[str], ntuples_dir: Path) -> None:
    pattern = str(ntuples_dir / "Zprime" / "*.root")

    ntuple = NTuple(inputs=pattern, tree_name=TREE, root=True, parquet=False, verbose=False)

    assert isinstance(ntuple.events, dak.Array)
    assert ntuple.events.npartitions == len(zprime_files)


def test_ntuple_root_directory_respects_maxfiles(ntuples_dir: Path) -> None:
    ntuple = NTuple(
        inputs=str(ntuples_dir / "DY"),
        tree_name=TREE,
        maxfiles=2,
        root=True,
        parquet=False,
        verbose=False,
    )

    assert ntuple.events.npartitions == 2


def test_ntuple_root_embedded_tree_path_syntax(sample_root_file: str) -> None:
    ntuple = NTuple(inputs=f"{sample_root_file}:{TREE}", root=True, parquet=False, verbose=False)

    assert isinstance(ntuple.events, dak.Array)
    assert ntuple.events.npartitions == 1


def test_ntuple_datasets_mode_returns_events_map(dy_files: list[str], zprime_files: list[str]) -> None:
    cfg = _build_min_config(
        {
            "DY": {"files": dy_files[:2], "treename": TREE, "metadata": {"year": 2024}},
            "Zprime": {"files": zprime_files[:1], "treename": TREE, "metadata": {"year": 2023}},
        }
    )

    ntuple = NTuple(datasets=["DY", "Zprime"], root=True, parquet=False, CONFIG=cfg, verbose=False)

    assert isinstance(ntuple.events, dict)
    assert set(ntuple.events.keys()) == {"DY", "Zprime"}
    assert ntuple.events["DY"].npartitions == 2
    assert ntuple.events["Zprime"].npartitions == 1
    assert ntuple.metadata["DY"]["year"] == 2024


def test_ntuple_parquet_roundtrip(sample_root_file: str, tmp_path: Path) -> None:
    from ydana.base.io import dump_to_parquet

    root_ntuple = NTuple(inputs=sample_root_file, tree_name=TREE, root=True, parquet=False, verbose=False)
    eager_events = root_ntuple.events[:10].compute()

    outdir = tmp_path / "parquet"
    dump_to_parquet(eager_events, str(outdir), ncores=1, verbose=False)

    parquet_ntuple = NTuple(inputs=str(outdir), root=False, parquet=True, verbose=False)

    assert isinstance(parquet_ntuple.events, dak.Array)
    assert parquet_ntuple.events.npartitions >= 1


def test_ntuple_requires_exactly_one_format(sample_root_file: str) -> None:
    with pytest.raises(ValueError, match="Either root or parquet"):
        NTuple(inputs=sample_root_file, tree_name=TREE, root=False, parquet=False, verbose=False)

    with pytest.raises(ValueError, match="Either root or parquet"):
        NTuple(inputs=sample_root_file, tree_name=TREE, root=True, parquet=True, verbose=False)


def test_ntuple_inputs_and_datasets_are_mutually_exclusive(sample_root_file: str) -> None:
    cfg = _build_min_config({"DY": {"files": [sample_root_file], "treename": TREE}})

    with pytest.raises(ValueError, match="mutually exclusive"):
        NTuple(
            inputs=sample_root_file,
            datasets=["DY"],
            tree_name=TREE,
            root=True,
            parquet=False,
            CONFIG=cfg,
            verbose=False,
        )


def test_ntuple_default_config_loads_all_filesets() -> None:
    ntuple = NTuple(root=True, parquet=False, maxfiles=1, verbose=False)

    assert isinstance(ntuple.events, dict)
    assert {"simulation", "DY", "Zprime"}.issubset(ntuple.events.keys())
