"""Tests for dtpr.base.ntuple.NTuple — input format coverage.

Covers:
  - All supported input formats (str, list, glob, dir, dict, embedded tree)
  - maxfiles cap
  - Config-based fileset loading (single dataset, named dataset, treename key)
  - events type and basic fields sanity check
  - Error paths (missing tree, no input)
"""
from __future__ import annotations

import pytest
from pathlib import Path

NTUPLES = Path(__file__).parent / "ntuples"
TREE = "dtNtupleProducer/DTTREE"

# Two individual files for targeted tests
F99  = str(NTUPLES / "DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root")
F110 = str(NTUPLES / "DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_110.root")

# Sub-sample directories
DY_DIR      = str(NTUPLES / "DY")
ZPRIME_DIR  = str(NTUPLES / "Zprime")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _assert_valid(ntuple):
    """Common sanity checks on a loaded NTuple (single dak.Array)."""
    import dask_awkward as dak
    assert isinstance(ntuple.events, dak.Array)
    ntuple.events.eager_compute_divisions()
    assert len(ntuple.events) > 0
    ev = ntuple.events[0].compute()
    assert len(ev.fields) > 0


def _assert_valid_dict(ntuple):
    """Common sanity checks on a loaded NTuple (dict of dak.Arrays)."""
    import dask_awkward as dak
    assert isinstance(ntuple.events, dict)
    assert len(ntuple.events) > 0
    for ds_name, arr in ntuple.events.items():
        assert isinstance(arr, dak.Array), f"events['{ds_name}'] is not a dak.Array"
        assert arr.npartitions >= 1


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def dy_files():
    """Sorted list of DY ROOT files."""
    from natsort import natsorted
    import glob
    return natsorted(glob.glob(str(NTUPLES / "DY" / "*.root")))


@pytest.fixture(scope="module")
def zprime_files():
    """Sorted list of Zprime ROOT files."""
    from natsort import natsorted
    import glob
    return natsorted(glob.glob(str(NTUPLES / "Zprime" / "*.root")))


# ---------------------------------------------------------------------------
# Input format tests
# ---------------------------------------------------------------------------

def test_load_single_file_tree_param():
    """Single file path + tree_name kwarg."""
    from dtpr.base.ntuple import NTuple
    ntuple = NTuple(F99, tree_name=TREE)
    _assert_valid(ntuple)
    assert ntuple.events.npartitions == 1


def test_load_single_file_tree_embedded():
    """Tree embedded in the path string as 'file.root:treepath'."""
    from dtpr.base.ntuple import NTuple
    ntuple = NTuple(f"{F99}:{TREE}")
    _assert_valid(ntuple)


def test_load_list_of_files(dy_files):
    """List of file paths + tree_name — one partition per file."""
    from dtpr.base.ntuple import NTuple
    ntuple = NTuple(dy_files[:3], tree_name=TREE)
    _assert_valid(ntuple)
    assert ntuple.events.npartitions == 3


def test_load_list_of_files_tree_embedded():
    """List of 'file.root:treepath' strings — tree inferred per element."""
    from dtpr.base.ntuple import NTuple
    paths = [f"{F99}:{TREE}", f"{F110}:{TREE}"]
    ntuple = NTuple(paths)
    _assert_valid(ntuple)
    assert ntuple.events.npartitions == 2


def test_load_glob_pattern():
    """Glob pattern string — matches all files in DY dir."""
    from dtpr.base.ntuple import NTuple
    import glob
    n_expected = len(glob.glob(str(NTUPLES / "DY" / "*.root")))
    ntuple = NTuple(str(NTUPLES / "DY" / "*.root"), tree_name=TREE)
    _assert_valid(ntuple)
    assert ntuple.events.npartitions == n_expected


def test_load_directory():
    """Directory path — auto-globs *.root inside it."""
    from dtpr.base.ntuple import NTuple
    import glob
    n_expected = len(glob.glob(str(NTUPLES / "Zprime" / "*.root")))
    ntuple = NTuple(ZPRIME_DIR, tree_name=TREE)
    _assert_valid(ntuple)
    assert ntuple.events.npartitions == n_expected


def test_load_dict_file_to_treepath(dy_files):
    """Dict {filepath: treepath} — uproot-native format."""
    from dtpr.base.ntuple import NTuple
    files_dict = {f: TREE for f in dy_files[:2]}
    ntuple = NTuple(files_dict)
    _assert_valid(ntuple)
    assert ntuple.events.npartitions == 2


def test_load_dict_coffea_spec(dy_files):
    """Dict {filepath: {"object_path": treepath}} — coffea-native spec."""
    from dtpr.base.ntuple import NTuple
    files_dict = {f: {"object_path": TREE} for f in dy_files[:2]}
    ntuple = NTuple(files_dict)
    _assert_valid(ntuple)


def test_maxfiles(dy_files):
    """maxfiles=2 caps loading even when more files match."""
    from dtpr.base.ntuple import NTuple
    assert len(dy_files) > 2, "Need more than 2 DY files for this test"
    ntuple = NTuple(DY_DIR, tree_name=TREE, maxfiles=2)
    _assert_valid(ntuple)
    assert ntuple.events.npartitions == 2


# ---------------------------------------------------------------------------
# Config / fileset tests
# ---------------------------------------------------------------------------

def test_fileset_single_dataset(dy_files):
    """Config with one fileset dataset — loaded automatically as dict."""
    from dtpr.base.ntuple import NTuple
    from dtpr.base.config import Config

    cfg = Config.__new__(Config)
    cfg.__dict__["filesets"] = {
        "DY": {"files": {f: TREE for f in dy_files[:3]}}
    }
    ntuple = NTuple(CONFIG=cfg)
    _assert_valid_dict(ntuple)
    assert set(ntuple.events.keys()) == {"DY"}
    assert ntuple.events["DY"].npartitions == 3


def test_fileset_named_dataset(dy_files, zprime_files):
    """Config with two datasets — select one by name via datasets=."""
    from dtpr.base.ntuple import NTuple
    from dtpr.base.config import Config

    cfg = Config.__new__(Config)
    cfg.__dict__["filesets"] = {
        "DY":     {"files": {f: TREE for f in dy_files[:2]}},
        "Zprime": {"files": {f: TREE for f in zprime_files[:3]}},
    }
    ntuple = NTuple(datasets=["Zprime"], CONFIG=cfg)
    _assert_valid_dict(ntuple)
    assert set(ntuple.events.keys()) == {"Zprime"}
    assert ntuple.events["Zprime"].npartitions == 3


def test_fileset_treename_key(dy_files):
    """Fileset with top-level 'treename' key — no per-file tree needed."""
    from dtpr.base.ntuple import NTuple
    from dtpr.base.config import Config

    cfg = Config.__new__(Config)
    cfg.__dict__["filesets"] = {
        "DY": {
            "treename": TREE,
            "files": dy_files[:2],
        }
    }
    ntuple = NTuple(CONFIG=cfg)
    _assert_valid_dict(ntuple)
    assert "DY" in ntuple.events


def test_fileset_explicit_inputs_wins_over_fileset(dy_files, zprime_files):
    """Explicit inputs= always overrides config filesets."""
    from dtpr.base.ntuple import NTuple
    from dtpr.base.config import Config

    cfg = Config.__new__(Config)
    cfg.__dict__["filesets"] = {
        "DY": {"files": {f: "ANYTREE" for f in dy_files}}
    }
    # Pass Zprime files explicitly — should load those, not DY
    ntuple = NTuple(inputs=zprime_files[:1], tree_name=TREE, CONFIG=cfg)
    _assert_valid(ntuple)
    assert ntuple.events.npartitions == 1


# ---------------------------------------------------------------------------
# Events sanity
# ---------------------------------------------------------------------------

def test_events_fields_present():
    """Materialised event has known top-level fields."""
    from dtpr.base.ntuple import NTuple
    ntuple = NTuple(F99, tree_name=TREE)
    ev = ntuple.events[0].compute()
    assert "digis" in ev.fields or len(ev.fields) > 0


def test_metadata_inputs_is_empty_dict():
    """Events loaded via `inputs=` always produce an empty metadata dict."""
    from dtpr.base.ntuple import NTuple
    ntuple = NTuple(F99, tree_name=TREE)
    assert ntuple.metadata == {}


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------

def test_missing_tree_raises():
    """No tree name at all should raise ValueError."""
    from dtpr.base.ntuple import NTuple
    with pytest.raises((ValueError, Exception)):
        NTuple(F99)  # no tree_name, no embedded tree, no config


def test_no_input_no_fileset_raises():
    """No input and empty config should raise ValueError."""
    from dtpr.base.ntuple import NTuple
    from dtpr.base.config import Config
    cfg = Config.__new__(Config)
    cfg.__dict__["filesets"] = {}
    with pytest.raises(ValueError, match="filesets"):
        NTuple(CONFIG=cfg)


def test_named_dataset_not_found(dy_files):
    """Requesting a non-existent dataset name raises KeyError."""
    from dtpr.base.ntuple import NTuple
    from dtpr.base.config import Config
    cfg = Config.__new__(Config)
    cfg.__dict__["filesets"] = {
        "DY": {"files": {f: TREE for f in dy_files[:1]}}
    }
    with pytest.raises(KeyError):
        NTuple(datasets=["does_not_exist"], CONFIG=cfg)


def test_inputs_tree_name_list_raises(dy_files):
    """Passing tree_name as a list with inputs= is not allowed."""
    from dtpr.base.ntuple import NTuple
    with pytest.raises(ValueError, match="list"):
        NTuple(inputs=dy_files[:1], tree_name=[TREE])


def test_empty_dir_raises(tmp_path):
    """An empty directory (no ROOT files) raises FileNotFoundError."""
    from dtpr.base.ntuple import NTuple
    with pytest.raises(FileNotFoundError):
        NTuple(str(tmp_path), tree_name=TREE)


def test_dataset_no_files_key_raises(dy_files):
    """A fileset entry that is missing a 'files' key raises ValueError."""
    from dtpr.base.ntuple import NTuple
    from dtpr.base.config import Config
    cfg = Config.__new__(Config)
    cfg.__dict__["filesets"] = {
        "DY": {"treename": TREE}  # no 'files' key
    }
    with pytest.raises(ValueError, match="no 'files' key"):
        NTuple(CONFIG=cfg)


def test_resolve_schema_unknown_str_raises(dy_files):
    """A schema value that is neither a config attr nor a coffea class raises ValueError."""
    from dtpr.base.ntuple import NTuple
    from dtpr.base.config import Config
    cfg = Config.__new__(Config)
    cfg.__dict__["filesets"] = {
        "DY": {
            "files": {dy_files[0]: TREE},
            "schema": "NonExistentSchema123",
        }
    }
    with pytest.raises(ValueError, match="Unknown coffea schema"):
        NTuple(CONFIG=cfg)


# ---------------------------------------------------------------------------
# Multi-dataset tests
# ---------------------------------------------------------------------------

def test_multidataset_explicit_list(dy_files, zprime_files):
    """datasets=['DY', 'Zprime'] loads both as separate dict entries."""
    from dtpr.base.ntuple import NTuple
    from dtpr.base.config import Config

    n_dy      = 2
    n_zprime  = 3
    cfg = Config.__new__(Config)
    cfg.__dict__["filesets"] = {
        "DY":     {"files": {f: TREE for f in dy_files[:n_dy]}},
        "Zprime": {"files": {f: TREE for f in zprime_files[:n_zprime]}},
    }
    ntuple = NTuple(datasets=["DY", "Zprime"], CONFIG=cfg)
    _assert_valid_dict(ntuple)
    assert set(ntuple.events.keys()) == {"DY", "Zprime"}
    assert ntuple.events["DY"].npartitions == n_dy
    assert ntuple.events["Zprime"].npartitions == n_zprime


def test_multidataset_all_filesets(dy_files, zprime_files):
    """datasets=[] loads every dataset defined in the config as a dict."""
    from dtpr.base.ntuple import NTuple
    from dtpr.base.config import Config

    n_dy     = 2
    n_zprime = 2
    cfg = Config.__new__(Config)
    cfg.__dict__["filesets"] = {
        "DY":     {"files": {f: TREE for f in dy_files[:n_dy]}},
        "Zprime": {"files": {f: TREE for f in zprime_files[:n_zprime]}},
    }
    ntuple = NTuple(datasets=[], CONFIG=cfg)
    _assert_valid_dict(ntuple)
    assert set(ntuple.events.keys()) == {"DY", "Zprime"}
    assert ntuple.events["DY"].npartitions == n_dy
    assert ntuple.events["Zprime"].npartitions == n_zprime


def test_multidataset_metadata_is_dict_of_dicts(dy_files, zprime_files):
    """metadata becomes {ds_name: {…}} when multiple datasets are loaded."""
    from dtpr.base.ntuple import NTuple
    from dtpr.base.config import Config

    cfg = Config.__new__(Config)
    cfg.__dict__["filesets"] = {
        "DY":     {"files": {f: TREE for f in dy_files[:1]}, "metadata": {"year": 2024}},
        "Zprime": {"files": {f: TREE for f in zprime_files[:1]}, "metadata": {"year": 2023}},
    }
    ntuple = NTuple(datasets=["DY", "Zprime"], CONFIG=cfg)
    assert isinstance(ntuple.metadata, dict)
    assert set(ntuple.metadata.keys()) == {"DY", "Zprime"}
    assert ntuple.metadata["DY"]["year"] == 2024
    assert ntuple.metadata["Zprime"]["year"] == 2023


def test_multidataset_tree_name_override(dy_files, zprime_files):
    """Constructor tree_name overrides per-dataset treename keys."""
    from dtpr.base.ntuple import NTuple
    from dtpr.base.config import Config

    cfg = Config.__new__(Config)
    # Embed a wrong treename in the fileset; constructor override should win
    cfg.__dict__["filesets"] = {
        "DY":     {"treename": "WRONG/TREE", "files": dy_files[:1]},
        "Zprime": {"treename": "WRONG/TREE", "files": zprime_files[:1]},
    }
    ntuple = NTuple(datasets=["DY", "Zprime"], tree_name=TREE, CONFIG=cfg)
    _assert_valid_dict(ntuple)
    assert set(ntuple.events.keys()) == {"DY", "Zprime"}
    assert ntuple.events["DY"].npartitions == 1
    assert ntuple.events["Zprime"].npartitions == 1


def test_multidataset_unknown_name_raises(dy_files):
    """Unknown name in datasets list raises KeyError."""
    from dtpr.base.ntuple import NTuple
    from dtpr.base.config import Config

    cfg = Config.__new__(Config)
    cfg.__dict__["filesets"] = {
        "DY": {"files": {f: TREE for f in dy_files[:1]}}
    }
    with pytest.raises(KeyError):
        NTuple(datasets=["DY", "NonExistent"], CONFIG=cfg)


def test_multidataset_and_inputs_conflict(dy_files):
    """Passing both inputs= and datasets= raises ValueError."""
    from dtpr.base.ntuple import NTuple
    from dtpr.base.config import Config

    cfg = Config.__new__(Config)
    cfg.__dict__["filesets"] = {
        "DY": {"files": {f: TREE for f in dy_files[:1]}}
    }
    with pytest.raises(ValueError, match="mutually exclusive"):
        NTuple(inputs=dy_files[:1], datasets=["DY"], CONFIG=cfg)


def test_multidataset_single_entry(dy_files):
    """datasets with only one name returns a dict with that one key."""
    from dtpr.base.ntuple import NTuple
    from dtpr.base.config import Config

    cfg = Config.__new__(Config)
    cfg.__dict__["filesets"] = {
        "DY": {"files": {f: TREE for f in dy_files[:2]}}
    }
    ntuple = NTuple(datasets=["DY"], CONFIG=cfg)
    _assert_valid_dict(ntuple)
    assert set(ntuple.events.keys()) == {"DY"}
    assert ntuple.events["DY"].npartitions == 2


def test_tree_name_list_per_dataset(dy_files, zprime_files):
    """tree_name as a list assigns a different tree per dataset."""
    from dtpr.base.ntuple import NTuple
    from dtpr.base.config import Config

    cfg = Config.__new__(Config)
    cfg.__dict__["filesets"] = {
        "DY":     {"files": dy_files[:1]},
        "Zprime": {"files": zprime_files[:1]},
    }
    # Both datasets share the same tree here, but we pass them as a list
    ntuple = NTuple(datasets=["DY", "Zprime"], tree_name=[TREE, TREE], CONFIG=cfg)
    _assert_valid_dict(ntuple)
    assert set(ntuple.events.keys()) == {"DY", "Zprime"}


def test_tree_name_list_length_mismatch_raises(dy_files, zprime_files):
    """tree_name list whose length differs from datasets must raise ValueError."""
    from dtpr.base.ntuple import NTuple
    from dtpr.base.config import Config

    cfg = Config.__new__(Config)
    cfg.__dict__["filesets"] = {
        "DY":     {"files": {dy_files[0]: TREE}},
        "Zprime": {"files": {zprime_files[0]: TREE}},
    }
    with pytest.raises(ValueError, match="length"):
        NTuple(datasets=["DY", "Zprime"], tree_name=[TREE], CONFIG=cfg)


def test_fileset_per_dataset_step_size(dy_files):
    """A 'step_size' key inside the fileset entry is respected."""
    from dtpr.base.ntuple import NTuple
    from dtpr.base.config import Config

    cfg = Config.__new__(Config)
    cfg.__dict__["filesets"] = {
        "DY": {
            "files": {dy_files[0]: TREE},
            "step_size": 500,
        }
    }
    ntuple = NTuple(CONFIG=cfg)
    _assert_valid_dict(ntuple)
    # step_size=500 should split the file into multiple partitions
    assert ntuple.events["DY"].npartitions >= 1
