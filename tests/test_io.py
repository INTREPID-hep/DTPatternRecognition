"""Tests for dtpr.utils.io — dump_to_root and _collect_branches."""

import os
import tempfile

import pytest
import awkward as ak
import uproot

from dtpr.utils.io import dump_to_root, _collect_branches

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

NTUPLE_FILE = os.path.abspath(
    os.path.join(os.path.dirname(__file__),
                 "ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root")
)


@pytest.fixture
def simple_events():
    """Events with scalars and a flat collection."""
    return ak.Array([
        {"run": 1, "lumi": 1, "digis": [{"wh": -1, "BX": 0}, {"wh": 2, "BX": 1}]},
        {"run": 2, "lumi": 2, "digis": [{"wh": 1, "BX": 0}]},
    ])


@pytest.fixture
def nested_events():
    """Events where nested-collection elements are full ParticleRecord-like objects.

    matched_showers: each element is a shower ParticleRecord with fields
                     (wh, sector, station, idx).  _collect_branches should find
                     ``idx`` via _IDX_PATTERN and write only
                     ``tps_matched_showers_idx`` — discarding wh/sector/station.
    raw_hits:        each element has only ``wire``, which does not match
                     _IDX_PATTERN, so the fallback ``tps_raw_hits_n`` is written.
    """
    return ak.Array([
        {
            "run": 1,
            "tps": [
                {"quality": 2, "BX": 0,
                 "matched_showers": [{"wh": 0, "sector": 5, "station": 1, "idx": 5}],
                 "raw_hits":        [{"wire": 11}, {"wire": 12}]},
                {"quality": 3, "BX": 1,
                 "matched_showers": [],
                 "raw_hits":        [{"wire": 20}]},
            ],
        },
        {
            "run": 2,
            "tps": [
                {"quality": 1, "BX": 0,
                 "matched_showers": [{"wh": 1,  "sector": 3, "station": 2, "idx": 2},
                                     {"wh": -1, "sector": 4, "station": 3, "idx": 7}],
                 "raw_hits":        [{"wire": 30}]},
            ],
        },
    ])


@pytest.fixture
def output_path():
    """Temporary file path for ROOT output; cleaned up after the test."""
    with tempfile.NamedTemporaryFile(suffix=".root", delete=False) as f:
        path = f.name
    yield path
    if os.path.exists(path):
        os.remove(path)


# ---------------------------------------------------------------------------
# _collect_branches unit tests
# ---------------------------------------------------------------------------

class TestCollectBranches:
    def test_scalar_fields(self, simple_events):
        branches = _collect_branches(simple_events)
        assert "run" in branches
        assert "lumi" in branches
        assert branches["run"].tolist() == [1, 2]

    def test_flat_collection_flattened(self, simple_events):
        branches = _collect_branches(simple_events)
        assert "digis_wh" in branches
        assert "digis_BX" in branches
        assert branches["digis_wh"].tolist() == [[-1, 2], [1]]

    def test_nested_with_idx_writes_only_idx(self, nested_events):
        branches = _collect_branches(nested_events)
        # Fixed suffix _ids regardless of internal field name (idx / id / index / …)
        assert "tps_matched_showers_ids" in branches
        # All non-id fields of the nested ParticleRecord must be suppressed
        for suppressed in ("tps_matched_showers_wh",
                           "tps_matched_showers_sector",
                           "tps_matched_showers_station"):
            assert suppressed not in branches, \
                f"nested ParticleRecord field '{suppressed}' must not become a branch"

    def test_nested_without_idx_uses_local_index(self, nested_events):
        branches = _collect_branches(nested_events)
        # No id-like field in raw_hits — fallback is ak.local_index (== layout.at),
        # mirroring ParticleRecord._id behaviour.
        assert "tps_raw_hits_ids" in branches
        assert "tps_raw_hits_n" not in branches
        assert branches["tps_raw_hits_ids"].tolist() == [[[0, 1], [0]], [[0]]]

    def test_nested_idx_values(self, nested_events):
        branches = _collect_branches(nested_events)
        # var * var * int: per event, per tp, list of matched shower ids
        assert branches["tps_matched_showers_ids"].tolist() == [[[5], []], [[2, 7]]]

    def test_empty_events_returns_empty_dict(self):
        events = ak.Array([])
        branches = _collect_branches(events)
        assert branches == {}


# ---------------------------------------------------------------------------
# dump_to_root — eager path
# ---------------------------------------------------------------------------

class TestDumpToRootEager:
    def test_creates_file(self, simple_events, output_path):
        dump_to_root(simple_events, output_path)
        assert os.path.exists(output_path)

    def test_default_treepath(self, simple_events, output_path):
        dump_to_root(simple_events, output_path)
        with uproot.open(output_path) as f:
            assert "DTPR/TREE" in f

    def test_custom_treepath(self, simple_events, output_path):
        dump_to_root(simple_events, output_path, treepath="myDir/myTree")
        with uproot.open(output_path) as f:
            assert "myDir/myTree" in f

    def test_scalars_roundtrip(self, simple_events, output_path):
        dump_to_root(simple_events, output_path)
        with uproot.open(output_path) as f:
            assert f["DTPR/TREE"]["run"].array().tolist() == [1, 2]

    def test_jagged_roundtrip(self, simple_events, output_path):
        dump_to_root(simple_events, output_path)
        with uproot.open(output_path) as f:
            assert f["DTPR/TREE"]["digis_wh"].array().tolist() == [[-1, 2], [1]]

    def test_default_format_is_ttree(self, simple_events, output_path):
        dump_to_root(simple_events, output_path)
        with uproot.open(output_path) as f:
            assert any(v == "TTree" for v in f.classnames().values())

    def test_rntuple_format(self, simple_events, output_path):
        dump_to_root(simple_events, output_path, format="RNTuple")
        with uproot.open(output_path) as f:
            assert any(v == "ROOT::RNTuple" for v in f.classnames().values())

    def test_nested_idx_roundtrip(self, nested_events, output_path):
        # TTree: doubly-jagged ids split into flat + _n branches
        dump_to_root(nested_events, output_path)
        with uproot.open(output_path) as f:
            tree = f["DTPR/TREE"]
            assert tree["tps_matched_showers_ids"].array().tolist() == [[5], [2, 7]]
            assert tree["tps_matched_showers_ids_n"].array().tolist() == [[1, 0], [2]]

    def test_nested_count_roundtrip(self, nested_events, output_path):
        # raw_hits has no id-like field — local_index fallback, also split for TTree
        dump_to_root(nested_events, output_path)
        with uproot.open(output_path) as f:
            tree = f["DTPR/TREE"]
            assert tree["tps_raw_hits_ids"].array().tolist() == [[0, 1, 0], [0]]
            assert tree["tps_raw_hits_ids_n"].array().tolist() == [[2, 1], [1]]

    def test_overwrites_existing_file(self, simple_events, output_path):
        dump_to_root(simple_events, output_path)
        dump_to_root(simple_events, output_path)   # second write must not raise
        with uproot.open(output_path) as f:
            assert f["DTPR/TREE"]["run"].array().tolist() == [1, 2]

    def test_empty_events_raises(self, output_path):
        with pytest.raises(ValueError, match="No fields"):
            dump_to_root(ak.Array([]), output_path)


# ---------------------------------------------------------------------------
# dump_to_root — dask path
# ---------------------------------------------------------------------------

class TestDumpToRootDask:
    def test_dask_array_is_computed(self, simple_events, output_path):
        import dask_awkward as dak
        devents = dak.from_awkward(simple_events, npartitions=2)
        dump_to_root(devents, output_path)
        with uproot.open(output_path) as f:
            assert f["DTPR/TREE"]["run"].array().tolist() == [1, 2]

    def test_dask_jagged_roundtrip(self, simple_events, output_path):
        import dask_awkward as dak
        devents = dak.from_awkward(simple_events, npartitions=2)
        dump_to_root(devents, output_path)
        with uproot.open(output_path) as f:
            assert f["DTPR/TREE"]["digis_wh"].array().tolist() == [[-1, 2], [1]]


# ---------------------------------------------------------------------------
# NTuple integration test
# ---------------------------------------------------------------------------

class TestDumpToRootNTuple:
    def test_ntuple_writes_positive_event_count(self, output_path):
        from dtpr.base.ntuple import NTuple
        ntuple = NTuple(NTUPLE_FILE, maxfiles=1)
        dump_to_root(ntuple.events, output_path)
        with uproot.open(output_path) as f:
            assert f["DTPR/TREE"].num_entries > 0

    def test_ntuple_expected_branches_present(self, output_path):
        from dtpr.base.ntuple import NTuple
        ntuple = NTuple(NTUPLE_FILE, maxfiles=1)
        dump_to_root(ntuple.events, output_path)
        with uproot.open(output_path) as f:
            keys = set(f["DTPR/TREE"].keys())
        for expected in ("digis_wh", "tps_quality", "segments_wh"):
            assert expected in keys, f"Expected branch '{expected}' not found"
