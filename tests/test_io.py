"""Tests for dtpr.utils.io — dump_to_root, dump_to_parquet and _collect_branches."""

import os
import tempfile

import pytest
import awkward as ak
import uproot

from dtpr.utils.io import dump_to_root, dump_to_parquet, _collect_branches

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


@pytest.fixture
def parquet_path():
    """Temporary file path for Parquet output; cleaned up after the test."""
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
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
        # mirroring ParticleRecord.id behaviour.
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

    def test_default_format_is_rntuple(self, simple_events, output_path):
        dump_to_root(simple_events, output_path)
        with uproot.open(output_path) as f:
            assert any(v == "ROOT::RNTuple" for v in f.classnames().values())

    def test_nested_idx_roundtrip(self, nested_events, output_path):
        # RNTuple: doubly-jagged ids stored natively as var * var * int
        dump_to_root(nested_events, output_path)
        with uproot.open(output_path) as f:
            assert f["DTPR/TREE"]["tps_matched_showers_ids"].array().tolist() == \
                [[[5], []], [[2, 7]]]

    def test_nested_count_roundtrip(self, nested_events, output_path):
        # raw_hits has no id-like field — local_index fallback, stored natively
        dump_to_root(nested_events, output_path)
        with uproot.open(output_path) as f:
            assert f["DTPR/TREE"]["tps_raw_hits_ids"].array().tolist() == \
                [[[0, 1], [0]], [[0]]]

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
# dump_to_parquet
# ---------------------------------------------------------------------------

class TestDumpToParquet:
    def test_creates_file(self, simple_events, parquet_path):
        dump_to_parquet(simple_events, parquet_path)
        assert os.path.exists(parquet_path)

    def test_scalar_roundtrip(self, simple_events, parquet_path):
        dump_to_parquet(simple_events, parquet_path)
        loaded = ak.from_parquet(parquet_path)
        assert loaded["run"].tolist() == [1, 2]
        assert loaded["lumi"].tolist() == [1, 2]

    def test_flat_collection_roundtrip(self, simple_events, parquet_path):
        dump_to_parquet(simple_events, parquet_path)
        loaded = ak.from_parquet(parquet_path)
        assert loaded["digis"]["wh"].tolist() == [[-1, 2], [1]]

    def test_nested_collection_preserved(self, nested_events, parquet_path):
        # Unlike dump_to_root, nested structure is NOT flattened
        dump_to_parquet(nested_events, parquet_path)
        loaded = ak.from_parquet(parquet_path)
        # The full nested record (incl. wh, sector, station) must survive
        assert "wh" in ak.fields(loaded["tps"]["matched_showers"])
        assert loaded["tps"]["matched_showers"]["idx"].tolist() == [[[5], []], [[2, 7]]]

    def test_dask_array_is_computed(self, simple_events, parquet_path):
        import dask_awkward as dak
        devents = dak.from_awkward(simple_events, npartitions=2)
        dump_to_parquet(devents, parquet_path)
        loaded = ak.from_parquet(parquet_path)
        assert loaded["run"].tolist() == [1, 2]

    def test_empty_events_raises(self, parquet_path):
        with pytest.raises(ValueError, match="No fields"):
            dump_to_parquet(ak.Array([]), parquet_path)


# ---------------------------------------------------------------------------
# reconstruct_nested_ids preprocessor
# ---------------------------------------------------------------------------

class TestReconstructNestedIds:
    """Tests for the reconstruct_nested_ids factory in dtpr.utils.preprocessors.

    In a TTree (produced by the old dump_to_root TTree path or by external tools
    using the flat+count convention), a ``var * var * int`` field is encoded as:
      - ``events["tps_matched_showers_ids"]`` — flat ids per event  (``var * int``)
      - ``events["tps_matched_showers_ids_n"]`` — count per *parent* TP (``var * int``)
    The reconstructor unfolds these back to ``var * var * int`` and injects
    the result into the ``tps`` collection.
    """

    @pytest.fixture
    def flat_events(self):
        """Events with top-level flat ids + count branches (TTree readback convention)."""
        return ak.Array([
            {
                "run": 1,
                "tps": [{"quality": 2}, {"quality": 3}],
                # flat ids per event (tp[0] contributed 1, tp[1] contributed 0)
                "tps_matched_showers_ids":   [5],
                "tps_matched_showers_ids_n": [1, 0],
            },
            {
                "run": 2,
                "tps": [{"quality": 1}],
                # flat ids per event (tp[0] contributed 2 ids)
                "tps_matched_showers_ids":   [2, 7],
                "tps_matched_showers_ids_n": [2],
            },
        ])

    def test_returns_callable(self):
        from dtpr.utils.preprocessors import reconstruct_nested_ids
        pp = reconstruct_nested_ids("tps_matched_showers_ids",
                                    "tps_matched_showers_ids_n",
                                    "tps")
        assert callable(pp)

    def test_name_set_on_callable(self):
        from dtpr.utils.preprocessors import reconstruct_nested_ids
        pp = reconstruct_nested_ids("tps_matched_showers_ids",
                                    "tps_matched_showers_ids_n",
                                    "tps")
        assert "tps" in pp.__name__

    def test_reconstructs_nested_ids(self, flat_events):
        from dtpr.utils.preprocessors import reconstruct_nested_ids
        pp = reconstruct_nested_ids("tps_matched_showers_ids",
                                    "tps_matched_showers_ids_n",
                                    "tps")
        pp(flat_events)
        result = flat_events["tps"]["tps_matched_showers"].tolist()
        assert result == [[[5], []], [[2, 7]]]

    def test_custom_out_field(self, flat_events):
        """out_field kwarg gives the new nested field a different name."""
        from dtpr.utils.preprocessors import reconstruct_nested_ids
        pp = reconstruct_nested_ids("tps_matched_showers_ids",
                                    "tps_matched_showers_ids_n",
                                    "tps",
                                    out_field="shower_ids_nested")
        pp(flat_events)
        assert flat_events["tps"]["shower_ids_nested"].tolist() == [[[5], []], [[2, 7]]]


# ---------------------------------------------------------------------------
# NTuple parquet round-trip
# ---------------------------------------------------------------------------

class TestNTupleParquet:
    """Tests for NTuple loading parquet files (bypasses NanoEventsFactory)."""

    @pytest.fixture
    def parquet_roundtrip(self, simple_events, parquet_path):
        """Write simple_events to parquet and return (path, original_events)."""
        dump_to_parquet(simple_events, parquet_path)
        return parquet_path, simple_events

    def test_ntuple_reads_parquet(self, parquet_roundtrip):
        import types
        from dtpr.base.ntuple import NTuple
        parquet_file, original = parquet_roundtrip
        # Minimal config: no schema, no pre-steps
        cfg = types.SimpleNamespace(ntuple_tree_name="DTPR/TREE", Schema=None, **{"pre-steps": None})
        ntuple = NTuple(parquet_file, CONFIG=cfg)
        assert hasattr(ntuple, "events")

    def test_ntuple_parquet_fields_preserved(self, parquet_roundtrip):
        import types
        from dtpr.base.ntuple import NTuple
        parquet_file, original = parquet_roundtrip
        cfg = types.SimpleNamespace(ntuple_tree_name="DTPR/TREE", Schema=None, **{"pre-steps": None})
        ntuple = NTuple(parquet_file, CONFIG=cfg)
        events = ntuple.events.compute()
        assert events["run"].tolist() == original["run"].tolist()
        assert events["digis"]["wh"].tolist() == original["digis"]["wh"].tolist()


# ---------------------------------------------------------------------------
# NTuple integration test (ROOT)
# ---------------------------------------------------------------------------

class TestDumpToRootNTuple:
    def test_ntuple_writes_positive_event_count(self, output_path):
        from dtpr.base.ntuple import NTuple
        ntuple = NTuple(NTUPLE_FILE, tree_name="dtNtupleProducer/DTTREE", maxfiles=1)
        dump_to_root(ntuple.events, output_path)
        with uproot.open(output_path) as f:
            assert f["DTPR/TREE"].num_entries > 0

    def test_ntuple_expected_branches_present(self, output_path):
        from dtpr.base.ntuple import NTuple
        ntuple = NTuple(NTUPLE_FILE, tree_name="dtNtupleProducer/DTTREE", maxfiles=1)
        dump_to_root(ntuple.events, output_path)
        with uproot.open(output_path) as f:
            keys = set(f["DTPR/TREE"].keys())
        for expected in ("digis_wh", "tps_quality", "segments_wh"):
            assert expected in keys, f"Expected branch '{expected}' not found"


# ---------------------------------------------------------------------------
# _resolve_fileset_input
# ---------------------------------------------------------------------------

NTUPLE_FILE2 = os.path.abspath(
    os.path.join(os.path.dirname(__file__),
                 "ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_110.root")
)


def _make_cfg(filesets=None, tree="DTPR/TREE"):
    """Return a minimal SimpleNamespace config with optional filesets."""
    import types
    return types.SimpleNamespace(
        ntuple_tree_name=tree,
        Schema=None,
        filesets=filesets,
        **{"pre-steps": None},
    )


# ---------------------------------------------------------------------------
# NTuple: fileset-driven loading
# ---------------------------------------------------------------------------

class TestNTupleFileset:
    """Integration tests for fileset-based NTuple loading."""

    def test_ntuple_from_config_fileset(self):
        """NTuple with no inputFolder reads from config filesets; events is a dict."""
        from dtpr.base.ntuple import NTuple
        treepath = "dtNtupleProducer/DTTREE"
        cfg = _make_cfg(filesets={
            "TestSample": {
                "files": {NTUPLE_FILE: treepath},
            }
        }, tree=treepath)
        ntuple = NTuple(CONFIG=cfg)
        assert isinstance(ntuple.events, dict)
        assert "TestSample" in ntuple.events
        assert ntuple.events["TestSample"].npartitions >= 1

    def test_ntuple_metadata_attached(self):
        """Metadata from fileset is stored per-dataset in ntuple.metadata."""
        from dtpr.base.ntuple import NTuple
        treepath = "dtNtupleProducer/DTTREE"
        cfg = _make_cfg(filesets={
            "TestSample": {
                "files": {NTUPLE_FILE: treepath},
                "metadata": {"year": 2026, "is_mc": False},
            }
        }, tree=treepath)
        ntuple = NTuple(CONFIG=cfg)
        assert ntuple.metadata == {"TestSample": {"year": 2026, "is_mc": False}}

    def test_ntuple_step_size_increases_partitions(self):
        """step_size=500 should produce more partitions than no step_size (file >500 entries)."""
        from dtpr.base.ntuple import NTuple
        treepath = "dtNtupleProducer/DTTREE"
        cfg = _make_cfg(tree=treepath)

        # Use file:tree embedding so explicit-input mode works without tree_name param
        ntuple_plain   = NTuple(f"{NTUPLE_FILE2}:{treepath}", CONFIG=cfg)
        ntuple_chunked = NTuple(f"{NTUPLE_FILE2}:{treepath}", step_size=500, CONFIG=cfg)

        assert ntuple_chunked.events.npartitions > ntuple_plain.events.npartitions

    def test_ntuple_step_size_from_fileset_yaml(self):
        """step_size defined inside filesets block is respected; events is a dict."""
        from dtpr.base.ntuple import NTuple
        treepath = "dtNtupleProducer/DTTREE"
        cfg = _make_cfg(filesets={
            "ds": {
                "files": {NTUPLE_FILE2: treepath},
                "step_size": 500,
            }
        }, tree=treepath)
        ntuple = NTuple(CONFIG=cfg)
        assert isinstance(ntuple.events, dict)
        assert ntuple.events["ds"].npartitions > 1

    def test_ntuple_constructor_step_size_overrides_fileset(self):
        """Constructor step_size wins over fileset step_size."""
        from dtpr.base.ntuple import NTuple
        treepath = "dtNtupleProducer/DTTREE"
        cfg = _make_cfg(filesets={
            "ds": {
                "files": {NTUPLE_FILE2: treepath},
                "step_size": 200,   # would give many partitions
            }
        }, tree=treepath)
        ntuple_yaml     = NTuple(CONFIG=cfg)
        ntuple_override = NTuple(CONFIG=cfg, step_size=800)
        # 800-entry chunks should yield fewer partitions than 200-entry chunks
        assert ntuple_override.events["ds"].npartitions <= ntuple_yaml.events["ds"].npartitions
