"""Tests for dtpr.analysis.dumper."""

from __future__ import annotations

import os
import tempfile
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import awkward as ak

from dtpr.analysis.dumper import _dump_one_dataset, dump


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def simple_events():
    return ak.Array([
        {"run": 1, "x": 10, "digis": [{"wh": -1}, {"wh": 2}]},
        {"run": 2, "x": 20, "digis": [{"wh": 1}]},
    ])


@pytest.fixture
def outdir():
    with tempfile.TemporaryDirectory() as d:
        yield d


def _make_mock_events(partitions):
    """Build a dask_awkward-like mock with a fixed partitions list."""
    mock_events = MagicMock()
    mock_events.npartitions = len(partitions)
    mock_events.partitions.__getitem__ = lambda self, i: partitions[i]
    return mock_events


# ---------------------------------------------------------------------------
# _dump_one_dataset
# ---------------------------------------------------------------------------

class TestDumpOneDataset:

    # ── ROOT mode ─────────────────────────────────────────────────────────

    def test_root_creates_file(self, simple_events, outdir):
        import dask_awkward as dak
        devents = dak.from_awkward(simple_events, npartitions=1)
        _dump_one_dataset(
            devents, outdir, "tag", per_partition=False,
            overwrite=False, to_root=True, ncores=1, label="test",
        )
        assert os.path.exists(os.path.join(outdir, "dumpedEvents_tag.root"))

    def test_root_filename_includes_tag(self, simple_events, outdir):
        import dask_awkward as dak
        devents = dak.from_awkward(simple_events, npartitions=1)
        _dump_one_dataset(
            devents, outdir, "_v2_DY", per_partition=False,
            overwrite=False, to_root=True, ncores=1, label="DY",
        )
        assert os.path.exists(os.path.join(outdir, "dumpedEvents__v2_DY.root"))

    def test_root_per_partition_creates_multiple_files(self, simple_events, outdir):
        import dask_awkward as dak
        devents = dak.from_awkward(simple_events, npartitions=2)
        _dump_one_dataset(
            devents, outdir, "_pp", per_partition=True,
            overwrite=False, to_root=True, ncores=1, label="test",
        )
        assert os.path.exists(os.path.join(outdir, "dumpedEvents__pp_0.root"))
        assert os.path.exists(os.path.join(outdir, "dumpedEvents__pp_1.root"))

    def test_root_creates_outfolder(self, simple_events, outdir):
        import dask_awkward as dak
        nested = os.path.join(outdir, "subdir", "deep")
        devents = dak.from_awkward(simple_events, npartitions=1)
        _dump_one_dataset(
            devents, nested, "_x", per_partition=False,
            overwrite=False, to_root=True, ncores=1, label="test",
        )
        assert os.path.exists(nested)

    # ── Parquet mode ──────────────────────────────────────────────────────

    def test_parquet_creates_file(self, simple_events, outdir):
        import dask_awkward as dak
        devents = dak.from_awkward(simple_events, npartitions=1)
        _dump_one_dataset(
            devents, outdir, "_pq", per_partition=False,
            overwrite=False, to_root=False, ncores=1, label="test",
        )
        assert os.path.exists(os.path.join(outdir, "dumpedEvents__pq.parquet"))

    def test_parquet_per_partition_creates_directory(self, simple_events, outdir):
        import dask_awkward as dak
        devents = dak.from_awkward(simple_events, npartitions=2)
        _dump_one_dataset(
            devents, outdir, "_pp", per_partition=True,
            overwrite=False, to_root=False, ncores=1, label="test",
        )
        out_dir = os.path.join(outdir, "dumpedEvents__pp")
        assert os.path.isdir(out_dir)
        assert any(f.endswith(".parquet") for f in os.listdir(out_dir))

    def test_parquet_roundtrip(self, simple_events, outdir):
        import dask_awkward as dak
        devents = dak.from_awkward(simple_events, npartitions=1)
        _dump_one_dataset(
            devents, outdir, "_rt", per_partition=False,
            overwrite=False, to_root=False, ncores=1, label="test",
        )
        loaded = ak.from_parquet(os.path.join(outdir, "dumpedEvents__rt.parquet"))
        assert loaded["run"].tolist() == simple_events["run"].tolist()


# ---------------------------------------------------------------------------
# dump() — public entry point
# ---------------------------------------------------------------------------

class TestDump:

    def _mock_ntuple(self, events):
        mock = MagicMock()
        mock.events = events
        return mock

    # ── validation ────────────────────────────────────────────────────────

    def test_raises_if_neither_format_set(self, simple_events, outdir):
        with pytest.raises(ValueError, match="exactly one"):
            dump(inputs=None, outfolder=outdir, to_root=False, to_parquet=False)

    def test_raises_if_both_formats_set(self, simple_events, outdir):
        with pytest.raises(ValueError, match="exactly one"):
            dump(inputs=None, outfolder=outdir, to_root=True, to_parquet=True)

    # ── single input → ROOT ───────────────────────────────────────────────

    def test_single_input_root(self, simple_events, outdir):
        import dask_awkward as dak
        devents = dak.from_awkward(simple_events, npartitions=1)
        with (
            patch("dtpr.analysis.dumper.NTuple") as MockNTuple,
            patch("dtpr.analysis.dumper.color_msg"),
        ):
            MockNTuple.return_value.events = devents
            dump(inputs="fake.root", outfolder=outdir, tag="_run", to_root=True, ncores=1)

        assert os.path.exists(os.path.join(outdir, "dumpedEvents__run.root"))

    # ── single input → Parquet ────────────────────────────────────────────

    def test_single_input_parquet(self, simple_events, outdir):
        import dask_awkward as dak
        devents = dak.from_awkward(simple_events, npartitions=1)
        with (
            patch("dtpr.analysis.dumper.NTuple") as MockNTuple,
            patch("dtpr.analysis.dumper.color_msg"),
        ):
            MockNTuple.return_value.events = devents
            dump(inputs="fake.root", outfolder=outdir, tag="_run", to_parquet=True, ncores=1)

        assert os.path.exists(os.path.join(outdir, "dumpedEvents__run.parquet"))

    # ── multi-dataset dispatch ────────────────────────────────────────────

    def test_multi_dataset_creates_per_dataset_root_files(self, simple_events, outdir):
        import dask_awkward as dak
        devents = dak.from_awkward(simple_events, npartitions=1)
        with (
            patch("dtpr.analysis.dumper.NTuple") as MockNTuple,
            patch("dtpr.analysis.dumper.color_msg"),
        ):
            MockNTuple.return_value.events = {"DY": devents, "Zprime": devents}
            dump(outfolder=outdir, tag="_v1", to_root=True, ncores=1)

        assert os.path.exists(os.path.join(outdir, "dumpedEvents__v1_DY.root"))
        assert os.path.exists(os.path.join(outdir, "dumpedEvents__v1_Zprime.root"))

    def test_multi_dataset_creates_per_dataset_parquet_files(self, simple_events, outdir):
        import dask_awkward as dak
        devents = dak.from_awkward(simple_events, npartitions=1)
        with (
            patch("dtpr.analysis.dumper.NTuple") as MockNTuple,
            patch("dtpr.analysis.dumper.color_msg"),
        ):
            MockNTuple.return_value.events = {"DY": devents, "Zprime": devents}
            dump(outfolder=outdir, tag="_v1", to_parquet=True, ncores=1)

        assert os.path.exists(os.path.join(outdir, "dumpedEvents__v1_DY.parquet"))
        assert os.path.exists(os.path.join(outdir, "dumpedEvents__v1_Zprime.parquet"))

    # ── per-partition mode ────────────────────────────────────────────────

    def test_per_partition_root(self, simple_events, outdir):
        import dask_awkward as dak
        devents = dak.from_awkward(simple_events, npartitions=2)
        with (
            patch("dtpr.analysis.dumper.NTuple") as MockNTuple,
            patch("dtpr.analysis.dumper.color_msg"),
        ):
            MockNTuple.return_value.events = devents
            dump(outfolder=outdir, tag="_pp", to_root=True, per_partition=True, ncores=1)

        assert os.path.exists(os.path.join(outdir, "dumpedEvents__pp_0.root"))
        assert os.path.exists(os.path.join(outdir, "dumpedEvents__pp_1.root"))

    def test_per_partition_parquet(self, simple_events, outdir):
        import dask_awkward as dak
        devents = dak.from_awkward(simple_events, npartitions=2)
        with (
            patch("dtpr.analysis.dumper.NTuple") as MockNTuple,
            patch("dtpr.analysis.dumper.color_msg"),
        ):
            MockNTuple.return_value.events = devents
            dump(outfolder=outdir, tag="_pp", to_parquet=True, per_partition=True, ncores=1)

        out_dir = os.path.join(outdir, "dumpedEvents__pp")
        assert os.path.isdir(out_dir)
        assert any(f.endswith(".parquet") for f in os.listdir(out_dir))
