"""Tests for dtpr.analysis.fill_histograms."""

from __future__ import annotations

import os
import sys
import types
import tempfile
import warnings
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, PropertyMock

import pytest
import awkward as ak
import hist

from dtpr.utils.histograms_base import Distribution, Efficiency, HistogramBase
from dtpr.analysis.fill_histograms import (
    _show_histo_names,
    load_histos_from_config,
    fill_partition,
    _reduce_and_save,
    _fill_one_dataset,
    fill_histos,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def simple_axis():
    return hist.axis.Regular(10, 0, 10, label="x")


@pytest.fixture
def dist_histo(simple_axis):
    return Distribution(
        name="test_dist",
        axis=simple_axis,
        func=lambda events: events["x"],
    )


@pytest.fixture
def eff_histo(simple_axis):
    return Efficiency(
        name="test_eff",
        axis=simple_axis,
        func=lambda events: (events["x"], events["x"] > 3),
    )


@pytest.fixture
def simple_events():
    return ak.Array([{"x": 1}, {"x": 5}, {"x": 8}])


@pytest.fixture
def outdir():
    with tempfile.TemporaryDirectory() as d:
        yield d


# ---------------------------------------------------------------------------
# _show_histo_names
# ---------------------------------------------------------------------------

class TestShowHistoNames:
    def test_few_histos_joined(self, dist_histo, eff_histo):
        histos = [dist_histo, eff_histo]
        with patch("dtpr.analysis.fill_histograms.color_msg") as mock_msg:
            _show_histo_names(histos)
        called_msg = mock_msg.call_args[0][0]
        assert "test_dist" in called_msg
        assert "test_eff" in called_msg

    def test_many_histos_truncated(self, simple_axis):
        histos = [
            Distribution(name=f"h{i}", axis=simple_axis, func=lambda e: e["x"])
            for i in range(10)
        ]
        with patch("dtpr.analysis.fill_histograms.color_msg") as mock_msg:
            _show_histo_names(histos, limit=6)
        called_msg = mock_msg.call_args[0][0]
        assert "more" in called_msg

    def test_exactly_limit_no_truncation(self, simple_axis):
        histos = [
            Distribution(name=f"h{i}", axis=simple_axis, func=lambda e: e["x"])
            for i in range(6)
        ]
        with patch("dtpr.analysis.fill_histograms.color_msg") as mock_msg:
            _show_histo_names(histos, limit=6)
        called_msg = mock_msg.call_args[0][0]
        assert "more" not in called_msg


# ---------------------------------------------------------------------------
# load_histos_from_config
# ---------------------------------------------------------------------------

class TestLoadHistosFromConfig:
    def _make_config(self, sources=(), names=None):
        """Build a minimal mock config with histo_sources / histo_names."""
        return SimpleNamespace(
            histo_sources=list(sources),
            histo_names=list(names) if names is not None else [],
        )

    def test_returns_histos_from_source(self, dist_histo):
        fake_module = types.ModuleType("fake_source")
        fake_module.histos = [dist_histo]
        cfg = self._make_config(sources=["fake_source"])

        with patch("importlib.import_module", return_value=fake_module):
            result = load_histos_from_config(config=cfg)

        assert len(result) == 1
        assert result[0].name == "test_dist"

    def test_single_histobase_wrapped_in_list(self, dist_histo):
        fake_module = types.ModuleType("fake_source")
        fake_module.histos = dist_histo  # bare instance, not a list
        cfg = self._make_config(sources=["fake_source"])

        with patch("importlib.import_module", return_value=fake_module):
            result = load_histos_from_config(config=cfg)

        assert len(result) == 1

    def test_invalid_histos_attribute_warns_and_skips(self):
        fake_module = types.ModuleType("bad_source")
        fake_module.histos = "not_a_histogram"
        cfg = self._make_config(sources=["bad_source"])

        with patch("importlib.import_module", return_value=fake_module):
            with pytest.warns(UserWarning, match="bad_source"):
                result = load_histos_from_config(config=cfg)

        assert result == []

    def test_list_with_invalid_elements_warns_and_skips(self):
        fake_module = types.ModuleType("bad_source")
        fake_module.histos = ["not_a_histogram"]
        cfg = self._make_config(sources=["bad_source"])

        with patch("importlib.import_module", return_value=fake_module):
            with pytest.warns(UserWarning, match="bad_source"):
                result = load_histos_from_config(config=cfg)

        assert result == []

    def test_names_filter_keeps_matching(self, dist_histo, eff_histo):
        fake_module = types.ModuleType("fake_source")
        fake_module.histos = [dist_histo, eff_histo]
        cfg = self._make_config(sources=["fake_source"], names=["test_dist"])

        with patch("importlib.import_module", return_value=fake_module):
            result = load_histos_from_config(config=cfg)

        assert len(result) == 1
        assert result[0].name == "test_dist"

    def test_names_filter_missing_warns(self, dist_histo):
        fake_module = types.ModuleType("fake_source")
        fake_module.histos = [dist_histo]
        cfg = self._make_config(sources=["fake_source"], names=["missing_histo"])

        with patch("importlib.import_module", return_value=fake_module):
            with pytest.warns(UserWarning, match="missing_histo"):
                result = load_histos_from_config(config=cfg)

        assert result == []

    def test_no_sources_returns_empty(self):
        cfg = self._make_config(sources=[])
        result = load_histos_from_config(config=cfg)
        assert result == []

    def test_multiple_sources_merged(self, simple_axis):
        mod_a = types.ModuleType("src_a")
        mod_a.histos = [Distribution("ha", simple_axis, lambda e: e["x"])]
        mod_b = types.ModuleType("src_b")
        mod_b.histos = [Distribution("hb", simple_axis, lambda e: e["x"])]
        cfg = self._make_config(sources=["src_a", "src_b"])

        def fake_import(name):
            return {"src_a": mod_a, "src_b": mod_b}[name]

        with patch("importlib.import_module", side_effect=fake_import):
            result = load_histos_from_config(config=cfg)

        assert {h.name for h in result} == {"ha", "hb"}


# ---------------------------------------------------------------------------
# fill_partition
# ---------------------------------------------------------------------------

class TestFillPartition:
    def test_fills_and_returns_clones(self, dist_histo, simple_events):
        delayed = fill_partition(simple_events, [dist_histo])
        result = delayed.compute()

        assert result is not None
        assert len(result) == 1
        assert isinstance(result[0], Distribution)
        assert result[0].h.sum() == len(simple_events)

    def test_skips_existing_output(self, dist_histo, simple_events, outdir):
        out_path = os.path.join(outdir, "existing.root")
        open(out_path, "w").close()  # create empty file

        delayed = fill_partition(simple_events, [dist_histo], out_path=out_path, overwrite=False)
        result = delayed.compute()

        assert result is None  # skipped

    def test_overwrites_existing_output(self, dist_histo, simple_events, outdir):
        out_path = os.path.join(outdir, "histograms_0.root")
        open(out_path, "w").close()  # pre-existing file

        delayed = fill_partition(simple_events, [dist_histo], out_path=out_path, overwrite=True)
        result = delayed.compute()

        assert result is None  # written, not returned
        assert os.path.getsize(out_path) > 0

    def test_writes_root_file_in_per_partition_mode(self, dist_histo, simple_events, outdir):
        out_path = os.path.join(outdir, "histograms_0.root")
        delayed = fill_partition(simple_events, [dist_histo], out_path=out_path)
        result = delayed.compute()

        assert result is None
        assert os.path.exists(out_path)

    def test_fill_error_warns_but_continues(self, simple_events, simple_axis, outdir):
        bad_histo = Distribution(
            name="bad",
            axis=simple_axis,
            func=lambda events: 1 / 0,  # always raises
        )
        delayed = fill_partition(simple_events, [bad_histo])
        with pytest.warns(UserWarning, match="bad"):
            result = delayed.compute()

        assert result is not None  # returns even if fill failed

    def test_efficiency_histogram_filled(self, eff_histo, simple_events):
        delayed = fill_partition(simple_events, [eff_histo])
        result = delayed.compute()

        assert result is not None
        assert result[0].den.sum() == len(simple_events)
        assert result[0].num.sum() == sum(1 for v in [1, 5, 8] if v > 3)


# ---------------------------------------------------------------------------
# _reduce_and_save
# ---------------------------------------------------------------------------

class TestReduceAndSave:
    def test_reduces_and_writes_root(self, dist_histo, simple_events, outdir):
        clone_a = dist_histo.empty_clone()
        clone_a.fill(simple_events)
        clone_b = dist_histo.empty_clone()
        clone_b.fill(simple_events)

        _reduce_and_save([[clone_a], [clone_b]], outdir, "_test")

        root_path = os.path.join(outdir, "histograms", "histograms_test.root")
        assert os.path.exists(root_path)

    def test_histogram_counts_summed(self, dist_histo, simple_events, outdir):
        import uproot

        clone_a = dist_histo.empty_clone()
        clone_a.fill(simple_events)
        clone_b = dist_histo.empty_clone()
        clone_b.fill(simple_events)

        _reduce_and_save([[clone_a], [clone_b]], outdir, "_tag")

        root_path = os.path.join(outdir, "histograms", "histograms_tag.root")
        with uproot.open(root_path) as f:
            h = f["test_dist"]
        assert h.values().sum() == 2 * len(simple_events)


# ---------------------------------------------------------------------------
# _fill_one_dataset
# ---------------------------------------------------------------------------

class TestFillOneDataset:
    def _make_mock_events(self, partitions):
        """Build a dask_awkward-like mock with pre-set partitions."""
        mock_events = MagicMock()
        mock_events.npartitions = len(partitions)
        mock_events.partitions.__getitem__ = lambda self, i: partitions[i]
        return mock_events

    def test_in_memory_mode_writes_single_root(self, dist_histo, simple_events, outdir):
        mock_events = self._make_mock_events([simple_events])
        _fill_one_dataset(
            mock_events, [dist_histo], outdir, "_test",
            per_partition=False, overwrite=False, ncores=1, label="test",
        )
        root_path = os.path.join(outdir, "histograms", "histograms_test.root")
        assert os.path.exists(root_path)

    def test_per_partition_mode_writes_partition_files(self, dist_histo, simple_events, outdir):
        mock_events = self._make_mock_events([simple_events, simple_events])
        _fill_one_dataset(
            mock_events, [dist_histo], outdir, "_test",
            per_partition=True, overwrite=False, ncores=1, label="test",
        )
        assert os.path.exists(os.path.join(outdir, "histograms", "histograms_test_0.root"))
        assert os.path.exists(os.path.join(outdir, "histograms", "histograms_test_1.root"))

    def test_partition_index_zero_padded(self, dist_histo, simple_events, outdir):
        mock_events = self._make_mock_events([simple_events] * 10)
        _fill_one_dataset(
            mock_events, [dist_histo], outdir, "_pad",
            per_partition=True, overwrite=False, ncores=1, label="pad",
        )
        # With 10 partitions, pad width = len("9") = 1 → "0" .. "9"
        assert os.path.exists(os.path.join(outdir, "histograms", "histograms_pad_0.root"))
        assert os.path.exists(os.path.join(outdir, "histograms", "histograms_pad_9.root"))


# ---------------------------------------------------------------------------
# fill_histos — public entry point
# ---------------------------------------------------------------------------

class TestFillHistos:
    def _make_mock_events(self, partition):
        mock_events = MagicMock()
        mock_events.npartitions = 1
        mock_events.partitions.__getitem__ = lambda self, i: partition
        return mock_events

    def test_no_histos_exits_early(self, outdir, simple_events):
        with (
            patch("dtpr.analysis.fill_histograms.NTuple") as MockNTuple,
            patch("dtpr.analysis.fill_histograms.load_histos_from_config", return_value=[]),
            patch("dtpr.analysis.fill_histograms.color_msg") as mock_msg,
        ):
            MockNTuple.return_value.events = self._make_mock_events(simple_events)
            fill_histos(outfolder=outdir)

        messages = [call[0][0] for call in mock_msg.call_args_list]
        assert any("No histograms" in m for m in messages)

    def test_single_input_runs_to_completion(self, dist_histo, simple_events, outdir):
        with (
            patch("dtpr.analysis.fill_histograms.NTuple") as MockNTuple,
            patch("dtpr.analysis.fill_histograms.load_histos_from_config", return_value=[dist_histo]),
            patch("dtpr.analysis.fill_histograms.color_msg"),
        ):
            MockNTuple.return_value.events = self._make_mock_events(simple_events)
            fill_histos(outfolder=outdir, tag="_run", ncores=1)

        assert os.path.exists(os.path.join(outdir, "histograms", "histograms_run.root"))

    def test_multi_dataset_writes_per_dataset_files(self, dist_histo, simple_events, outdir):
        with (
            patch("dtpr.analysis.fill_histograms.NTuple") as MockNTuple,
            patch("dtpr.analysis.fill_histograms.load_histos_from_config", return_value=[dist_histo]),
            patch("dtpr.analysis.fill_histograms.color_msg"),
        ):
            MockNTuple.return_value.events = {
                "DY": self._make_mock_events(simple_events),
                "Zprime": self._make_mock_events(simple_events),
            }
            fill_histos(outfolder=outdir, tag="_v1", ncores=1)

        assert os.path.exists(os.path.join(outdir, "histograms", "histograms_v1_DY.root"))
        assert os.path.exists(os.path.join(outdir, "histograms", "histograms_v1_Zprime.root"))

    def test_per_partition_mode(self, dist_histo, simple_events, outdir):
        with (
            patch("dtpr.analysis.fill_histograms.NTuple") as MockNTuple,
            patch("dtpr.analysis.fill_histograms.load_histos_from_config", return_value=[dist_histo]),
            patch("dtpr.analysis.fill_histograms.color_msg"),
        ):
            MockNTuple.return_value.events = self._make_mock_events(simple_events)
            fill_histos(outfolder=outdir, tag="_pp", ncores=1, per_partition=True)

        assert os.path.exists(os.path.join(outdir, "histograms", "histograms_pp_0.root"))
