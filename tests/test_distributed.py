"""Distributed-client tests using a local dask cluster.

Spins up a ``dask.distributed.LocalCluster`` (in-process) so the full
distributed code path is exercised without any external infrastructure.

Covered
-------
- :func:`~dtpr.utils.functions.make_dask_sched_kwargs`:
  returns ``{}`` when a client is active, regardless of *ncores*.
- :func:`~dtpr.analysis.fill_histograms._fill_one_dataset`:
  completes correctly when a distributed client is present.
- :func:`~dtpr.analysis.dumper._dump_one_dataset`:
  completes (ROOT and Parquet) when a distributed client is present.
"""

from __future__ import annotations

import os
import tempfile
from unittest.mock import patch

import pytest
import awkward as ak
import dask_awkward as dak


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def distributed_client():
    """Start a LocalCluster + Client for the test module, tear down after."""
    distributed = pytest.importorskip("dask.distributed")
    with distributed.LocalCluster(
        n_workers=2,
        threads_per_worker=1,
        processes=False,   # in-process threads — faster, avoids pickling issues
    ) as cluster:
        with distributed.Client(cluster) as client:
            yield client


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


# ---------------------------------------------------------------------------
# make_dask_sched_kwargs — distributed path
# ---------------------------------------------------------------------------

class TestMakeDaskSchedKwargsDistributed:
    def test_ncores_gt1_returns_empty_when_client_active(self, distributed_client):
        """With an active client, ncores > 1 must defer to the cluster (returns {})."""
        from dtpr.utils.functions import make_dask_sched_kwargs
        result = make_dask_sched_kwargs(ncores=4)
        assert result == {}, (
            "Expected {} (distributed) but got local-processes kwargs — "
            "make_dask_sched_kwargs did not detect the active client."
        )

    def test_ncores_minus1_returns_empty_when_client_active(self, distributed_client):
        """ncores=-1 always defers; with a client, result is still {}."""
        from dtpr.utils.functions import make_dask_sched_kwargs
        assert make_dask_sched_kwargs(ncores=-1) == {}

    def test_ncores_1_forces_synchronous_even_with_client(self, distributed_client):
        """ncores=1 always returns synchronous — explicit debug override."""
        from dtpr.utils.functions import make_dask_sched_kwargs
        result = make_dask_sched_kwargs(ncores=1)
        assert result == {"scheduler": "synchronous"}


# ---------------------------------------------------------------------------
# _fill_one_dataset — distributed execution
# ---------------------------------------------------------------------------

_noop = lambda *a, **kw: None  # picklable stub — MagicMock can't survive cloudpickle in dask workers


class TestFillOneDatasetDistributed:
    def test_in_memory_mode_completes(self, distributed_client, simple_events, outdir):
        """fill_histograms completes correctly with an active distributed client."""
        import hist
        from dtpr.utils.histograms_base import Distribution
        from dtpr.analysis.fill_histograms import _fill_one_dataset

        histo = Distribution(
            name="dist_x",
            axis=hist.axis.Regular(10, 0, 30, label="x"),
            func=lambda events: events["x"],
        )
        devents = dak.from_awkward(simple_events, npartitions=2)
        with patch("dtpr.analysis.fill_histograms.color_msg", new=_noop):
            _fill_one_dataset(
                devents, [histo], outdir, "_dist",
                per_partition=False, overwrite=False, ncores=2, label="test",
            )

        root_path = os.path.join(outdir, "histograms", "histograms_dist.root")
        assert os.path.exists(root_path), "Expected merged ROOT output file."

    def test_per_partition_mode_completes(self, distributed_client, simple_events, outdir):
        """Per-partition fill completes and writes partition files."""
        import hist
        from dtpr.utils.histograms_base import Distribution
        from dtpr.analysis.fill_histograms import _fill_one_dataset

        histo = Distribution(
            name="dist_x",
            axis=hist.axis.Regular(10, 0, 30, label="x"),
            func=lambda events: events["x"],
        )
        devents = dak.from_awkward(simple_events, npartitions=2)
        with patch("dtpr.analysis.fill_histograms.color_msg", new=_noop):
            _fill_one_dataset(
                devents, [histo], outdir, "_pp",
                per_partition=True, overwrite=False, ncores=2, label="test",
            )

        out_dir = os.path.join(outdir, "histograms")
        assert os.path.exists(os.path.join(out_dir, "histograms_pp_0.root"))
        assert os.path.exists(os.path.join(out_dir, "histograms_pp_1.root"))

    def test_histogram_values_correct(self, distributed_client, simple_events, outdir):
        """Distributed reduce gives the same counts as synchronous execution."""
        import hist
        import uproot
        from dtpr.utils.histograms_base import Distribution
        from dtpr.analysis.fill_histograms import _fill_one_dataset

        histo = Distribution(
            name="dist_x",
            axis=hist.axis.Regular(10, 0, 30, label="x"),
            func=lambda events: events["x"],
        )
        devents = dak.from_awkward(simple_events, npartitions=2)
        with patch("dtpr.analysis.fill_histograms.color_msg", new=_noop):
            _fill_one_dataset(
                devents, [histo], outdir, "_val",
                per_partition=False, overwrite=False, ncores=2, label="test",
            )

        root_path = os.path.join(outdir, "histograms", "histograms_val.root")
        with uproot.open(root_path) as f:
            total = f["dist_x"].values().sum()
        assert total == len(simple_events)


# ---------------------------------------------------------------------------
# _dump_one_dataset — distributed execution
# ---------------------------------------------------------------------------

class TestDumpOneDatasetDistributed:
    def test_root_dump_completes(self, distributed_client, simple_events, outdir):
        """ROOT dump completes with a distributed client active."""
        from dtpr.analysis.dumper import _dump_one_dataset

        devents = dak.from_awkward(simple_events, npartitions=2)
        with patch("dtpr.analysis.dumper.color_msg"):
            _dump_one_dataset(
                devents, outdir, "_dist", per_partition=False,
                overwrite=False, to_root=True, ncores=2, label="test",
            )
        assert os.path.exists(os.path.join(outdir, "dumpedEvents__dist.root"))

    def test_parquet_dump_completes(self, distributed_client, simple_events, outdir):
        """Parquet dump completes with a distributed client active."""
        from dtpr.analysis.dumper import _dump_one_dataset

        devents = dak.from_awkward(simple_events, npartitions=2)
        with patch("dtpr.analysis.dumper.color_msg"):
            _dump_one_dataset(
                devents, outdir, "_dist", per_partition=False,
                overwrite=False, to_root=False, ncores=2, label="test",
            )
        assert os.path.exists(os.path.join(outdir, "dumpedEvents__dist.parquet"))

    def test_per_partition_root_dump_completes(self, distributed_client, simple_events, outdir):
        """Per-partition ROOT dump creates all partition files via the cluster."""
        from dtpr.analysis.dumper import _dump_one_dataset

        devents = dak.from_awkward(simple_events, npartitions=2)
        with patch("dtpr.analysis.dumper.color_msg"):
            _dump_one_dataset(
                devents, outdir, "_pp", per_partition=True,
                overwrite=False, to_root=True, ncores=2, label="test",
            )
        assert os.path.exists(os.path.join(outdir, "dumpedEvents__pp_0.root"))
        assert os.path.exists(os.path.join(outdir, "dumpedEvents__pp_1.root"))
