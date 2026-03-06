"""Columnar histogram filling — the engine behind ``dtpr fill-histos``.

Execution modes
---------------

**In-memory map-reduce** (default, no ``--per-partition``)
    Each dask partition is materialised, histograms are filled, and the
    results are summed (``h1 + h2``) in the main process.  A single ROOT
    file is written at the end.  Suitable for most jobs.

**Per-partition output** (``--per-partition`` flag)
    Each partition is processed independently and its histograms are
    written to an individual ROOT file
    (``<outfolder>/histograms/histograms{tag}_NNNN.root``).
    Existing output files are *skipped* — the job is **resume-safe** after
    a failure.  Use ``dtpr merge-histos`` to merge the per-partition ROOT
    files into a single file. Use --force-overwrite to re-process and overwrite existing per-partition files.

Multiple datasets
-----------------
When ``datasets`` is specified (or when ``inputs`` is omitted and the config
defines ``filesets:``), each dataset is processed independently and its
histograms are saved to a separate ROOT file:

    histograms{tag}_{dataset_name}.root

Per-partition outputs follow the same convention:

    histograms{tag}_{dataset_name}_NNNN.root

Parallelism
-----------
Controlled by ``ncores`` (identical semantics to ``dtpr dump``):

- ``ncores == 1``   → always synchronous (single-threaded, easiest to debug),
  even when a distributed client is active.
- ``ncores == -1``  → dask default (threaded); OK for I/O-bound work.
- ``ncores > 1``    → local ``"processes"`` scheduler with *ncores* workers,
  **unless** a distributed client is active — in that case tasks go to the
  cluster and ``ncores`` is ignored (except ``ncores == 1``).

A ``dask.distributed.Client`` is activated transparently at the CLI level
via ``--scheduler-address``; no per-command changes are needed.
"""

from __future__ import annotations

import os
import warnings
from functools import reduce

import dask
from ..base import NTuple
from ..base.config import RUN_CONFIG
from importlib import import_module

from ..utils.functions import color_msg, create_outfolder, make_dask_sched_kwargs
from ..utils.histograms_base import HistogramBase, save_to_root


# ---------------------------------------------------------------------------
# Utility / helpers
# ---------------------------------------------------------------------------

def _show_histo_names(histos: list[HistogramBase], limit: int = 6) -> None:
    names = [h.name for h in histos]
    msg = (
        f"{', '.join(names[:limit])} and {len(names) - limit} more..."
        if len(names) > limit else ", ".join(names)
    )
    color_msg(f"Histograms to fill: {msg}", color="yellow", indentLevel=0)


def load_histos_from_config(config=None) -> list[HistogramBase]:
    """Load and filter histogram instances from the configured sources.

    Reads ``histo_sources`` and ``histo_names`` from histograms map in *config*  (defaults to
    :data:`~dtpr.base.config.RUN_CONFIG`).

    Returns
    -------
    list[HistogramBase]
        Histogram instances ready to be filled.

    Warnings
    --------
    - If a source module does not have a valid ``histos`` attribute.
    - If any of the specified histogram names are not found in any source.
    """
    cfg = config or RUN_CONFIG
    # Support both a nested ``histograms:`` config section and top-level attributes
    # (backward-compatible fallback).
    h_cfg = getattr(cfg, "histograms", None) or cfg

    if isinstance(h_cfg, dict):
        sources = h_cfg.get("histo_sources")
        names_filter = set(h_cfg.get("histo_names"))
    else:
        sources = getattr(h_cfg, "histo_sources", None)
        names_filter = set(getattr(h_cfg, "histo_names", None))

    all_histos: list[HistogramBase] = []
    for source in sources:
        module = import_module(source)
        module_histos = getattr(module, "histos", [])
        if isinstance(module_histos, HistogramBase):
            module_histos = [module_histos]
        elif not isinstance(module_histos, list):
            warnings.warn(
                f"Source module {source!r} has no 'histos' attribute or it is not a list or a HistogramBase instance. Skipping.",
                stacklevel=2,
            )
            continue
        if any(not isinstance(h, HistogramBase) for h in module_histos):
            warnings.warn(
                f"Source module {source!r} has an invalid 'histos' attribute. Skipping.",
                stacklevel=2,
            )
            continue
        all_histos.extend(module_histos)

    if names_filter:
        all_histos = [h for h in all_histos if h.name in names_filter]
        found = {h.name for h in all_histos}
        missing = names_filter - found
        if missing:
            warnings.warn(
                f"The following histograms were not found in any source: "
                f"{', '.join(sorted(missing))}",
                stacklevel=2,
            )

    return all_histos


def _reduce_and_save(
    all_results: list[list[HistogramBase]], outfolder: str, tag: str
) -> None:
    """Sum histograms across partitions and write ROOT output."""
    nhist = len(all_results[0])
    final_histos = [
        reduce(lambda a, b: a + b, [part[i] for part in all_results])
        for i in range(nhist)
    ]
    outpath = os.path.join(outfolder, "histograms")
    create_outfolder(outpath)
    root_path = os.path.join(outpath, f"histograms{tag}.root")
    save_to_root(final_histos, root_path)
    color_msg(f"Histograms saved → {root_path}", color="green", indentLevel=1)

# ---------------------------------------------------------------------------
# Partition worker  (runs inside the dask worker / thread)
# ---------------------------------------------------------------------------

@dask.delayed
def fill_partition(partition, histos_template, out_path=None, overwrite=False, label="") -> list[HistogramBase] | None:
    """
    Fill histograms for a single partition (already materialized as ak.Array).

    Parameters
    ----------
    partition : ak.Array
        Partition data. Note: when a dask_awkward.Array is passed to a @dask.delayed function,
        dask materializes it automatically before calling the function, so this is already a plain ak.Array.
    histos_template : list[HistogramBase]
        Template histograms. Each worker gets its own clones via :meth:`HistogramBase.empty_clone`.
    out_path : str
        **Per-partition mode**: write filled histograms to this ROOT file and return ``None``.
        If the file already exists it is skipped (resume-safe).
        **In-memory mode** (`""`): return the filled clones so the caller can reduce them.
    overwrite : bool
        When ``True``, re-process and overwrite existing per-partition output files instead of skipping them.
        Ignored when *out_path* is ``None``.
    label : str
        Human-readable name used in log messages (dataset name or ``"inputs"``).

    Returns
    -------
    list[HistogramBase] or None
        Filled clones (in-memory mode) or ``None`` (per-partition mode).
    """
    # ── Resume: output already exists ───────────────────────────────────────
    if not overwrite and out_path is not None and os.path.exists(out_path):
        color_msg(f"[{label}] Skipping existing file {out_path} (resume-safe)", color="yellow", indentLevel=1)
        return None

    # ── Fill clones ─────────────────────────────────────────────────────────
    clones = [h.empty_clone() for h in histos_template]
    for h in clones:
        try:
            h.fill(partition)
        except Exception as exc:
            warnings.warn(
                f"Problem filling histogram {h.name!r}: {exc}",
                stacklevel=2,
            )

    # ── Per-partition: write ROOT file ───────────────────────────────────────
    if out_path is not None:
        color_msg(f"[{label}] Saving partition to {out_path}...", color="purple", indentLevel=1)
        save_to_root(clones, out_path)
        return None

    return clones


# ---------------------------------------------------------------------------
# Per-dataset worker
# ---------------------------------------------------------------------------

def _fill_one_dataset(
    events,
    histos: list[HistogramBase],
    outfolder: str,
    file_tag: str,
    per_partition: bool,
    overwrite: bool,
    ncores: int,
    label: str,
) -> None:
    """Fill histograms for a single ``dak.Array`` and write output.

    Parameters
    ----------
    events : dask_awkward.Array
        Lazy event array for one dataset (or explicit inputs).
    histos : list[HistogramBase]
        Histogram templates.
    outfolder : str
        Root output directory.  ``histograms/`` sub-folder is created inside.
    file_tag : str
        Tag appended to the output filename (includes dataset name for
        multi-dataset runs), e.g. ``"_v2_DY"`` → ``histograms_v2_DY.root``.
    per_partition : bool
        Write one ROOT file per partition (resume-safe).
    overwrite : bool
        Re-process and overwrite existing per-partition files.
    ncores : int
        Scheduler hint (``1`` = sync, ``-1`` = default, ``>1`` = processes).
    label : str
        Human-readable name used in log messages (dataset name or ``"inputs"``).
    """
    npartitions = events.npartitions
    color_msg(
        f"[{label}] {len(histos)} histogram(s) × {npartitions} partition(s)",
        color="blue", indentLevel=1,
    )

    out_dir = os.path.join(outfolder, "histograms")
    pad = len(str(npartitions - 1))

    create_outfolder(out_dir)

    # ── Build delayed task graph ─────────────────────────────────────────
    tasks = [
        fill_partition(
            events.partitions[i],
            histos,
            out_path=(
                os.path.join(out_dir, f"histograms{file_tag}_{str(i).zfill(pad)}.root")
                if per_partition else None
            ),
            overwrite=overwrite,
            label=label,
        )
        for i in range(npartitions)
    ]

    # ── Execute ──────────────────────────────────────────────────────────
    sched_kwargs, sched_label = make_dask_sched_kwargs(ncores)

    color_msg(f"[{label}] scheduler: {sched_label}", color="purple", indentLevel=1)
    all_results = list(dask.compute(*tasks, **sched_kwargs))

    # ── Per-partition: files already on disk ──────────────────────────────
    if per_partition:
        return

    # ── In-memory: reduce and write single ROOT file ──────────────────────
    color_msg(f"[{label}] merging partitions...", color="purple", indentLevel=1)
    _reduce_and_save(all_results, outfolder, file_tag)


# ---------------------------------------------------------------------------
# Public entry-point
# ---------------------------------------------------------------------------

def fill_histos(
    inputs: str | list | dict | None = None,
    outfolder: str = "./results",
    tag: str = "",
    maxfiles: int = -1,
    datasets: str | list[str] | None = None,
    tree_name: str | list[str] | None = None,
    ncores: int = -1,
    per_partition: bool = False,
    overwrite: bool = False,
) -> None:
    """Fill histograms from NTuple files.

    Parameters
    ----------
    inputs : str, list, or None
        Explicit input path(s) forwarded to :class:`~dtpr.base.ntuple.NTuple`.
        Mutually exclusive with *datasets*.
        ``None`` → use ``filesets:`` from config (same as ``datasets=[]``).
    outfolder : str
        Output directory.  A ``histograms/`` sub-folder is created inside.
    tag : str
        String appended to the output filename,
        e.g. ``"_v2"`` → ``histograms_v2.root``.
        For multiple datasets the dataset name is also appended:
        ``histograms_v2_DY.root``.
    maxfiles : int
        Cap on number of files loaded per dataset.  ``-1`` = all.
    datasets : str or list[str] or None
        Named datasets to load from ``filesets:`` in the config.
        ``[]`` or ``None`` (with no *inputs*) → load **all** filesets.
        Ignored when *inputs* is given.
    tree_name : str or list[str] or None
        TTree path.  ``str`` → same for all datasets.  ``list[str]`` →
        one entry per dataset (must match *datasets* length). Falls back to config or embedded
        ``"file.root:treepath"`` syntax.
    ncores : int
        ``1`` = synchronous, ``-1`` = dask default, ``>1`` = N processes.
    per_partition : bool
        Write one ROOT file per partition under ``<outfolder>/histograms/``
        (``histograms{tag}_NNNN.root`` or ``histograms{tag}_{dataset}_NNNN.root``).
        Existing files are skipped (resume-safe).
        When ``False`` (default) a single merged ROOT file is written per dataset.
    overwrite : bool
        When ``True``, re-process and overwrite existing per-partition output
        files instead of skipping them.  Ignored when *per_partition* is
        ``False``.  Default ``False``.
    """
    color_msg("Running fill-histos...", "green")
    color_msg("Loading ntuples", "cyan", indentLevel=0)
    ntuple = NTuple(
        inputs=inputs,
        maxfiles=maxfiles,
        tree_name=tree_name,
        datasets=datasets,
    )
    histos = load_histos_from_config()

    if not histos:
        color_msg("No histograms to fill.", color="red", indentLevel=0, bold=True)
        return

    _show_histo_names(histos)

    # ── Dispatch: single array vs. dict of datasets ───────────────────────
    if isinstance(ntuple.events, dict):
        color_msg(
            f"Processing {len(ntuple.events)} dataset(s): "
            f"{', '.join(ntuple.events)}",
            color="purple", indentLevel=0,
        )
        for ds_name, ds_events in ntuple.events.items():
            # Dataset name appended to tag so output files are unambiguously named.
            _fill_one_dataset(
                ds_events, histos, outfolder, f"{tag}_{ds_name}",
                per_partition, overwrite, ncores, label=ds_name,
            )
    else:
        _fill_one_dataset(
            ntuple.events, histos, outfolder, tag,
            per_partition, overwrite, ncores, label="inputs",
        )

    color_msg("Done!", color="green")
