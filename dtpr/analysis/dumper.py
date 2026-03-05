"""Dumper analysis module — exports NTuple events to ROOT or Parquet.

Called via ``dtpr dump -r`` (ROOT) or ``dtpr dump -p`` (Parquet).

Output formats
--------------
Exactly one format must be selected:

- **ROOT** (``-r`` / ``--root``): events are flattened into ``{col}_{field}``
  branches and written as an RNTuple.  Nested cross-references are stored as
  index lists.  **One-way export** — nesting cannot be recovered from ROOT.
- **Parquet** (``-p`` / ``--parquet``): the full awkward array is written
  as-is, preserving all nesting levels.  Can be reloaded by
  :class:`~dtpr.base.ntuple.NTuple` for further processing.

Per-partition output
--------------------
When ``--per-partition`` is set, each dask partition is written independently:

- ROOT  → ``dumpedEvents_{tag}_NNNN.root``
- Parquet → ``dumpedEvents_{tag}/part_NNNN.parquet`` (directory)

Existing output files are *skipped* — the job is **resume-safe** after a
failure.  Use ``--force-overwrite`` to re-process and overwrite them.

Multiple datasets
-----------------
When ``datasets`` is specified (or when ``inputs`` is omitted and the config
defines ``filesets:``), each dataset is written to its own output file:

    dumpedEvents_{tag}_{dataset_name}.root
    dumpedEvents_{tag}_{dataset_name}.parquet
    dumpedEvents_{tag}_{dataset_name}/        (Parquet + ``--per-partition``)

Parallelism
-----------
Controlled by ``ncores`` (identical semantics to ``dtpr fill-histos``):

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

from ..base.ntuple import NTuple
from ..utils.functions import color_msg, create_outfolder, make_dask_sched_kwargs
from ..utils.io import dump_to_root, dump_to_parquet


# ---------------------------------------------------------------------------
# Per-dataset worker
# ---------------------------------------------------------------------------

def _dump_one_dataset(
    events,
    outfolder: str,
    file_tag: str,
    per_partition: bool,
    overwrite: bool,
    to_root: bool,
    ncores: int,
    label: str,
) -> None:
    """Write one dataset's events to ROOT or Parquet.

    Parameters
    ----------
    events : dask_awkward.Array
        Lazy event array for one dataset (or explicit inputs).
    outfolder : str
        Output directory.  Created automatically if it does not exist.
    file_tag : str
        Tag appended to the output filename stem (includes dataset name for
        multi-dataset runs), e.g. ``"_v2_DY"`` → ``output_v2_DY.root``.
    per_partition : bool
        Write one output file per dask partition (resume-safe).
        For Parquet this creates a directory; for ROOT individual ``.root``
        files suffixed with the partition index.
    overwrite : bool
        Re-process and overwrite existing per-partition files.
    to_root : bool
        ``True`` → ROOT output; ``False`` → Parquet output.
    ncores : int
        Scheduler hint (``1`` = sync, ``-1`` = default, ``>1`` = processes).
    label : str
        Human-readable name used in log messages (dataset name or ``"inputs"``).
    """
    npartitions = events.npartitions
    color_msg(
        f"[{label}] 1 {('ROOT' if to_root else 'Parquet')} file × {npartitions} partition(s)",
        color="blue", indentLevel=1,
    )

    sched_kwargs = make_dask_sched_kwargs(ncores)
    try:
        from dask.distributed import get_client
        get_client()
        sched_label = "distributed"
    except Exception:
        sched_label = sched_kwargs.get("scheduler", "threaded (dask default)")
    color_msg(f"[{label}] scheduler: {sched_label}", color="purple", indentLevel=1)

    create_outfolder(outfolder)

    if to_root:
        out_path = os.path.join(outfolder, f"dumpedEvents_{file_tag}.root")
        dump_func = dump_to_root
    else:
        out_path = (
            os.path.join(outfolder, f"dumpedEvents_{file_tag}")
            if per_partition
            else os.path.join(outfolder, f"dumpedEvents_{file_tag}.parquet")
        )
        dump_func = dump_to_parquet
    dump_func(
        events, out_path,
        per_partition=per_partition, ncores=ncores, overwrite=overwrite,
    )

    color_msg(f"[{label}] Events written → {out_path}", color="green", indentLevel=1)


# ---------------------------------------------------------------------------
# Public entry-point
# ---------------------------------------------------------------------------

def dump(
    inputs: str | list | dict | None = None,
    outfolder: str = "./results",
    tag: str = "",
    maxfiles: int = -1,
    datasets: str | list[str] | None = None,
    tree_name: str | list[str] | None = None,
    per_partition: bool = False,
    to_root: bool = False,
    to_parquet: bool = False,
    ncores: int = -1,
    overwrite: bool = False,
) -> None:
    """Load DT NTuple files and export the event array to ROOT or Parquet.

    Exactly one of ``--root`` / ``--parquet`` must be supplied.

    Parameters
    ----------
    inputs : str, list[str], or None
        Explicit input path(s): file, directory, glob, or
        ``"file.root:treepath"``.
        ``None`` falls back to filesets defined in the run-config YAML.
        Mutually exclusive with *datasets*.
    outfolder : str
        Output directory.  Created automatically if it does not exist.
        Output filename stem: ``dumpedEvents_{tag}.root`` /
        ``dumpedEvents_{tag}.parquet`` (single dataset) or
        ``dumpedEvents_{tag}_{dataset}.root`` / … (multi-dataset).
    tag : str
        Suffix appended to the output filename stem.
    maxfiles : int
        Cap on the number of input files per dataset.  ``-1`` = all.
    datasets : str | list[str] | None
        Named datasets to load from ``filesets:`` in the config.
        ``[]`` or ``None`` (with no *inputs*) → load **all** filesets.
        Ignored when *inputs* is given.
    tree_name : str | list[str] | None
        TTree path inside ROOT files (e.g. ``"dtNtupleProducer/DTTREE"``).
        Can also be embedded in *inputs* as ``"file.root:treepath"``.
    per_partition : bool
        Write one output file per dask partition instead of a single merged
        file.  For Parquet this creates a directory; for ROOT it creates
        individual ``.root`` files suffixed with the partition index.
    ncores : int
        ``1`` = synchronous (debug), ``-1`` = dask default, ``>1`` = local
        processes.  Ignored when a distributed client is already active
        (``--scheduler-address``).
    overwrite : bool
        When ``True``, re-process and overwrite existing per-partition output
        files instead of skipping them.  Default ``False``.
    to_root : bool
        Export to ROOT format (``-r`` / ``--root`` flag with dtpr command).
    to_parquet : bool
        Export to Parquet format (``-p`` / ``--parquet`` flag with dtpr command).

    Raises
    ------
    ValueError
        If neither or both of *to_root* / *to_parquet* are set.
    """
    if to_root == to_parquet:  # both True or both False
        raise ValueError(
            "Specify exactly one output format: --root (-r) or --parquet (-p)."
        )

    fmt = "ROOT" if to_root else "Parquet"
    color_msg(f"Running dump ({fmt})...", "green")
    color_msg("Loading ntuples", "cyan", indentLevel=0)

    ntuple = NTuple(
        inputs=inputs,
        maxfiles=maxfiles,
        tree_name=tree_name,
        datasets=datasets,
    )

    # ── Dispatch: single array vs. dict of datasets ───────────────────────
    if isinstance(ntuple.events, dict):
        color_msg(
            f"Processing {len(ntuple.events)} dataset(s): "
            f"{', '.join(ntuple.events)}",
            color="purple", indentLevel=0,
        )
        for ds_name, ds_events in ntuple.events.items():
            _dump_one_dataset(
                ds_events, outfolder, f"{tag}_{ds_name}",
                per_partition, overwrite, to_root, ncores, label=ds_name,
            )
    else:
        _dump_one_dataset(
            ntuple.events, outfolder, tag,
            per_partition, overwrite, to_root, ncores, label="inputs",
        )

    color_msg("Done!", color="green")
