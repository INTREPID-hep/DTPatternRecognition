"""I/O utilities for the DTPR framework.

Currently provides :func:`dump_to_root`, a convenience function that writes a
processed event array to a new ROOT file using *uproot*.
"""

from __future__ import annotations
import uproot
import os
import awkward as ak
import dask
import dask_awkward as dak
from ..base.particle import ParticleRecord
from ..utils.functions import compute_on_partitions, color_msg, create_outfolder, make_dask_sched_kwargs


def _collect_branches(events) -> dict:
    """Recursively flatten the event array into a ``{branch_name: array}`` dict.

    Branch naming rules
    -------------------
    * Top-level scalar field ``"run"``
        → branch ``"run"``  (``int`` or ``float`` per event)

    * Top-level collection ``"digis"`` (record of 1-D jagged sub-fields)
        → one branch per sub-field: ``"digis_wh"``, ``"digis_BX"``, …

    * Nested collection inside a collection  ``"tps"."matched_showers"``
      where each element is a full ``ParticleRecord`` (doubly-jagged)
        → the ``id`` of each nested particle is written as
          ``"tps_matched_showers_ids"``  (``var * var * int``) —
          a list of ids per parent particle per event, e.g. ``[[1, 4], [0, 9]]``.
          All other fields of the nested particle (``wh``, ``sector``, …) are
          suppressed; the id list is sufficient for cross-referencing into the
          top-level collection.
          If no id-like field is found, falls back to ``ak.local_index``
          (the positional index of each nested particle within its parent list,
          identical to what :attr:`ParticleRecord.id` returns via ``layout.at``).
          Branch is still named ``"tps_matched_showers_ids"``.
    """
    branches: dict = {}

    for field in ak.fields(events):
        col = events[field]
        subfields = ak.fields(col)

        if not subfields:
            # Plain scalar-per-event field
            branches[field] = col
            continue

        # Particle collection — iterate its sub-fields
        for subfield in subfields:
            subcol = col[subfield]
            nested_subfields = list(ak.fields(subcol))

            if nested_subfields:
                # Nested collection of ParticleRecords (e.g. tps.matched_showers).
                # Delegate id-extraction to ParticleRecord.ids_from_array, which
                # mirrors id exactly: explicit field if found, local_index otherwise.
                branches[f"{field}_{subfield}_ids"] = ParticleRecord.ids_from_array(subcol)
            else:
                # Plain jagged leaf (e.g. tps.quality  →  var * int)
                branches[f"{field}_{subfield}"] = subcol

    return branches


# ---------------------------------------------------------------------------
# Partition-level workers — @dask.delayed, composable by users
# ---------------------------------------------------------------------------

@dask.delayed
def write_root_partition(
    partition,
    out_path: str,
    treepath: str = "DTPR/TREE",
    overwrite: bool = False,
    label: str = "",
) -> None:
    """Write one materialised partition to a ROOT file (RNTuple).

    This is the building block used by :func:`dump_to_root` in per-partition
    mode.  Being ``@dask.delayed``, it can be composed into custom dask graphs
    without going through the high-level entry-point::

        import dask
        from dtpr.utils.io import write_root_partition

        tasks = [
            write_root_partition(events.partitions[i], f"out_{i}.root")
            for i in range(events.npartitions)
        ]
        dask.compute(*tasks)

    Parameters
    ----------
    partition : ak.Array
        Materialised awkward array for one partition.
    out_path : str
        Output ``.root`` file path.
    treepath : str
        RNTuple path inside the ROOT file.  Default ``"DTPR/TREE"``.
    overwrite : bool
        If ``False`` (default) skip writing when *out_path* already exists.
    """
    # ── Resume: output already exists ───────────────────────────────────────
    if not overwrite and os.path.exists(out_path):
        color_msg(f"[{label}] Skipping existing file {out_path} (resume-safe)", color="yellow", indentLevel=1)
        return
    # ── write partition to ROOT ───────────────────────────────────────────────
    branches = _collect_branches(partition)
    if not branches:
        Warning.warn("No events found in partition — skipping ROOT output.", stacklevel=2)
        return

    with uproot.recreate(out_path) as f:
        f[treepath] = branches
        color_msg(f"[{label}] Saving partition to {out_path}", color="green", indentLevel=1)


@dask.delayed
def write_parquet_partition(
    partition,
    path: str,
    overwrite: bool = False,
    label: str = "",
) -> None:
    """Write one materialised partition to a Parquet file.

    Mirrors :func:`write_root_partition` for the Parquet format::

        import dask
        from dtpr.utils.io import write_parquet_partition

        tasks = [
            write_parquet_partition(events.partitions[i], f"out/part_{i}.parquet")
            for i in range(events.npartitions)
        ]
        dask.compute(*tasks)

    Parameters
    ----------
    partition : ak.Array
        Materialised awkward array for one partition.
    path : str
        Output Parquet file path.
    overwrite : bool
        If ``False`` (default) skip writing when *path* already exists.
    """
    if not overwrite and os.path.exists(path):
        color_msg(f"[{label}]Skipping existing file {path} (resume-safe)", color="yellow", indentLevel=1)
        return
    if not ak.fields(partition):
        raise ValueError("No fields found in partition — nothing to write.")

    color_msg(f"[{label}]Saved partition to {path}", color="green", indentLevel=1)
    ak.to_parquet(partition, path)


def dump_to_root(
    events,
    outfolder: str,
    treepath: str = "DTPR/TREE",
    file_tag: str = "",
    per_partition: bool = False,
    overwrite: bool = False,
    ncores: int = -1,
    label: str = "",
) -> None:
    """Write an event array to a new ROOT file.

    .. warning:: **One-way export only.**
       The output is a *flattened* representation of the event array — nested
       particle collections are decomposed into ``{col}_{field}`` branches.
       The nesting information (e.g. that ``tps_matched_showers_ids`` encodes
       cross-references into the ``showers`` collection) is not preserved in the
       ROOT file.  If you need to **persist the full array structure for later
       re-use within this framework**, use :func:`dump_to_parquet` instead —
       awkward-parquet preserves all nesting levels natively and can be reloaded
       by :class:`~dtpr.base.ntuple.NTuple`.

    Parameters
    ----------
    events : ak.Array or dask_awkward.Array
        The event array produced by :func:`~dtpr.base.pipeline.execute_pipeline`
        (or loaded directly from an NTuple).  If a dask-awkward array is passed,
        it is materialised by calling ``.compute()`` before writing.
    outfolder : str
        Output folder path.
    treepath : str
        RNTuple path inside the ROOT file.  Default ``"DTPR/TREE"``.
        Use ``"/"``-separated names to create sub-directories,
        e.g. ``"myDir/events"``.
    per_partition : bool, optional
        If ``True`` and *events* is a dask-awkward array, each partition is
        written to a separate file.  The file name is derived from *path* by
        inserting the zero-padded partition index before the extension:
        ``output.root`` → ``output_0.root``, ``output_1.root``, …
        When ``False`` (default) all partitions are materialised together and
        written to a single file.

    Notes
    -----
    **Branch layout** (see :func:`_collect_branches` for the full rules):

    * ``events["run"]`` → branch ``run``  (scalar per event)
    * ``events["digis"]["wh"]`` → branch ``digis_wh``  (``var * int``)
    * ``events["tps"]["matched_showers"]`` — each element is a shower
      ``ParticleRecord`` with fields ``(wh, sector, station, idx)``.  Only the
      ``id`` of each matched shower is written, as a list-per-tp:
      → branch ``tps_matched_showers_ids``  (``var * var * int``),
      e.g. ``[[1, 4], [0, 9]]``.  Non-id particle fields are suppressed.
    * Nested collection whose particle records have no id-like field
      → ``ak.local_index`` (positional index, same as ``ParticleRecord.id``
      fallback) is written as ``tps_raw_hits_ids``  (``var * var * int``).

    The output uses ROOT's **RNTuple** format (requires ROOT 6.28+), which
    stores jagged arrays natively without counter branches.
    **Existing files are overwritten** — ``uproot.recreate`` is used.

    Examples
    --------
    ::

        from dtpr.utils.io import dump_to_root

        events = ntuple.events
        dump_to_root(events, "ntuples", treepath="dtpr/events")

        # One file per dask partition:
        dump_to_root(events, "processed.root", per_partition=True)
        # → processed_0.root, processed_1.root, …
    """
    npartitions = events.npartitions
    color_msg(
        f"[{label}] 1 'ROOT' file × {npartitions} partition(s)",
        color="blue", indentLevel=1,
    )

    out_dir = os.path.join(outfolder, "roots")
    pad = len(str(npartitions - 1))

    create_outfolder(out_dir)

    # ── Build delayed task graph ─────────────────────────────────────────
    tasks = [
        write_root_partition(
            events.partitions[i],
            out_path=(
                os.path.join(out_dir, f"rntuple{file_tag}_{str(i).zfill(pad)}.root")
                if per_partition else os.path.join(out_dir, f"rntuple{file_tag}.root")
            ),
            treepath=treepath,
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
    _reduce_and_save(all_results, os.path.join(out_dir, f"events.parquet"))


def dump_to_parquet(
    events,
    path: str,
    per_partition: bool = False,
    ncores: int = -1,
    overwrite: bool = False,
    label: str = "",
) -> None:
    """Persist the full event array to Parquet, preserving all nesting.

    Unlike :func:`dump_to_root`, the output is **not flattened** — all fields
    and nested collections are written as-is, exactly as found in the event
    array.  The file can be reloaded with :class:`~dtpr.base.ntuple.NTuple`
    (parquet support) and the reconstructed array will have the same structure.

    Parameters
    ----------
    events : ak.Array or dask_awkward.Array
        The event array to persist.  If a dask-awkward array is passed,
        it is materialised by calling ``.compute()`` before writing.
    path : str
        Output file path (``per_partition=False``) or **directory** path
        (``per_partition=True``), e.g. ``"output.parquet"`` or
        ``"output_dir/"``.
        If the path does not end with ``".parquet"``, a ``.parquet`` suffix is
        **not** appended automatically — you are free to use any name.
    per_partition : bool, optional
        If ``True`` and *events* is a dask-awkward array, each partition is
        written to a separate Parquet file inside the directory *path*:
        ``output_dir/part_000.parquet``, ``part_001.parquet``, …
        Existing files are *skipped* (resume-safe).
        When ``False`` (default) all data is materialised and written to a
        single file.
    ncores : int
        ``1`` = synchronous, ``-1`` = dask default, ``>1`` = N processes.
        Only meaningful when *per_partition* is ``True``.

    Examples
    --------
    ::

        from dtpr.utils.io import dump_to_parquet

        events = ntuple.events
        dump_to_parquet(events, "processed.parquet")

        # One file per dask partition (preserves partition boundaries):
        dump_to_parquet(events, "processed_dir/", per_partition=True)

        # Round-trip (single file or directory both work):
        from dtpr.base.ntuple import NTuple
        ntuple2 = NTuple("processed.parquet")
        ntuple3 = NTuple("processed_dir/")
    """
    npartitions = events.npartitions
    color_msg(
        f"[{label}] 1 'Parquet' file × {npartitions} partition(s)",
        color="blue", indentLevel=1,
    )

    out_dir = os.path.join(path, "parquets")
    pad = len(str(npartitions - 1))

    create_outfolder(out_dir)

    # ── Build delayed task graph ───────────────────────────────────────── 

    task = [
        write_parquet_partition(
            events.partitions[i],
            path=(os.path.join(out_dir, f"part_{str(i).zfill(pad)}.parquet") 
                    if per_partition else os.path.join(out_dir, f"events.parquet")
                ),
            overwrite=overwrite,
            label=label,
        )
        for i in range(npartitions)
    ]

    # ── Execute ──────────────────────────────────────────────────────────
    sched_kwargs, sched_label = make_dask_sched_kwargs(ncores)

    color_msg(f"[{label}] scheduler: {sched_label}", color="purple", indentLevel=1)
    all_results = dask.compute(*task, **sched_kwargs)

    if per_partition:
        return

    # ── In-memory: single file ───────────────────────────────────────────────
    color_msg(f"[{label}] merging partitions...", color="purple", indentLevel=1)
    _reduce_and_save(all_results, os.path.join(out_dir, f"events.parquet"))


if __name__ == "__main__":
    # ---- NTuple integration test ----
    print("\n" + "=" * 60)
    print("NTuple integration test")
    print("=" * 60)
    import tempfile
    from dtpr.base.ntuple import NTuple

    OUTPUT = tempfile.gettempdir()
    TREEPATH = "DTPR/TREE"


    NTUPLE_FILE = os.path.abspath(os.path.join(
        os.path.dirname(__file__),
        "../../tests/ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root",
    ))

    ntuple = NTuple(NTUPLE_FILE, maxfiles=1, tree_name="dtNtupleProducer/DTTREE")
    print(f"Fields on loaded events: {ak.fields(ntuple.events)}")

    dump_to_root(ntuple.events, OUTPUT, treepath=TREEPATH)
    print(f"Written to: {OUTPUT}")