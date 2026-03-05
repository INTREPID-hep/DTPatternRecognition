"""I/O utilities for the DTPR framework.

Currently provides :func:`dump_to_root`, a convenience function that writes a
processed event array to a new ROOT file using *uproot*.
"""

from __future__ import annotations

import awkward as ak
from ..base.particle import ParticleRecord


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


def dump_to_root(
    events,
    path: str,
    treepath: str = "DTPR/TREE",
    *,
    per_partition: bool = False,
    ncores: int = -1,
    overwrite: bool = False,
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
    path : str
        Output file path, e.g. ``"output.root"``.
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
        dump_to_root(events, "processed.root", treepath="dtpr/events")

        # One file per dask partition:
        dump_to_root(events, "processed.root", per_partition=True)
        # → processed_0.root, processed_1.root, …
    """
    import uproot
    import os

    try:
        import dask_awkward as dak
        _is_dak = isinstance(events, dak.Array)
    except ImportError:
        _is_dak = False

    if per_partition and _is_dak:
        import dask
        stem, ext = os.path.splitext(path)
        ext = ext or ".root"
        n = events.npartitions
        pad = len(str(n - 1))

        @dask.delayed
        def _write_partition(partition, part_path):
            if not overwrite and os.path.exists(part_path):
                return
            branches = _collect_branches(partition)
            if not branches:
                raise ValueError(f"No fields found in partition — nothing to write.")
            with uproot.recreate(part_path) as f:
                f[treepath] = branches

        from ..utils.functions import make_dask_sched_kwargs, color_msg
        tasks = []
        for i in range(n):
            part_path = f"{stem}_{str(i).zfill(pad)}{ext}"
            if not overwrite and os.path.exists(part_path):
                color_msg(f"  Skipping partition {i} → {os.path.basename(part_path)} (exists)", color="yellow", indentLevel=1)
            else:
                color_msg(f"  Writing partition {i} → {os.path.basename(part_path)}", color="purple", indentLevel=1)
            tasks.append(_write_partition(events.partitions[i], part_path))
        dask.compute(*tasks, **make_dask_sched_kwargs(ncores))
        return

    # Single-file path — materialise if needed
    if _is_dak:
        from ..utils.functions import make_dask_sched_kwargs
        events = events.compute(**make_dask_sched_kwargs(ncores))

    branches = _collect_branches(events)
    if not branches:
        raise ValueError("No fields found on the events array — nothing to write.")

    with uproot.recreate(path) as f:
        f[treepath] = branches


def dump_to_parquet(
    events,
    path: str,
    *,
    per_partition: bool = False,
    ncores: int = -1,
    overwrite: bool = False,
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
    import os

    try:
        import dask_awkward as dak
        _is_dak = isinstance(events, dak.Array)
    except ImportError:
        _is_dak = False

    if per_partition and _is_dak:
        import dask
        os.makedirs(path, exist_ok=True)
        n = events.npartitions
        pad = len(str(n - 1))

        @dask.delayed
        def _write_partition(partition, part_path):
            if not overwrite and os.path.exists(part_path):
                return
            if not ak.fields(partition):
                raise ValueError(f"No fields found in partition — nothing to write.")
            ak.to_parquet(partition, part_path)

        from ..utils.functions import make_dask_sched_kwargs, color_msg
        tasks = []
        for i in range(n):
            part_path = os.path.join(path, f"part_{str(i).zfill(pad)}.parquet")
            if not overwrite and os.path.exists(part_path):
                color_msg(f"  Skipping partition {i} → {os.path.basename(part_path)} (exists)", color="yellow", indentLevel=1)
            else:
                color_msg(f"  Writing partition {i} → {os.path.basename(part_path)}", color="purple", indentLevel=1)
            tasks.append(_write_partition(events.partitions[i], part_path))
        dask.compute(*tasks, **make_dask_sched_kwargs(ncores))
        return

    # Single-file path — materialise if needed
    if _is_dak:
        from ..utils.functions import make_dask_sched_kwargs
        events = events.compute(**make_dask_sched_kwargs(ncores))

    if not ak.fields(events):
        raise ValueError("No fields found on the events array — nothing to write.")

    ak.to_parquet(events, path)


if __name__ == "__main__":
    # ---- NTuple integration test ----
    print("\n" + "=" * 60)
    print("NTuple integration test")
    print("=" * 60)
    import tempfile, os

    OUTPUT = os.path.join(tempfile.gettempdir(), "dtpr_io_smoke.root")
    TREEPATH = "DTPR/TREE"

    import os
    from dtpr.base.ntuple import NTuple

    NTUPLE_FILE = os.path.abspath(os.path.join(
        os.path.dirname(__file__),
        "../../tests/ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root",
    ))
    NTUPLE_OUT = os.path.join(tempfile.gettempdir(), "dtpr_io_ntuple.root")

    ntuple = NTuple(NTUPLE_FILE, maxfiles=1, tree_name="dtNtupleProducer/DTTREE")
    print(f"Fields on loaded events: {ak.fields(ntuple.events)}")

    dump_to_root(ntuple.events, NTUPLE_OUT, treepath=TREEPATH)
    print(f"Written to: {NTUPLE_OUT}")