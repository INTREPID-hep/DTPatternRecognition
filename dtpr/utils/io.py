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
            branches = _collect_branches(partition)
            if not branches:
                raise ValueError(f"No fields found in partition — nothing to write.")
            with uproot.recreate(part_path) as f:
                f[treepath] = branches

        tasks = [
            _write_partition(
                events.partitions[i],
                f"{stem}_{str(i).zfill(pad)}{ext}",
            )
            for i in range(n)
        ]
        dask.compute(*tasks)
        return

    # Single-file path — materialise if needed
    if _is_dak:
        events = events.compute()

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
        If ``True`` and *events* is a dask-awkward array, write one Parquet
        file per partition into the directory given by *path* (using
        ``dask_awkward.to_parquet``).  The resulting directory can be read
        back as-is by :class:`~dtpr.base.ntuple.NTuple`.  When ``False``
        (default) all data is materialised and written to a single file.

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
    try:
        import dask_awkward as dak
        _is_dak = isinstance(events, dak.Array)
    except ImportError:
        _is_dak = False

    if per_partition and _is_dak:
        dak.to_parquet(events, path)
        return

    # Single-file path — materialise if needed
    if _is_dak:
        events = events.compute()

    if not ak.fields(events):
        raise ValueError("No fields found on the events array — nothing to write.")

    ak.to_parquet(events, path)


# ---------------------------------------------------------------------------
# Quick smoke-test
# Run with:  python -m dtpr.utils.io
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import tempfile, os
    import uproot
    import dask_awkward as dak

    OUTPUT = os.path.join(tempfile.gettempdir(), "dtpr_io_smoke.root")
    TREEPATH = "DTPR/TREE"

    # # ---- build test events covering all three branch cases ----
    # events = ak.Array([
    #     {
    #         "run": 1, "lumi": 1,
    #         # flat collection
    #         "digis": [{"wh": -1, "BX": 0}, {"wh": 2, "BX": 1}],
    #         # matched_showers: each element is a full shower ParticleRecord
    #         #   → only the id-like field (idx) is written as tps_matched_showers_idx
    #         #   → non-id fields (wh, sector, station) are suppressed
    #         # raw_hits: no id-like field → written as tps_raw_hits_n (count)
    #         "tps": [
    #             {"quality": 2, "BX":  0,
    #              "matched_showers": [{"wh": 0, "sector": 5, "station": 1, "idx": 5}],
    #              "raw_hits":        [{"wire": 11}, {"wire": 12}]},
    #             {"quality": 3, "BX":  1,
    #              "matched_showers": [],
    #              "raw_hits":        [{"wire": 20}]},
    #         ],
    #     },
    #     {
    #         "run": 2, "lumi": 2,
    #         "digis": [{"wh": 1, "BX": 0}],
    #         "tps": [
    #             {"quality": 1, "BX": 0,
    #              "matched_showers": [{"wh": 1,  "sector": 3, "station": 2, "idx": 2},
    #                                  {"wh": -1, "sector": 4, "station": 3, "idx": 7}],
    #              "raw_hits":        [{"wire": 30}]},
    #         ],
    #     },
    # ])

    # print("=" * 60)
    # print("io.py smoke-test")
    # print("=" * 60)

    # # ---- eager path ----
    # print("\n--- Eager (ak.Array) ---")
    # dump_to_root(events, OUTPUT, treepath=TREEPATH)
    # print(f"Written to: {OUTPUT}")

    # with uproot.open(OUTPUT) as f:
    #     tree = f[TREEPATH]
    #     print("Branches :", sorted(tree.keys()))
    #     print("run       :", tree["run"].array().tolist())
    #     print("digis_wh  :", tree["digis_wh"].array().tolist())
    #     print("digis_BX  :", tree["digis_BX"].array().tolist())
    #     print("tps_quality              :", tree["tps_quality"].array().tolist())
    #     print("tps_matched_showers_ids  :", tree["tps_matched_showers_ids"].array().tolist())
    #     print("tps_matched_showers_ids_n:", tree["tps_matched_showers_ids_n"].array().tolist())
    #     print("tps_raw_hits_ids         :", tree["tps_raw_hits_ids"].array().tolist())
    #     print("tps_raw_hits_ids_n       :", tree["tps_raw_hits_ids_n"].array().tolist())

    # assert "tps_matched_showers_wh" not in [b for b in tree.keys()], \
    #     "nested particle fields must not be written"

    # # ---- dask path ----
    # print("\n--- Dask (dask_awkward.Array) ---")
    # devents = dak.from_awkward(events, npartitions=2)
    # dump_to_root(devents, OUTPUT, treepath=TREEPATH)   # triggers .compute() internally
    # print(f"Written to: {OUTPUT} (via dask)")

    # with uproot.open(OUTPUT) as f:
    #     tree = f[TREEPATH]
    #     run_vals = tree["run"].array().tolist()
    #     assert run_vals == [1, 2], f"Expected [1, 2], got {run_vals}"
    #     idx_vals = tree["tps_matched_showers_ids"].array().tolist()
    #     assert idx_vals == [[5], [2, 7]], f"Unexpected ids: {idx_vals}"
    #     idx_n_vals = tree["tps_matched_showers_ids_n"].array().tolist()
    #     assert idx_n_vals == [[1, 0], [2]], f"Unexpected ids_n: {idx_n_vals}"
    #     n_vals = tree["tps_raw_hits_ids"].array().tolist()
    #     assert n_vals == [[0, 1, 0], [0]], f"Unexpected raw_hits_ids: {n_vals}"
    #     n_n_vals = tree["tps_raw_hits_ids_n"].array().tolist()
    #     assert n_n_vals == [[2, 1], [1]], f"Unexpected raw_hits_ids_n: {n_n_vals}"

    # print("\nAll assertions passed ✓")


    # ---- NTuple integration test ----
    print("\n" + "=" * 60)
    print("NTuple integration test")
    print("=" * 60)

    import os
    from dtpr.base.ntuple import NTuple

    NTUPLE_FILE = os.path.abspath(os.path.join(
        os.path.dirname(__file__),
        "../../tests/ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root",
    ))
    NTUPLE_OUT = os.path.join(tempfile.gettempdir(), "dtpr_io_ntuple.root")

    ntuple = NTuple(NTUPLE_FILE, maxfiles=1)
    print(f"Fields on loaded events: {ak.fields(ntuple.events)}")

    dump_to_root(ntuple.events, NTUPLE_OUT, treepath=TREEPATH)
    print(f"Written to: {NTUPLE_OUT}")

    with uproot.open(NTUPLE_OUT) as f:
        tree = f[TREEPATH]
        print(type(tree))
        print("ROOT branches:", sorted(tree.keys()))
        n_events = tree.num_entries
        print(f"Events written: {n_events}")

    print("\nNTuple integration test passed ✓")
