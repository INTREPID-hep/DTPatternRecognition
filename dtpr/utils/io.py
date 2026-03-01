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
        → the ``_id`` of each nested particle is written as
          ``"tps_matched_showers_ids"``  (``var * var * int``) —
          a list of ids per parent particle per event, e.g. ``[[1, 4], [0, 9]]``.
          All other fields of the nested particle (``wh``, ``sector``, …) are
          suppressed; the id list is sufficient for cross-referencing into the
          top-level collection.
          If no id-like field is found, falls back to ``ak.local_index``
          (the positional index of each nested particle within its parent list,
          identical to what :attr:`ParticleRecord._id` returns via ``layout.at``).
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
                # Delegate id-extraction to ParticleRecord._ids_from_array, which
                # mirrors _id exactly: explicit field if found, local_index otherwise.
                branches[f"{field}_{subfield}_ids"] = ParticleRecord._ids_from_array(subcol)
            else:
                # Plain jagged leaf (e.g. tps.quality  →  var * int)
                branches[f"{field}_{subfield}"] = subcol

    return branches


def _to_ttree_branches(branches: dict) -> dict:
    """Re-encode doubly-jagged branches for TTree compatibility.

    ROOT TTrees cannot store ``var * var * T`` branches.  The standard
    encoding is two branches:

    * ``{name}``    (``var * int``) — flat list of ids per event
      (``ak.flatten(arr, axis=-1)``)
    * ``{name}_n``  (``var * int``) — number of ids per *parent* particle
      (``ak.num(arr, axis=-1)``), so the doubly-jagged structure can be
      reconstructed as ``ak.unflatten(ids, ids_n)``.

    All other branch types pass through unchanged.
    """
    out: dict = {}
    for name, arr in branches.items():
        if str(arr.type.content).startswith("var * var * "):  # doubly-jagged
            out[name] = ak.flatten(arr, axis=-1)   # var * int per event
            out[name + "_n"] = ak.num(arr, axis=-1)  # var * int per event
        else:
            out[name] = arr
    return out


def dump_to_root(events, path: str, treepath: str = "DTPR/TREE",
                 format: str = "TTree") -> None:
    """Write an event array to a new ROOT file.

    Parameters
    ----------
    events : ak.Array or dask_awkward.Array
        The event array produced by :func:`~dtpr.base.pipeline.execute_pipeline`
        (or loaded directly from an NTuple).  If a dask-awkward array is passed,
        it is materialised by calling ``.compute()`` before writing.
    path : str
        Output file path, e.g. ``"output.root"``.
    treepath : str
        TTree path inside the ROOT file.  Default ``"DTPR/TREE"``.
        Use ``"/"``-separated names to create sub-directories, e.g. ``"myDir/events"``.
    format : {"TTree", "RNTuple"}
        Output format.  Default ``"TTree"`` (standard ROOT TTree, readable by any
        ROOT version).  Use ``"RNTuple"`` for the new columnar format (ROOT 6.28+).

        .. note::
           TTrees cannot store ``var * var * T`` branches.  Any ``*_ids`` branch
           produced from a nested particle collection is automatically split into
           two branches: ``{name}`` (flat ids per event) and ``{name}_n``
           (count per parent particle) — see :func:`_to_ttree_branches`.
           RNTuple stores all arrays as-is.

    Notes
    -----
    **Branch layout** (see :func:`_collect_branches` for the full rules):

    * ``events["run"]`` → branch ``run``  (scalar per event)
    * ``events["digis"]["wh"]`` → branch ``digis_wh``  (``var * int``)
    * ``events["tps"]["matched_showers"]`` — each element is a shower ``ParticleRecord``
      with fields ``(wh, sector, station, idx)``.  The ``_id`` of each matched shower
      is written as a list-per-tp:
      → branch ``tps_matched_showers_ids``  (``var * var * int``), e.g. ``[[1, 4], [0, 9]]``.
      Non-id particle fields (``wh``, ``sector``, ``station``) are suppressed.
    * Nested collection whose particle records have no id-like field
      → ``ak.local_index`` (positional index, same as ``ParticleRecord._id``
      fallback via ``layout.at``) is written as ``tps_raw_hits_ids`` /
      ``tps_raw_hits_ids_n`` (TTree) or ``tps_raw_hits_ids`` (RNTuple).

    For TTree output, doubly-jagged ``*_ids`` branches are split — see
    :func:`_to_ttree_branches` for the encoding details.

    Scalar branches are fixed-type leaves; jagged branches become ``std::vector<T>``
    (uproot 5 handles this automatically for ``ak.Array`` of type ``var * T``).

    **Existing files are overwritten** — ``uproot.recreate`` is used.

    Examples
    --------
    ::

        from dtpr.utils.io import dump_to_root

        events = ntuple.events()
        events = execute_pipeline(events, steps)
        dump_to_root(events, "processed.root", treepath="dtpr/events")
    """
    import uproot

    # Materialise dask arrays before writing
    try:
        import dask_awkward as dak
        if isinstance(events, dak.Array):
            events = events.compute()
    except:
        pass

    branches = _collect_branches(events)

    if not branches:
        raise ValueError("No fields found on the events array — nothing to write.")

    with uproot.recreate(path) as f:
        if format.upper() == "RNTUPLE":
            f[treepath] = branches
        else:  # TTree (default)
            ttree_branches = _to_ttree_branches(branches)
            branch_types = {n: str(a.type.content) for n, a in ttree_branches.items()}
            f.mktree(treepath, branch_types)
            f[treepath].extend(ttree_branches)


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
