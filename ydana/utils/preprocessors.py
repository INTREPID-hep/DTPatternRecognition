"""Columnar preprocessors — functions with signature
``fn(events: ak.Array) -> None``.

Each function receives the full event array and **mutates it in-place** via
``events["field"] = value`` (leverages ``ak.Array.__setitem__`` /
``dak.Array.__setitem__``).  No return value is used.
For nested collections: ``events["col"] = ak.with_field(events["col"], value, "key")``.
"""

from __future__ import annotations

from typing import Callable

import awkward as ak
import numpy as np
import dask_awkward as dak


def add_genmuon_dR(events: ak.Array) -> None:
    """Add a per-event ``dR`` field between the first two gen-muons.

    Events with fewer than two gen-muons get ``dR = -999.0``.
    Mutates ``events`` in-place by setting ``events["dR"]``.
    """
    genmuons = events["genmuons"]
    has_two = ak.num(genmuons) >= 2

    eta = ak.pad_none(genmuons["eta"], 2, axis=1)
    phi = ak.pad_none(genmuons["phi"], 2, axis=1)

    eta0 = ak.fill_none(eta[:, 0], 0.0)
    eta1 = ak.fill_none(eta[:, 1], 0.0)
    phi0 = ak.fill_none(phi[:, 0], 0.0)
    phi1 = ak.fill_none(phi[:, 1], 0.0)

    d_eta = eta1 - eta0
    d_phi = phi1 - phi0
    d_phi = ak.where(d_phi > np.pi, d_phi - 2.0 * np.pi, d_phi)
    d_phi = ak.where(d_phi < -np.pi, d_phi + 2.0 * np.pi, d_phi)

    dR = np.sqrt(d_eta**2 + d_phi**2)
    events["dR"] = ak.where(has_two, dR, -999.0)


# ---------------------------------------------------------------------------
# Nested-ids reconstruction
# ---------------------------------------------------------------------------


def reconstruct_nested_ids(
    flat_field: str,
    n_field: str,
    col: str,
    out_field: str | None = None,
) -> Callable:
    """Factory: return a preprocessor that rebuilds a doubly-jagged ids field.

    A ROOT TTree (or any data source using the flat+count convention) encodes
    a ``var * var * int`` field as two **top-level** branches on the events
    array:

    * ``events[flat_field]``  — ``var * int``, all ids for an event concatenated
      across all parent particles (e.g. ``[[5], [2, 7]]``).
    * ``events[n_field]``     — ``var * int``, number of ids contributed by each
      parent particle in that event (e.g. ``[[1, 0], [2]]``).

    The returned preprocessor reconstructs the original ``var * var * int``
    field via ``ak.unflatten(flat, counts, axis=1)`` and injects it into the
    *col* collection under the name *out_field*.

    Parameters
    ----------
    flat_field : str
        Top-level events field with the flat per-event ids,
        e.g. ``"tps_matched_showers_ids"``  (``var * int``).
    n_field : str
        Top-level events field with the per-parent-particle counts,
        e.g. ``"tps_matched_showers_ids_n"``  (``var * int``).
    col : str
        Name of the collection to which the nested field is added,
        e.g. ``"tps"``.
    out_field : str, optional
        Name of the new nested field within *col*.  Defaults to
        *flat_field* with the trailing ``_ids`` suffix stripped,
        e.g. ``"tps_matched_showers"`` when *flat_field* is
        ``"tps_matched_showers_ids"``.

    Returns
    -------
    Callable
        A preprocessor ``fn(events) -> None`` that mutates *events* in-place.

    Examples
    --------
    Use in a YAML pre-steps block:

    .. code-block:: yaml

       pre-steps:
         - name: reconstruct_nested_ids
           args:
             - "tps_matched_showers_ids"
             - "tps_matched_showers_ids_n"
             - "tps"

    Or programmatically::

        from ydana.utils.preprocessors import reconstruct_nested_ids

        pp = reconstruct_nested_ids(
            "tps_matched_showers_ids",
            "tps_matched_showers_ids_n",
            "tps",
        )
        pp(events)
        # events["tps"]["matched_showers_ids"] is now var * var * int
    """
    # Infer the output field name if not provided.
    # Strip the trailing "_ids" suffix so e.g. "tps_matched_showers_ids" → "tps_matched_showers".
    resolved_out = (
        out_field
        if out_field is not None
        else (flat_field[:-4] if flat_field.endswith("_ids") else flat_field)
    )

    def _preprocessor(events: ak.Array | dak.Array) -> None:
        flat_ids = events[flat_field]  # var * int per event
        counts = events[n_field]  # var * int per event (count per parent)
        # ak.unflatten requires 1-D counts.  We have jagged counts (one per TP
        # per event), so we need a two-step unflatten:
        #   1. flatten everything to 1D and rebuild per-TP lists
        #   2. group the per-TP lists back into per-event lists
        flat_all = ak.flatten(flat_ids)  # 1-D
        per_tp_counts = ak.flatten(counts)  # 1-D
        n_tps_per_event = ak.num(counts, axis=1)  # 1-D
        per_tp_ids = ak.unflatten(flat_all, per_tp_counts)  # var * int
        nested = ak.unflatten(per_tp_ids, n_tps_per_event)  # var * var * int
        events[col] = ak.with_field(events[col], nested, resolved_out)

    _preprocessor.__name__ = f"reconstruct_nested_ids({flat_field!r}, {n_field!r} → {col!r}.{resolved_out!r})"
    _preprocessor.__qualname__ = _preprocessor.__name__
    return _preprocessor
