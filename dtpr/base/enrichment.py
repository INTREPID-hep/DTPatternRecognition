"""
Enrichment pass — computed fields, sorters, and filters.

Called after ``NanoEventsFactory`` has loaded the structural columns.  For each
particle type declared in the config, this module:

1. Adds **computed fields** defined by ``expr`` (columnar Python/awkward
   expression) or ``src`` (a callable that receives the full ragged collection
   and returns a new field array).
2. Applies a per-particle **filter** mask declared as a boolean expression over
   the collection's fields.
3. **Sorts** particles within each event using ``ak.argsort`` on the declared
   field.

Expression language requirements
---------------------------------
``expr`` strings are evaluated with a namespace that maps every *current* field
name of the collection to its corresponding ``ak.Array``.  The ``ak`` module is
also present under the name ``ak``.  Pure-Python ternary conditionals
(``x if cond else y``) do **not** work on arrays — use ``ak.where`` instead.
All standard Python builtins (``abs``, ``int``, ``float`` …) are available.

``filter`` / ``sorter.by`` strings are evaluated in the same namespace.  The
old per-particle syntax using ``p.field`` (e.g. ``p.quality >= 0``) is *not*
understood here; use bare field names (e.g. ``quality >= 0``).

Any evaluation error is caught and emitted as a ``UserWarning`` so that a
misconfigured expression does not abort the whole loading pipeline.
"""

from __future__ import annotations

import builtins
import warnings
from functools import partial

import awkward as ak


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def enrich(events: ak.Array, config) -> ak.Array:
    """Apply computed fields, sorters, and filters to every particle type.

    Parameters
    ----------
    events : ak.Array
        The raw event array returned by ``NanoEventsFactory``.  Each field
        whose name matches a key in ``config.particle_types`` is treated as a
        ragged collection of particle records.
    config : Config
        A :class:`~dtpr.base.config.Config` instance with a
        ``particle_types`` attribute.

    Returns
    -------
    ak.Array
        A new event array with enriched particle collections.
    """
    particle_types: dict = getattr(config, "particle_types", {}) or {}

    for ptype, pinfo in particle_types.items():
        if not isinstance(pinfo, dict):
            continue
        if ptype not in events.fields:
            continue

        collection: ak.Array = events[ptype]
        attrs: dict = pinfo.get("attributes") or {}

        # ------------------------------------------------------------------
        # 1. Computed fields
        # ------------------------------------------------------------------
        for attr, attr_info in attrs.items():
            if not isinstance(attr_info, dict):
                # plain value (e.g. matched_segments: []) — skip
                continue

            expr: str | None = attr_info.get("expr")
            if expr:
                collection = _apply_expr(collection, attr, expr, ptype)

            src: str | None = attr_info.get("src")
            if src:
                collection = _apply_src(collection, attr, src, attr_info, ptype)

        # ------------------------------------------------------------------
        # 2. Filter
        # ------------------------------------------------------------------
        filter_expr: str | None = pinfo.get("filter")
        if filter_expr and isinstance(filter_expr, str):
            collection = _apply_filter(collection, filter_expr, ptype)

        # ------------------------------------------------------------------
        # 3. Sorter
        # ------------------------------------------------------------------
        sorter: dict | None = pinfo.get("sorter")
        if sorter and isinstance(sorter, dict):
            collection = _apply_sorter(collection, sorter, ptype)

        # Write the enriched collection back into the event array
        events = ak.with_field(events, collection, ptype)

    return events


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _build_ns(collection: ak.Array) -> dict:
    """Build an eval namespace from the current fields of *collection*."""
    ns: dict = {}
    for field in collection.fields:
        try:
            ns[field] = collection[field]
        except Exception:
            pass  # skip fields that cannot be retrieved
    ns["ak"] = ak
    # Expose all Python builtins so that abs(), int(), etc. work
    ns.update({name: getattr(builtins, name) for name in dir(builtins) if not name.startswith("_")})
    return ns


def _apply_expr(
    collection: ak.Array,
    attr: str,
    expr: str,
    ptype: str,
) -> ak.Array:
    """Evaluate *expr* in a field namespace and attach the result as *attr*."""
    try:
        ns = _build_ns(collection)
        result = eval(expr, ns)  # noqa: S eval
        collection = ak.with_field(collection, result, attr)
    except Exception as exc:
        warnings.warn(
            f"[enrich] expr evaluation failed for {ptype}.{attr} "
            f"(expr={expr!r}): {exc}",
            stacklevel=4,
        )
    return collection


def _apply_src(
    collection: ak.Array,
    attr: str,
    src: str,
    attr_info: dict,
    ptype: str,
) -> ak.Array:
    """Resolve *src* to a callable and apply it to the collection."""
    try:
        from ..utils.functions import get_callable_from_src  # local import to avoid circular

        method = get_callable_from_src(src)
        if method is None:
            warnings.warn(
                f"[enrich] src callable not found for {ptype}.{attr}: {src!r}",
                stacklevel=4,
            )
            return collection
        kwargs: dict = attr_info.get("kwargs") or {}
        if kwargs:
            method = partial(method, **kwargs)
        result = method(collection)
        collection = ak.with_field(collection, result, attr)
    except Exception as exc:
        warnings.warn(
            f"[enrich] src application failed for {ptype}.{attr} "
            f"(src={src!r}): {exc}",
            stacklevel=4,
        )
    return collection


def _apply_filter(
    collection: ak.Array,
    filter_expr: str,
    ptype: str,
) -> ak.Array:
    """Evaluate *filter_expr* as a boolean mask and filter the collection."""
    try:
        ns = _build_ns(collection)
        mask = eval(filter_expr, ns)  # noqa: S eval
        collection = collection[mask]
    except Exception as exc:
        warnings.warn(
            f"[enrich] filter evaluation failed for {ptype} "
            f"(filter={filter_expr!r}): {exc}",
            stacklevel=4,
        )
    return collection


def _apply_sorter(
    collection: ak.Array,
    sorter: dict,
    ptype: str,
) -> ak.Array:
    """Sort particles within each event by the field named in *sorter['by']*."""
    by: str | None = sorter.get("by")
    if not by:
        return collection
    reverse: bool = sorter.get("reverse", False)
    try:
        sort_key = collection[by]
        order = ak.argsort(sort_key, ascending=not reverse, axis=1)
        collection = collection[order]
    except Exception as exc:
        warnings.warn(
            f"[enrich] sorter failed for {ptype} (by={by!r}): {exc}",
            stacklevel=4,
        )
    return collection
