"""Columnar selectors — functions with signature
``(events: ak.Array) -> ak.Array``.

Each function receives the full event array and must return a boolean
``ak.Array`` of the same length.  Events where the mask is ``False`` are
dropped by :class:`~dtpr.base.ntuple.NTuple`.
"""

from __future__ import annotations

import awkward as ak


def test_selector(events: ak.Array) -> ak.Array:
    """Keep only events that contain at least two gen-muons."""
    return ak.num(events["genmuons"]) > 1
