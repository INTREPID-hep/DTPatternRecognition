"""Columnar event selectors — functions with signature
``fn(events: ak.Array) -> ak.Array[bool]``.

Each function receives the full event array and returns a boolean array of
length ``nevents``.  Use these as ``src:`` targets from a pipeline selector
step with no ``target`` key (event-level filtering).
"""

from __future__ import annotations

import awkward as ak


def baseline(events: ak.Array) -> ak.Array:
    """Baseline filter: keep events that have at least one gen-muon with a
    matched offline segment.

    Conditions (both must hold per event):
    * ``ak.num(genmuons) >= 1``
    * at least one gen-muon has ``ak.num(matched_segments) >= 1``

    Requires ``genmuons.matched_segments`` to exist on the event array
    (added by a matching preprocessor step that runs before this selector).

    Returns a boolean ``ak.Array`` of length ``nevents``.
    """
    has_muon = ak.num(events["genmuons"], axis=1) > 0
    has_match = ak.any(ak.num(events["genmuons"]["matched_segments"], axis=2) > 0, axis=1)
    return has_muon & has_match
