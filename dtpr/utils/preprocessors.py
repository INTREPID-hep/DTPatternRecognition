"""Columnar preprocessors — functions with signature
``(events: ak.Array, **kwargs) -> ak.Array``.

Each function receives the full event array and must return a (possibly
modified) event array.  Fields can be added via ``ak.with_field``.
"""

from __future__ import annotations

import math
from typing import Optional

import awkward as ak
import numpy as np


def test_preprocessor(
    events: ak.Array,
    dummy_val: Optional[float] = -999.0,
) -> ak.Array:
    """Add a per-event ``dR`` field between the first two gen-muons.

    Events with fewer than two gen-muons get ``dR = dummy_val``.
    """
    genmuons = events["genmuons"]
    has_two = ak.num(genmuons) >= 2

    # Pad ragged genmuon arrays to at least 2 entries (extras become None)
    eta = ak.pad_none(genmuons["eta"], 2, axis=1)
    phi = ak.pad_none(genmuons["phi"], 2, axis=1)

    eta0 = ak.fill_none(eta[:, 0], 0.0)
    eta1 = ak.fill_none(eta[:, 1], 0.0)
    phi0 = ak.fill_none(phi[:, 0], 0.0)
    phi1 = ak.fill_none(phi[:, 1], 0.0)

    d_eta = eta1 - eta0
    d_phi = phi1 - phi0
    # Wrap d_phi to [-pi, pi]
    d_phi = ak.where(d_phi > np.pi, d_phi - 2.0 * np.pi, d_phi)
    d_phi = ak.where(d_phi < -np.pi, d_phi + 2.0 * np.pi, d_phi)

    dR = np.sqrt(d_eta**2 + d_phi**2)
    dR = ak.where(has_two, dR, float(dummy_val))

    return ak.with_field(events, dR, "dR")
