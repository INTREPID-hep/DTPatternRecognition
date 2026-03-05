"""Example histogram definitions using the columnar dtpr histogram API.

Each entry in ``histos`` is an instance of a class from
:mod:`dtpr.utils.histograms_base` (:class:`Distribution`,
:class:`Efficiency`, :class:`Distribution2D`).

The ``func`` callable receives a **materialised** ``ak.Array`` (one
partition) and must return awkward / numpy arrays — *not* per-event
scalars.  Jagged results are automatically flattened before filling.

These histograms assume:
  - ``events["genmuons"]`` exists with fields ``pt``, ``eta``, ``phi``.
  - ``events["dR"]`` exists (added by the ``add_genmuon_dR`` preprocessor).
  - Events have already been filtered to those with ≥ 2 gen-muons (via
    the ``select-has-genmuons`` pre-step), so ``[:, 0]`` and ``[:, 1]``
    are always valid.
"""

import hist
from dtpr.utils.histograms_base import Distribution

# ---------------------------------------------------------------------------
# Histogram list
# ---------------------------------------------------------------------------

histos = [
    # --- Leading muon properties
    Distribution(
        name="LeadingMuon_pt",
        axis=hist.axis.Regular(20, 0, 1000, label=r"Leading muon $p_T$ [GeV]"),
        func=lambda events: events["genmuons"]["pt"][:, 0],
    ),
    Distribution(
        name="LeadingMuon_eta",
        axis=hist.axis.Regular(10, -3, 3, label=r"Leading muon $\eta$"),
        func=lambda events: events["genmuons"]["eta"][:, 0],
    ),
    # --- Subleading muon properties
    # (safe after select-has-genmuons pre-step guarantees ≥2 gen-muons)
    Distribution(
        name="SubLeadingMuon_pt",
        axis=hist.axis.Regular(20, 0, 1000, label=r"Subleading muon $p_T$ [GeV]"),
        func=lambda events: events["genmuons"]["pt"][:, 1],
    ),
    Distribution(
        name="SubLeadingMuon_eta",
        axis=hist.axis.Regular(10, -3, 3, label=r"Subleading muon $\eta$"),
        func=lambda events: events["genmuons"]["eta"][:, 1],
    ),
    # --- Muon dR  (computed by add_genmuon_dR preprocessor)
    Distribution(
        name="muon_DR",
        axis=hist.axis.Regular(20, 1, 6, label=r"$\Delta R$ (both muons)"),
        func=lambda events: events["dR"],
    ),
]
