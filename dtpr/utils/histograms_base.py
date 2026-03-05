"""Base classes for columnar histogram definitions.

All histogram types share:
  - A ``fill(events)`` method that accepts a materialised ``ak.Array``
    (one partition).  Internally the method calls ``self.func`` — a
    **user-supplied columnar function** — and fills the underlying
    ``hist.Hist`` objects.
  - An ``empty_clone()`` method that creates an empty histogram with
    the same binning and function, used by the parallel map step.
  - An ``__add__`` operator for the reduce step
    (``result = clone_from_partition_0 + clone_from_partition_1``).
  - ``save(path)`` / ``load(path)`` helpers for pickle serialisation.

Usage
-----
Define histograms in a source module that exposes a ``histos`` list::

    import hist
    from dtpr.utils.histograms_base import Distribution, Efficiency

    histos = [
        Distribution(
            name="LeadingMuon_pt",
            axis=hist.axis.Regular(20, 0, 1000, label=r"Leading muon $p_T$"),
            func=lambda events: events["genmuons"]["pt"][:, 0],
        ),
        Efficiency(
            name="fwshower_eff_MB1",
            axis=hist.axis.Regular(5, -2.5, 2.5, label="Wheel"),
            func=lambda events: (
                events["segments"]["wh"][events["segments"]["st"] == 1],   # values
                events["segments"]["has_shower"][events["segments"]["st"] == 1],  # mask
            ),
        ),
    ]

For parametrised families (e.g. one histogram per station), use the
:func:`foreach` helper::

    from dtpr.utils.histograms_base import foreach, Efficiency

    histos += foreach(
        cls=Efficiency,
        name_template="fwshower_eff_MB{st}",
        param="st",
        values=[1, 2, 3, 4],
        axis=hist.axis.Regular(5, -2.5, 2.5, label="Wheel"),
        func=lambda events, *, st: (
            events["segments"]["wh"][events["segments"]["st"] == st],
            events["segments"]["has_shower"][events["segments"]["st"] == st],
        ),
    )
"""

from __future__ import annotations

import os
import pickle
from abc import ABC, abstractmethod
from typing import Any

import awkward as ak
import hist


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class HistogramBase(ABC):
    """Abstract base for all dtpr histogram types.

    Subclasses must implement :meth:`fill`, :meth:`empty_clone`, and
    :meth:`__add__`.  :meth:`save` and :meth:`load` are provided here
    as pickle-based convenience helpers.
    """

    name: str

    @abstractmethod
    def fill(self, events: ak.Array) -> None:
        """Fill histogram(s) from a columnar (materialised) events array."""

    @abstractmethod
    def empty_clone(self) -> "HistogramBase":
        """Return an empty histogram with the same axes and fill function."""

    @abstractmethod
    def __add__(self, other: "HistogramBase") -> "HistogramBase":
        """Merge two histograms filled from different partitions."""

    def save(self, path: str) -> None:
        """Pickle this histogram to *path* (creates parent dirs if needed)."""
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, "wb") as f:
            pickle.dump(self, f)

    @classmethod
    def load(cls, path: str) -> "HistogramBase":
        """Load a pickled histogram from *path*."""
        with open(path, "rb") as f:
            return pickle.load(f)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={self.name!r})"


# ---------------------------------------------------------------------------
# Concrete types
# ---------------------------------------------------------------------------

class Distribution(HistogramBase):
    """1-D distribution histogram.

    Parameters
    ----------
    name : str
        Unique name — used as the ROOT branch name when saving.
    axis : hist.axis.*
        Binning axis (any boost-histogram / hist axis type).
    func : callable
        ``fn(events: ak.Array) -> ak.Array``

        Columnar function.  The return value may be jagged (e.g.
        ``events["digis"]["wh"]``, shape ``var * int``); it is flattened
        before filling.
    """

    def __init__(self, name: str, axis: Any, func) -> None:
        self.name = name
        self._axis = axis
        self.func = func
        self.h = hist.Hist(axis, name=name)

    def fill(self, events: ak.Array) -> None:
        vals = self.func(events)
        flat = ak.to_numpy(ak.flatten(vals, axis=None))
        self.h.fill(flat)

    def empty_clone(self) -> "Distribution":
        clone = Distribution.__new__(Distribution)
        clone.name = self.name
        clone._axis = self._axis
        clone.func = self.func
        clone.h = hist.Hist(self._axis, name=self.name)
        return clone

    def __add__(self, other: "Distribution") -> "Distribution":
        result = self.empty_clone()
        result.h = self.h + other.h
        return result


class Efficiency(HistogramBase):
    """Efficiency histogram (numerator + denominator pair).

    Parameters
    ----------
    name : str
        Base name.  The denominator histogram is stored as
        ``{name}_den`` and the numerator as ``{name}_num``.
    axis : hist.axis.*
        Binning axis shared by numerator and denominator.
    func : callable
        ``fn(events: ak.Array) -> tuple[ak.Array, ak.Array]``

        Returns ``(values, passes)``.  Both arrays may be jagged and
        will be flattened together.  ``passes`` must be castable to
        ``bool``.

    Examples
    --------
    ::

        Efficiency(
            name="tp_eff_MB1",
            axis=hist.axis.Regular(5, -2.5, 2.5, label="Wheel"),
            func=lambda events: (
                # denominator: wheel of all segments in station 1
                events["segments"]["wh"][events["segments"]["st"] == 1],
                # numerator mask: those that have a matched TP
                ak.num(events["segments"]["matched_tps"], axis=-1)[events["segments"]["st"] == 1] > 0,
            ),
        )
    """

    def __init__(self, name: str, axis: Any, func) -> None:
        self.name = name
        self._axis = axis
        self.func = func
        self.den = hist.Hist(axis, name=f"{name}_den")
        self.num = hist.Hist(axis, name=f"{name}_num")

    def fill(self, events: ak.Array) -> None:
        vals, passes = self.func(events)
        v = ak.to_numpy(ak.flatten(vals,   axis=None))
        p = ak.to_numpy(ak.flatten(passes, axis=None)).astype(bool)
        self.den.fill(v)
        self.num.fill(v[p])

    def empty_clone(self) -> "Efficiency":
        clone = Efficiency.__new__(Efficiency)
        clone.name = self.name
        clone._axis = self._axis
        clone.func = self.func
        clone.den = hist.Hist(self._axis, name=f"{self.name}_den")
        clone.num = hist.Hist(self._axis, name=f"{self.name}_num")
        return clone

    def __add__(self, other: "Efficiency") -> "Efficiency":
        result = self.empty_clone()
        result.den = self.den + other.den
        result.num = self.num + other.num
        return result


class Distribution2D(HistogramBase):
    """2-D distribution histogram.

    Parameters
    ----------
    name : str
    axis_x, axis_y : hist.axis.*
    func : callable
        ``fn(events: ak.Array) -> tuple[ak.Array, ak.Array]``

        Returns ``(x_values, y_values)``.  Both arrays are flattened
        together before filling.
    """

    def __init__(self, name: str, axis_x: Any, axis_y: Any, func) -> None:
        self.name = name
        self._axis_x = axis_x
        self._axis_y = axis_y
        self.func = func
        self.h = hist.Hist(axis_x, axis_y, name=name)

    def fill(self, events: ak.Array) -> None:
        x, y = self.func(events)
        xf = ak.to_numpy(ak.flatten(x, axis=None))
        yf = ak.to_numpy(ak.flatten(y, axis=None))
        self.h.fill(xf, yf)

    def empty_clone(self) -> "Distribution2D":
        clone = Distribution2D.__new__(Distribution2D)
        clone.name = self.name
        clone._axis_x = self._axis_x
        clone._axis_y = self._axis_y
        clone.func = self.func
        clone.h = hist.Hist(self._axis_x, self._axis_y, name=self.name)
        return clone

    def __add__(self, other: "Distribution2D") -> "Distribution2D":
        result = self.empty_clone()
        result.h = self.h + other.h
        return result


# ---------------------------------------------------------------------------
# Factory helper for parametrised families
# ---------------------------------------------------------------------------

def foreach(cls, name_template: str, param: str, values, **kwargs) -> list[HistogramBase]:
    """Expand one histogram class into multiple instances over a parameter range.

    Avoids explicit ``for`` loops in histogram definition modules.

    Parameters
    ----------
    cls : type
        One of :class:`Distribution`, :class:`Efficiency`,
        :class:`Distribution2D`.
    name_template : str
        Name pattern with a ``{param}`` placeholder,
        e.g. ``"fwshower_eff_MB{st}"``.
    param : str
        The parameter name used in *name_template* and passed as a
        keyword argument to ``func``.
    values : iterable
        Values to expand over (e.g. ``[1, 2, 3, 4]`` for stations).
    **kwargs
        Additional keyword arguments forwarded to ``cls.__init__``.
        The ``func`` kwarg must accept ``*, {param}`` as a keyword
        argument so each instance gets its own bound value.

    Examples
    --------
    ::

        histos = foreach(
            cls=Efficiency,
            name_template="fwshower_eff_MB{st}",
            param="st",
            values=[1, 2, 3, 4],
            axis=hist.axis.Regular(5, -2.5, 2.5, label="Wheel"),
            func=lambda events, *, st: (
                events["segments"]["wh"][events["segments"]["st"] == st],
                events["segments"]["has_shower"][events["segments"]["st"] == st],
            ),
        )
    """
    base_func = kwargs.pop("func")
    return [
        cls(
            name=name_template.format(**{param: v}),
            func=lambda events, _v=v: base_func(events, **{param: _v}),
            **kwargs,
        )
        for v in values
    ]


# ---------------------------------------------------------------------------
# I/O helpers: write a list of histograms to ROOT or pickle
# ---------------------------------------------------------------------------

def save_to_root(histos: list[HistogramBase], path: str) -> None:
    """Write a list of histograms to a ROOT file using uproot.

    ``hist.Hist`` objects (which inherit from ``boost_histogram.Histogram``)
    are written natively by uproot 5.  Existing files are overwritten.

    Parameters
    ----------
    histos : list[HistogramBase]
    path : str
        Output ``.root`` file path.
    """
    import uproot

    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with uproot.recreate(path) as f:
        for h in histos:
            if isinstance(h, Efficiency):
                f[f"{h.name}_num"] = h.num
                f[f"{h.name}_den"] = h.den
            elif isinstance(h, (Distribution, Distribution2D)):
                f[h.name] = h.h
            else:
                raise TypeError(
                    f"Cannot write histogram of type {type(h).__name__!r} to ROOT. "
                    "Only Distribution, Distribution2D, and Efficiency are supported."
                )
