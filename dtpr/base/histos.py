"""Histogram module — ``Histogram`` wrapper + fill / I/O helpers.

The core class is :class:`Histogram`, which pairs a ``hist.dask.Hist``
with a fill function.  The fill function maps an events array to a dict of named arrays, which
are then passed as kwargs to the underlying histogram's ``fill()`` method.  The type of the events 
array determines the fill path:

- ``dask_awkward.Array`` → lazy fill via ``hist.dask.Hist``; builds a Dask task graph inside the 
  histogram, no compute triggered.  Call ``dask.compute(histo.h)`` (or use the module-level :func:`fill`) 
  to materialise.
- Any other array type (e.g. ``awkward.Array``, numpy) → synchronous fill via a ``hist.Hist`` 
  accumulator (no Dask).  All proxied attributes (``values()``, ``plot1d()``, etc.) then reflect 
  this eagerly filled histogram.

Module-level functions
----------------------
fill        : lazy or eager fill + optional ROOT output
to_root     : write materialised (or eagerly filled) histograms to ROOT
from_config : load a ``histos`` list from a dotted module path
expand      : parametrised histogram factory (replaces ``foreach``)

Lazy fill mechanics
-------------------
``histo.h`` is already a ``hist.dask.Hist`` from ``__init__``.
Calling ``histo.fill(dak_events)`` calls ``self.h.fill(**dict_of_dak_cols)``
which builds a Dask task graph inside ``self.h`` — **no compute**.

``dask.compute(*[h.h for h in histos])`` triggers a **single** scheduler
call that optimises the shared task graph and returns ``list[hist.Hist]``
(one materialised histogram per wrapper).

Per-partition fill
------------------
Instead of an in-memory tree-reduce, ``events.to_delayed()`` slices the 
array into individual delayed partitions. Each partition is passed to a 
delayed worker (``_write_partition_histos``) as an eager ``awkward.Array``. 
The worker fills standard ``hist.Hist`` objects locally and writes them 
directly to disk, making the process highly performant and resume-safe.
"""

from __future__ import annotations

import os
import warnings
from importlib import import_module
from typing import Callable

import dask
import hist
import hist.dask
import uproot
import awkward as ak

from .config import RUN_CONFIG, Config
from ..utils.functions import color_msg, create_outfolder, make_dask_sched_kwargs, _is_dak
from ..utils.tqdm import ProgressBarFactory

# ---------------------------------------------------------------------------
# Histogram wrapper
# ---------------------------------------------------------------------------

class Histogram:
    """Thin wrapper pairing a ``hist.dask.Hist`` with a fill function.

    Parameters
    ----------
    *axes : hist.axis.*
        Axis definitions forwarded directly to ``hist.dask.Hist``.
        Axis ``name`` attributes must match the keys returned by *func*.
    func : callable(events) -> dict[str, array]
        Maps an events array to a ``dict`` of named arrays.
        Keys must match the ``name`` attribute of each axis in ``*axes``.
    name : str
        Human label for logging and ROOT output.
        Defaults to the ``name`` attribute of the first axis.
    storage : bh.storage.*, optional
        boost-histogram storage type (e.g. ``hist.storage.Weight()``).
        Defaults to ``hist.storage.Double()``.

    Notes
    -----
    All ``hist.dask.Hist`` attributes and methods are transparently
    proxied via ``__getattr__``:  ``histo.plot1d()``, ``histo.axes``,
    ``histo.values()``, ``histo.reset()``, slicing, ``histo.to_hist()``,
    etc. all work directly on the wrapper.

    Examples
    --------
    1-D::

        Histogram(
            hist.axis.Regular(20, 0, 1000, name="pt", label=r"$p_T$ [GeV]"),
            func=lambda ev: {"pt": ev["genmuons"]["pt"][:, 0]},
            name="LeadingMuon_pt",
        )

    Efficiency (``hist.axis.Boolean`` — 2-bin True/False)::

        Histogram(
            hist.axis.Regular(5, -2.5, 2.5, name="wh", label="Wheel"),
            hist.axis.Boolean(name="pass", label="Has matched TP"),
            func=lambda ev: {
                "wh":   ev["segments"]["wh"][ev["segments"]["st"] == 1],
                "pass": ak.num(ev["segments"]["matched_tps"], axis=-1)
                              [ev["segments"]["st"] == 1] > 0,
            },
            name="eff_MB1",
        )

    Weighted::

        Histogram(
            hist.axis.Regular(20, 0, 1000, name="pt"),
            func=lambda ev: {"pt": ev["genmuons"]["pt"][:, 0],
                              "weight": ev["weight"]},
            name="LeadingMuon_pt_weighted",
            storage=hist.storage.Weight(),
        )
    """

    def __init__(
        self,
        *axes,
        func: Callable,
        name: str = "",
        storage=None,
    ) -> None:
        kw = {"storage": storage} if storage else {}
        self.func = func
        self.name = name or axes[0].name
        self._h = hist.dask.Hist(*axes, **kw)
        # Accumulator for synchronous (ak.Array / numpy) fills — created on
        # first use so it carries the same axes + storage as self.h.
        self._eager_h: hist.Hist | None = None

    def fill(self, events, **kwargs) -> None:
        """Fill from events.

        Calls ``self.func(events)`` -> dict of named arrays, then fills the
        appropriate underlying histogram:

        - ``dak.Array`` inputs -> lazy fill via ``hist.dask.Hist``; builds a
          Dask task graph inside ``self.h``, no compute triggered.
          Call ``dask.compute(histo.h)`` (or use the module-level
          :func:`fill`) to materialise.
        - Any other input (``ak.Array``, numpy, ...) -> synchronous fill via a
          ``hist.Hist`` accumulator (``self._eager_h``).  All proxied
          attributes (``values()``, ``plot1d()``, etc.) then reflect this
          eagerly filled histogram.

        Extra keyword arguments (``weight=``, ``sample=``, ``threads=``, ...)
        are forwarded to the underlying ``fill()`` call.
        """
        result = self.func(events)
        if _is_dak(events):
            # Lazy path -- hist.dask.Hist accumulates the Dask task graph.
            self._h.fill(**result, **kwargs)
        else:
            # Eager path -- plain hist.Hist for synchronous filling.
            if self._eager_h is None:
                self._eager_h = hist.Hist(
                    *list(self._h.axes),
                    storage=self._h.storage_type(),
                )
            self._eager_h.fill(**result, **kwargs)

    def __getattr__(self, name: str):
        # Proxy attribute lookups to the appropriate underlying histogram:
        #   - _eager_h (hist.Hist) when populated via eager fills.
        #   - self._h   (hist.dask.Hist) otherwise (lazy / unfilled).
        # Only invoked when the normal per-instance attribute lookup fails.

        if name.startswith("__dask_"):
            # Prevent Dask from duck-typing this wrapper as a task graph!
            raise AttributeError(name)

        eager_h = self.__dict__.get("_eager_h")
        if eager_h is not None:
            return getattr(eager_h, name)
        try:
            return getattr(self.__dict__["_h"], name)
        except KeyError:
            raise AttributeError(name)

    def __repr__(self) -> str:
        return f"Histogram(name={self.name!r})"

    @property
    def h(self) -> hist.Hist | hist.dask.Hist:
        """Return the active underlying histogram object."""
        return self._eager_h if self._eager_h is not None else self._h

# ---------------------------------------------------------------------------
# expand — parametrised histogram factory
# ---------------------------------------------------------------------------

def expand(histo: Histogram, **kwargs) -> list[Histogram]:
    """Build a list of :class:`Histogram` objects over a parameter range.

    Parameters
    ----------
    histo : Histogram
        Template histogram to use for each value. `func` should be written to accept the parameter as a kwarg, e.g. ``st=1``.
    **kwargs : dict
        Parameter values to expand over (e.g. station numbers ``st=[1, 2, 3, 4]``).

    Examples
    --------
    ::

        import hist, awkward as ak
        from dtpr.base.histos import expand, Histogram

        histos += expand(
            histo=Histogram(
                hist.axis.Regular(5, -2.5, 2.5, name="wh"),
                hist.axis.Boolean(name="pass"),
                func=lambda ev, st=st: {
                    "wh":   ev["segments"]["wh"][ev["segments"]["st"] == st],
                    "pass": ak.num(ev["segments"]["matched_tps"], axis=-1)
                                  [ev["segments"]["st"] == st] > 0,
                },
                name=f"eff_MB{st}",
            ),
            st=[1, 2, 3, 4],
        )
    """
    return [
        Histogram(
            *histo.h.axes,
            func=lambda ev, v=v: histo.func(ev, **{k: v for k in kwargs}),
            name=histo.name.format(**{k: v for k, v in kwargs}),
            storage=histo.h.storage_type(),
        ) for v in kwargs.values()
    ]


# ---------------------------------------------------------------------------
# from_config — load histogram list from dotted module path or RUN_CONFIG
# ---------------------------------------------------------------------------

def from_config(config_src: str | None = None, config: Config = None) -> list[Histogram]:
    """Load a ``histos`` list from a dotted Python module path.

    Parameters
    ----------
    config_src : str or None
        Dotted module path, e.g. ``"my_analysis.histos"``.
        The module must expose a ``histos`` attribute that is a list of
        :class:`Histogram` instances.

        When ``None`` (default) the function falls back to reading
        ``histo_sources`` and ``histo_names`` from
        :data:`~dtpr.base.config.RUN_CONFIG`.

    Returns
    -------
    list[Histogram]
        Histogram instances ready to be filled.

    Warnings
    --------
    - If a source module has no valid ``histos`` attribute.
    - If any requested histogram names are not found in any source.
    """
    if config_src is not None:
        # Simple path: import the module and return its histos list.
        module = import_module(config_src)
        result = getattr(module, "histos", [])
        return result if isinstance(result, list) else [result]

    cfg = config or RUN_CONFIG
    h_cfg = getattr(cfg, "histograms", None) or cfg

    if isinstance(h_cfg, dict):
        sources = h_cfg.get("histo_sources") or []
        names_filter = set(h_cfg.get("histo_names") or [])
    else:
        sources = list(getattr(h_cfg, "histo_sources", None) or [])
        names_filter = set(getattr(h_cfg, "histo_names", None) or [])

    all_histos: list[Histogram] = []
    for source in sources:
        module = import_module(source)
        module_histos = getattr(module, "histos", [])
        if isinstance(module_histos, Histogram):
            module_histos = [module_histos]
        elif not isinstance(module_histos, list):
            warnings.warn(
                f"Source module {source!r} has no 'histos' attribute or it is not "
                "a list / Histogram instance. Skipping.",
                stacklevel=2,
            )
            continue
        all_histos.extend(module_histos)

    if names_filter:
        all_histos = [h for h in all_histos if h.name in names_filter]
        found = {h.name for h in all_histos}
        missing = names_filter - found
        if missing:
            warnings.warn(
                f"The following histograms were not found in any source: "
                f"{', '.join(sorted(missing))}",
                stacklevel=2,
            )

    return all_histos


# ---------------------------------------------------------------------------
# to_root — write histograms to a ROOT file
# ---------------------------------------------------------------------------

def to_root(histos, path: str) -> None:
    """Write histograms to a ROOT file via uproot.

    Accepts :class:`Histogram` wrappers, dictionaries of wrappers, 
    or raw ``hist.Hist`` / ``bh.Histogram`` objects. uproot's UHI dispatch
    auto-selects ``TH1D`` / ``TH2D`` / ``TH3D`` (up to 3 dimensions).

    If a histogram contains a ``hist.axis.Boolean`` axis (e.g., for efficiency), 
    it is automatically split into two ROOT histograms: 
    ``<name>_num`` (True) and ``<name>_den`` (sum of True + False).

    Parameters
    ----------
    histos : dict, list, Histogram, or hist.Hist
        The histogram(s) to save. Can be a single object, a list of objects, 
        or a dictionary mapping names to objects.
    path : str
        Output ``.root`` file path. Parent directories are created if needed.
    """
    create_outfolder(os.path.dirname(os.path.abspath(path)))
    
    # 1. Normalize input into an iterable of (name, histogram) pairs
    if isinstance(histos, dict):
        items = histos.items()
    else:
        # Wrap single objects in a list so we can safely iterate
        if not isinstance(histos, (list, tuple)):
            histos = [histos]
        # Auto-generate names for list items if they don't have one
        items = ((getattr(h, "name", None) or str(id(h)), h) for h in histos)

    # 2. Open file and write (Zero repetition!)
    with uproot.recreate(path) as f:
        for name, h in items:
            # Extract hist object
            obj = h.h if isinstance(h, Histogram) else h
            
            # Dynamically search for a Boolean axis
            bool_axis_name = None
            for ax in obj.axes:
                if isinstance(ax, hist.axis.Boolean):
                    bool_axis_name = ax.name
                    break  # Found it, stop looking
            
            if bool_axis_name:
                # UHI dict-slicing: Safely slices the axis by name, regardless of position!
                f[f"{name}_num"] = obj[{bool_axis_name: True}]
                f[f"{name}_den"] = obj[{bool_axis_name: sum}]
            else:
                f[name] = obj

# ---------------------------------------------------------------------------
# _write_partition_histos — delayed helper for per-partition ROOT output
# ---------------------------------------------------------------------------

@dask.delayed
def _write_partition_histos(
    eager_events, 
    histos_config: list[Histogram], 
    out_path: str, 
) -> None:
    """Write one partition's materialised histograms to a ROOT file.

    This is a ``@dask.delayed`` worker called by :func:`fill` in 
    per-partition mode. It receives one fully computed ``awkward.Array`` 
    partition, fills standard ``hist.Hist`` objects locally, and saves them 
    to disk directly.
    """
    # 1. Fill standard (non-dask) histograms locally
    filled_hists = {}
    for h_cfg in histos_config:
        h_eager = hist.Hist(*h_cfg.h.axes, storage=h_cfg.h.storage_type())
        try:
            cols = h_cfg.func(eager_events)
            h_eager.fill(**cols)
        except Exception as exc:
            warnings.warn(f"Problem filling histogram {h_cfg.name!r} in partition: {exc}", stacklevel=2)
        filled_hists[h_cfg.name] = h_eager

    # 2. Save to ROOT
    to_root(filled_hists, out_path)



# ---------------------------------------------------------------------------
# fill — orchestrate lazy or eager fill + ROOT output
# ---------------------------------------------------------------------------

def fill(
    histos: list[Histogram],
    events,
    outfolder: str = "./results",
    tag: str = "",
    per_partition: bool = False,
    overwrite: bool = False,
    ncores: int = -1,
    label: str = "",
    verbose: bool = True,
    save: bool = True,
) -> dict:
    """Fill histograms from events and (optionally) write ROOT output.

    Parameters
    ----------
    histos : list[Histogram]
        Histogram wrapper definitions to fill.
    events : dak.Array or ak.Array
        Event data. The type determines the fill path:

        - ``dak.Array`` → lazy; a single ``dask.compute`` is triggered
          at the end, optimising the shared task graph across all histograms.
        - ``ak.Array``  → synchronous fill via boost-histogram (no Dask).
    outfolder : str
        Root output directory. A ``histograms/`` sub-folder is created.
    tag : str
        String appended to the output filename,
        e.g. ``"_v2"`` → ``histograms_v2.root``.
    per_partition : bool
        (*Lazy path only*) Write one ROOT file per Dask partition under
        ``<outfolder>/histograms/histograms{tag}_NNNN.root``.
        Existing files are skipped (resume-safe).
        Bypasses tree-reduce by mapping delayed tasks over array partitions.
    overwrite : bool
        Re-process and overwrite existing per-partition files.
        Ignored when *per_partition* is ``False``.
    ncores : int
        Scheduler hint: ``1`` = synchronous, ``-1`` = dask default,
        ``>1`` = N local process workers.
    label : str
        Human-readable label for log messages (e.g. dataset name).
    verbose : bool
        Whether to print log messages about the fill progress and output paths.
    save : bool
        Whether to save the filled histograms to ROOT files. Set to ``False`` to
        only perform the fill in-memory and return the results without writing to disk.
    Returns
    -------
    dict[str, hist.Hist]
        Dictionary mapping histogram names to their materialised objects.
        Returns an empty dictionary ``{}`` for per-partition mode (results 
        are saved directly to disk to preserve memory).

    Notes
    -----
    **Lazy (dak.Array) — in-memory path**:
    Each ``histo.fill(events)`` call invokes ``hist.dask.Hist.fill`` with
    ``dak.Array`` kwargs, appending to the internal task graph.
    ``dask.compute(*[h.h for h in histos])`` materialises everything in a
    single scheduler pass. The original wrapper objects are updated **in-place**,
    and a dictionary of the final objects is returned.

    **Lazy (dak.Array) — per-partition path**:
    Uses ``events.to_delayed()`` to slice the array. A delayed worker 
    receives the eager ``awkward.Array``, computes the columns, fills 
    standard histograms locally, and writes directly to disk. Original 
    in-memory wrappers remain empty.

    **Eager (ak.Array)**:
    The wrapper bypasses Dask entirely and routes the array to a synchronous 
    ``hist.Hist`` accumulator. The original wrapper objects are updated 
    **in-place**, and a dictionary of the final objects is returned.
    """
    if not histos:
        raise ValueError("No histograms provided to fill.")

    # ── 1. Sanity Checks ────────────────────────────────────────────────────
    if events is None:
        raise ValueError("No events provided to fill histograms.")

    if len(ak.fields(events)) == 0:
        raise ValueError("Events array has no columns/fields.")

    _is_eager = False
    if not _is_dak(events):
        if len(events) == 0:
            warnings.warn("Events array is empty (0 rows). Skipping histogram fill.", stacklevel=2)
            return {}
        _is_eager = True
    # ────────────────────────────────────────────────────────────────────────

    out_dir = os.path.join(outfolder, "histograms")
    if save or per_partition:
        create_outfolder(out_dir)

    # Log label includes optional user-provided label for context (e.g. dataset name)
    log_label = f"[fill|{label}]" if label else "[fill]"
    # Base kwargs to avoid repeating progress bar boilerplate
    pbar_base = {"show": verbose, "ascii": True, "delay": 0.25} # dealay to avoid pbar of the first dask graph building tasks loop

    def _safe_fill(h, ev):
        """Helper to keep the fill loops clean."""
        try:
            h.fill(ev)
        except Exception as exc:
            raise ValueError(f"Problem filling histogram {h.name!r}: {exc}") from exc

    results = {}

    # ------------------------------------------------------------------
    # Eager path — ak.Array (no Dask at all)
    # ------------------------------------------------------------------

    if _is_eager:
        desc = color_msg(f"{log_label} Filling histograms", "purple", 1, return_str=True)
        pbar_base.pop("delay")  # No need to delay the progress bar in the eager path
        with ProgressBarFactory(mode="eager", total=len(histos), desc=desc, unit=" hist", **pbar_base) as pbar:
            for histo in histos:
                _safe_fill(histo, events)
                pbar.update(1)
        results = {h.name: getattr(h, "obj", h._eager_h) for h in histos}

    # ------------------------------------------------------------------
    # Lazy path — dak.Array
    # ------------------------------------------------------------------
    else:
        sched_kwargs, sched_label = make_dask_sched_kwargs(ncores)
        npartitions = events.npartitions

        if verbose:
            color_msg(f"{log_label} {len(histos)} hist(s) × {npartitions} partition(s) | {sched_label}", "blue", 1)

        # -- Per-partition: bypasses tree-reduce via delayed mapping ----------
        if per_partition:
            if not save:
                warnings.warn("per_partition=True inherently saves files. `save=False` ignored.", stacklevel=2)
            pad = len(str(max(npartitions - 1, 0)))
            tasks = []
            for i, part in enumerate(events.to_delayed()):
                out_path = os.path.join(out_dir, f"histograms{tag}_{str(i).zfill(pad)}.root")
                if not overwrite and os.path.exists(out_path):
                    if verbose:
                        color_msg(f"{log_label} Skipping {out_path} (resume-safe)", "yellow", 1)
                    continue
                tasks.append(_write_partition_histos(part, histos, out_path))

            if not tasks:
                if verbose:
                    color_msg(f"{log_label} All partition files exist. Nothing to do.", "green", 1)
                return {}

            desc = color_msg(f"{log_label} Processing partitions", "purple", 1, return_str=True)
            with ProgressBarFactory(mode="lazy", desc=desc, unit=" part", **pbar_base):
                dask.compute(*tasks, **sched_kwargs)

            if verbose:
                color_msg(f"{log_label} Partitions saved → histograms{tag}_...root", "green", 1)
            return results  # Empty dict; results are saved directly to disk

        # -- In-memory: hist.dask.Hist accumulates the task graph ----------
        # Build graph
        for histo in histos:
            _safe_fill(histo, events)

        # Compute graph
        desc = color_msg(f"{log_label} Processing graph nodes", "purple", 1, return_str=True)
        with ProgressBarFactory(mode="lazy", desc=desc, unit=" node", **pbar_base):
            computed = list(dask.compute(*[h.h for h in histos], **sched_kwargs)) 

        for wrapper, res in zip(histos, computed):
            wrapper._eager_h = res
            results[wrapper.name] = res

    # ------------------------------------------------------------------
    # Unified Save & Return
    # ------------------------------------------------------------------
    if save:
        out_path = os.path.join(out_dir, f"histograms{tag}.root")
        to_root(results, out_path)
        if verbose:
            color_msg(f"{log_label} histograms saved → {out_path}", "green", 1)

    return results
