"""
NTuple — columnar ROOT file loader using coffea + Awkward Arrays.

Two-pass pipeline:

Pass 1 (structural): :class:`~dtpr.base.schema.PatternSchema` maps flat ROOT
  branches into nested Awkward particle records.

Pass 2 (enrichment): :func:`~dtpr.base.enrichment.enrich` applies ``expr`` /
  ``src`` computed attributes, sorters, and filters declared in the YAML config.

Public interface:
  ``ntuple.events[i]``           → single :class:`~dtpr.base.event.EventRecord`
  ``len(ntuple.events)``         → total event count
  ``for ev in ntuple.events``    → iteration over EventRecords
  ``ntuple.events.get_by_number(n)``

Input formats (passed straight to ``uproot`` / ``coffea``):
  - ``"file.root"``                              single file
  - ``"/dir/*.root"``                            glob pattern
  - ``"/dir/"``                                  directory (expands to ``/dir/*.root``)
  - ``["a.root", "b.root"]``                     list of files
  - ``{"/dir/*.root": "tree", ...}``             dict (uproot-native, treepath explicit)
  - ``{"/f.root": {"object_path": "t", "steps": [...]}, ...}``  chunked dict
"""

from __future__ import annotations

import glob as _glob
import os
import warnings
from functools import partial

from coffea.nanoevents import NanoEventsFactory
from natsort import natsorted

from .config import RUN_CONFIG
from .event_list import EventList
from .schema import PatternSchema
from ..utils.functions import color_msg, get_callable_from_src


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_allowed_branches(config) -> set[str]:
    """Build the branch allow-list purely from config — no file I/O.

    Collects:
    1. Every ``branch:`` value declared under ``particle_types`` attributes.
    2. Every entry in the ``event_branches`` list in the config.
    3. Every entry in the optional ``extra_branches`` list in the config.
    """
    allowed: set[str] = set()

    for ptype, pinfo in (getattr(config, "particle_types", None) or {}).items():
        if not isinstance(pinfo, dict):
            continue
        for attr_name, attr_info in (pinfo.get("attributes") or {}).items():
            if isinstance(attr_info, dict):
                branch = attr_info.get("branch")
                if branch:
                    allowed.add(branch)

    for branch in (getattr(config, "event_branches", None) or []):
        allowed.add(branch)

    for branch in (getattr(config, "extra_branches", None) or []):
        allowed.add(branch)

    return allowed


def _preprocess_input(input_path, treepath: str, maxfiles: int = -1):
    """Normalise *input_path* into an uproot-native form.

    Handles two special cases that uproot does not handle natively:
    - A bare directory string → expanded to a ``{file: treepath}`` dict
      (with optional ``maxfiles`` slicing and natural-sort ordering).
    - A list of file paths → converted to a ``{file: treepath}`` dict.

    Everything else (single file str, glob str, explicit dict) is passed
    through after injecting the treepath into plain strings that lack one.
    """
    if isinstance(input_path, dict):
        # Already fully specified by the caller, pass through as-is.
        return input_path

    if isinstance(input_path, list):
        files = input_path
        if maxfiles > 0:
            files = files[:maxfiles]
        return {f: treepath for f in files}

    if isinstance(input_path, str):
        if os.path.isdir(input_path):
            # Expand directory → natural-sorted list of .root files.
            files = natsorted(_glob.glob(os.path.join(input_path, "*.root")))
            if not files:
                raise FileNotFoundError(f"No .root files found in directory: {input_path}")
            if maxfiles > 0:
                files = files[:maxfiles]
            return {f: treepath for f in files}

        # Glob pattern (contains * or ?): expand to individual file paths.
        # Handles both "/path/*.root" and "/path/*.root:TREENAME" forms.
        if "*" in input_path or "?" in input_path:
            if ":" in input_path:
                # Strip an embedded treepath suffix (last colon-separated segment).
                glob_part, _embedded_tree = input_path.rsplit(":", 1)
            else:
                glob_part = input_path
            files = natsorted(_glob.glob(glob_part))
            if not files:
                raise FileNotFoundError(f"No .root files matched glob: {glob_part}")
            if maxfiles > 0:
                files = files[:maxfiles]
            return {f: treepath for f in files}

        # Single file — inject treepath if not already present.
        if ":" not in input_path:
            return f"{input_path}:{treepath}"
        return input_path

    # pathlib.Path or already-opened uproot object — pass through unchanged.
    return input_path


# ---------------------------------------------------------------------------
# NTuple class
# ---------------------------------------------------------------------------

class NTuple:
    """
    Columnar NTuple loader.  Loads one or more ROOT files lazily via
    ``coffea.NanoEventsFactory`` + :class:`~dtpr.base.schema.PatternSchema`
    using ``uproot.dask`` as the backend (handles any number of files natively).

    Parameters
    ----------
    inputFolder : str, list, or dict
        Passed directly to ``NanoEventsFactory.from_root`` after minimal
        normalisation (see :func:`_preprocess_input`).  Accepted formats:

        * ``"path/to/file.root"``              — single file
        * ``"/path/to/dir/"``                  — directory (glob-expanded)
        * ``"/path/*.root"``                   — glob pattern
        * ``["a.root", "b.root", ...]``        — list of paths
        * ``{"/path/*.root": "treepath", ...}`` — uproot-native dict
    selectors : list of callables, optional
        Columnar selector functions ``(ak.Array) → bool array``.
    preprocessors : list of callables, optional
        Columnar preprocessor functions ``(ak.Array) → ak.Array``.
    maxfiles : int, optional
        Maximum number of files when *inputFolder* is a directory or list.
        ``-1`` (default) loads all files.
    CONFIG : Config, optional
        A :class:`~dtpr.base.config.Config` instance.  Defaults to the global
        ``RUN_CONFIG``.
    """

    def __init__(
        self,
        inputFolder,
        selectors=None,
        preprocessors=None,
        maxfiles: int = -1,
        CONFIG=None,
    ):
        self._selectors = list(selectors) if selectors is not None else []
        self._preprocessors = list(preprocessors) if preprocessors is not None else []
        self._maxfiles = maxfiles
        self.CONFIG = CONFIG if CONFIG is not None else RUN_CONFIG

        # Resolve tree name from config
        _tree_name = getattr(self.CONFIG, "ntuple_tree_name", None)
        if _tree_name is None:
            warnings.warn("No ntuple_tree_name in CONFIG. Defaulting to '/TTREE'.")
            self._tree_name = "/TTREE"
        else:
            self._tree_name = _tree_name

        # Load selectors / preprocessors declared in the config YAML.
        self._load_from_config("ntuple_selectors")
        self._load_from_config("ntuple_preprocessors")

        # Build events array
        self.events = self._load_events(inputFolder)

    # ------------------------------------------------------------------
    # Internal loading pipeline
    # ------------------------------------------------------------------

    def _load_events(self, input_path) -> EventList:
        """Run the two-pass loading pipeline and return an :class:`EventList`.

        Uses ``mode='dask'`` exclusively, which delegates to ``uproot.dask``
        internally and therefore handles 1 or N files natively without any
        per-file loop or ``ak.concatenate``.

        Branch filtering must be passed via ``uproot_options["filter_name"]``
        (not ``iteritems_options``) because dask mode bypasses the
        ``tree.iteritems`` path used by virtual/eager modes.

        After the full pipeline the array's partition sizes are materialised
        with ``eager_compute_divisions()`` so that ``len()`` works without a
        full compute.  Individual event access (``events[i]``) is handled
        lazily by ``EventList.__getitem__``.
        """
        clean_treepath = self._tree_name.lstrip("/")

        # Normalise input → dict {file: treepath} or a plain "file:treepath" string.
        processed = _preprocess_input(input_path, clean_treepath, self._maxfiles)
        color_msg(f"Loading events from: {input_path}", "blue", 1)

        # Build branch allow-list purely from config (no file I/O).
        allowed = _extract_allowed_branches(self.CONFIG)
        schema_cls = PatternSchema.with_config(self.CONFIG)

        # uproot.dask handles multiple files natively.
        # filter_name goes into uproot_options (not iteritems_options).
        raw_events = NanoEventsFactory.from_root(
            processed,
            schemaclass=schema_cls,
            mode="dask",
            uproot_options={"filter_name": list(allowed)},
        ).events()

        # Pass 2 — enrichment (computed fields, sorters, filters)
        from .enrichment import enrich
        raw_events = enrich(raw_events, self.CONFIG)

        # Apply columnar preprocessors
        for pp in self._preprocessors:
            raw_events = pp(raw_events)

        # Apply columnar selectors
        for sel in self._selectors:
            raw_events = raw_events[sel(raw_events)]

        # Materialise partition sizes so that len() works without a full compute.
        raw_events.eager_compute_divisions()

        return EventList(raw_events)

    # ------------------------------------------------------------------
    # Config loading (selectors / preprocessors)
    # ------------------------------------------------------------------

    def _load_from_config(self, config_key: str):
        """Load callables from the YAML config into the selector/preprocessor lists."""
        if "selector" in config_key:
            target_list = self._selectors
            item_type = "selector"
        elif "preprocessor" in config_key:
            target_list = self._preprocessors
            item_type = "preprocessor"
        else:
            raise ValueError(
                f"Invalid config_key '{config_key}'. Must contain 'selector' or 'preprocessor'."
            )

        items: list = []
        for name, item_info in getattr(self.CONFIG, config_key, {}).items():
            src = item_info.get("src")
            if src is None:
                raise ValueError(f"{item_type.capitalize()} '{name}' has no src in config.")
            item = get_callable_from_src(src)
            if item is None:
                raise ImportError(f"{item_type.capitalize()} '{name}' not found: {src}")
            kwargs = item_info.get("kwargs") or {}
            items.append(partial(item, **kwargs) if kwargs else item)

        if items:
            target_list.extend(items)


if __name__ == "__main__":
    import os as _os
    _input = "/mnt/c/Users/estradadaniel/cernbox/ZprimeToMuMu_M-6000_TuneCP5_14TeV-pythia8/ZprimeToMuMu_M-6000_PU200/v1/0000/"
    # _os.path.abspath(
    #     _os.path.join(
    #         _os.path.dirname(__file__),
    #         "../../tests/ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root",
    #     )
    # )
    ntuple = NTuple(_input)
    print(f"Loaded {len(ntuple.events)} events")
    ev0 = ntuple.events[0]
    print(ev0)
    print("digis[0]:", ev0["digis"][0])

