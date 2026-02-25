"""
NTuple — columnar ROOT file loader using coffea + Awkward Arrays.

Replaces the old ``ROOT.TChain`` row-wise approach with a lazy two-pass pipeline:

Pass 1 (structural): :class:`~dtpr.base.schema.PatternSchema` maps flat ROOT
  branches into nested Awkward particle records.

Pass 2 (enrichment): :func:`~dtpr.base.enrichment.enrich` applies ``expr`` /
  ``src`` computed attributes, sorters, and filters declared in the YAML config.
  (Wired in Step 4 — stub only in this revision.)

Public interface is unchanged:
  ``ntuple.events[i]``           → single :class:`~dtpr.base.event.EventRecord`
  ``len(ntuple.events)``         → total event count
  ``for ev in ntuple.events``    → iteration over EventRecords
  ``ntuple.events.get_by_number(n)``
"""

from __future__ import annotations

import glob
import os
import warnings
from functools import partial

import uproot
from coffea.nanoevents import NanoEventsFactory
from natsort import natsorted

from .config import RUN_CONFIG
from .event_list import EventList
from .schema import PatternSchema
from ..utils.functions import color_msg, get_callable_from_src


# ---------------------------------------------------------------------------
# File-collection helpers
# ---------------------------------------------------------------------------

def _collect_files(input_path: str, maxfiles: int = -1) -> list[str]:
    """Return a sorted list of absolute ``.root`` file paths.

    Accepts a single file, a directory (searched recursively), or a glob
    pattern.

    Parameters
    ----------
    input_path : str
        Path to a single ``.root`` file, a directory, or a glob pattern.
    maxfiles : int
        Maximum number of files to return.  ``-1`` means all.
    """
    if os.path.isfile(input_path):
        files = [os.path.abspath(input_path)]
    elif os.path.isdir(input_path):
        found: list[str] = []
        for entry in os.scandir(input_path):
            if entry.is_file() and entry.name.endswith(".root"):
                found.append(entry.path)
            elif entry.is_dir():
                found.extend(_collect_files(entry.path, maxfiles=-1))
        files = natsorted(found)
    else:
        files = natsorted(
            os.path.abspath(p) for p in glob.glob(input_path) if p.endswith(".root")
        )

    if maxfiles > 0:
        files = files[:maxfiles]
    return files


def _build_allow_list(config, files: list[str], treepath: str) -> set[str]:
    """Assemble the branch allow-list from config + event-level branches.

    Parameters
    ----------
    config : Config
        A :class:`~dtpr.base.config.Config` instance.
    files : list[str]
        The resolved list of ROOT files.  Only the first file is opened to
        sample the available branch names.
    treepath : str
        TTree path inside the ROOT file (leading ``/`` is stripped for uproot).
    """
    allowed: set[str] = set()

    # 1. All branch: values declared in particle_types attributes
    for ptype, pinfo in getattr(config, "particle_types", {}).items():
        if not isinstance(pinfo, dict):
            continue
        for attr_name, attr_info in (pinfo.get("attributes") or {}).items():
            if isinstance(attr_info, dict):
                branch = attr_info.get("branch")
                if branch:
                    allowed.add(branch)

    # 2. All event_* branches present in the first file
    clean_treepath = treepath.lstrip("/")
    try:
        with uproot.open(files[0]) as f:
            tree_keys: set[str] = set(f[clean_treepath].keys())
        allowed |= {b for b in tree_keys if b.startswith("event_")}
    except Exception as exc:
        warnings.warn(f"Could not read branch list from {files[0]}: {exc}")

    # 3. Optional extra_branches from config
    extra = getattr(config, "extra_branches", None) or []
    allowed |= set(extra)

    return allowed


# ---------------------------------------------------------------------------
# NTuple class
# ---------------------------------------------------------------------------

class NTuple:
    """
    Columnar NTuple loader.  Loads one or more ROOT files lazily via
    ``coffea.NanoEventsFactory`` + :class:`~dtpr.base.schema.PatternSchema`.

    Parameters
    ----------
    inputFolder : str
        Path to a single ROOT file, a folder of ROOT files, or a glob pattern.
    selectors : list of callables, optional
        Columnar selector functions ``(ak.Array) → bool array``.  Applied after
        enrichment; events where the mask is ``False`` are dropped.
        *(Step 5 — stub in this revision.)*
    preprocessors : list of callables, optional
        Columnar preprocessor functions ``(ak.Array) → ak.Array``.  Applied
        after enrichment; return value replaces the events array.
        *(Step 5 — stub in this revision.)*
    maxfiles : int, optional
        Maximum number of ROOT files to load.  ``-1`` loads all.
    CONFIG : Config, optional
        A :class:`~dtpr.base.config.Config` instance.  Defaults to the global
        ``RUN_CONFIG``.
    """

    def __init__(
        self,
        inputFolder: str,
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
        # NOTE: application is columnar (Step 5). For now they are stored only.
        self._load_from_config("ntuple_selectors")
        self._load_from_config("ntuple_preprocessors")

        # Build events array
        self.events = self._load_events(inputFolder)

    # ------------------------------------------------------------------
    # Internal loading pipeline
    # ------------------------------------------------------------------

    def _load_events(self, input_path: str) -> EventList:
        """Run the two-pass loading pipeline and return an :class:`EventList`."""
        files = _collect_files(input_path, self._maxfiles)
        if not files:
            raise FileNotFoundError(f"No ROOT files found at: {input_path}")

        color_msg(f"Loading {len(files)} file(s) from: {input_path}", "blue", 1)
        for f in files:
            color_msg(os.path.basename(f), indentLevel=2)

        # Update maxfiles to reflect actual count
        self._maxfiles = len(files)

        # Build branch allow-list and schema
        allowed = _build_allow_list(self.CONFIG, files, self._tree_name)
        schema_cls = PatternSchema.with_config(self.CONFIG)

        # Pass 1 — structural via NanoEventsFactory
        clean_treepath = self._tree_name.lstrip("/")
        file_map = {f: clean_treepath for f in files}

        raw_events = NanoEventsFactory.from_root(
            file_map,
            schemaclass=schema_cls,
            iteritems_options={"filter_name": allowed},
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
    _input = _os.path.abspath(
        _os.path.join(
            _os.path.dirname(__file__),
            "../../tests/ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root",
        )
    )
    ntuple = NTuple(_input)
    print(f"Loaded {len(ntuple.events)} events")
    ev0 = ntuple.events[0]
    print(ev0)
    print("digis[0]:", ev0["digis"][0])
