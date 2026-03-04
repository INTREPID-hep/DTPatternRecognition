"""
NTuple — file loader using coffea + Awkward Arrays.

Loading pre-steps
-----------------
**ROOT** (``.root`` files):

1. Parse ``Schema:`` from config → build allow-list + PatternSchema subclass.
2. Optionally ``partition`` the fileset (via ``coffea.dataset_tools``) to
   compute chunk boundaries when ``step_size`` is set.
3. Call ``NanoEventsFactory.from_root`` in ``mode='dask'`` (fully lazy).
4. Inject constant fields declared in the Schema (numeric values).
5. Execute the ``pre-steps:`` steps via :func:`~dtpr.base.pipeline.execute_pipeline`.

**Parquet** (``.parquet`` files):

1. Read lazily with ``dask_awkward.from_parquet`` → returns ``dask_awkward.Array``.
   Set ``split_row_groups=True`` for one partition per row group.
2. Execute the ``pre-steps:`` steps (same as ROOT).

Input resolution
----------------
1. If *inputs* is given (e.g. CLI ``-i``), it is used directly.
2. Otherwise, the ``filesets:`` section of the config YAML is consulted.

Public interface::

    # From CLI path
    ntuple = NTuple("/path/to/dir/")

    # From config filesets (no path needed)
    ntuple = NTuple(dataset="DYJets")

    # With chunking
    ntuple = NTuple("/path/to/dir/", step_size=100_000)

    ntuple.events                            # always dask_awkward.Array (lazy)
    ntuple.events["digis"]["BX"].compute()   # materialise on demand

Input formats:
  - ``"file.root"``                              single ROOT file
  - ``"/dir/*.root"``                            glob pattern
  - ``"/dir/"``                                  directory (``*.root`` or ``*.parquet``)
  - ``["a.root", "b.root"]``                     list of files
  - ``{"/dir/*.root": "tree", ...}``             dict (uproot-native, treepath explicit)
  - ``"output.parquet"``                         single Parquet file
  - ``["a.parquet", "b.parquet"]``               list of Parquet files
"""

from __future__ import annotations

import glob as _glob
import os
import warnings

from coffea.nanoevents import NanoEventsFactory
from natsort import natsorted

from .config import RUN_CONFIG
from .schema import PatternSchema, _branches_from_schema, _inject_constants
from .pipeline import execute_pipeline
from ..utils.functions import color_msg

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _format_input(
    files,
    tree_path: str | None,
    max_files: int = -1,
):
    """
    Normalize files into an uproot-native {file: tree_path} dict.

    Handles:
      - dict: {filepath: tree_spec} (tree_spec can be a string or {"object_path": ..., steps: ...} dict)
      - list: [filepath, ...], ["file:tree", ...], or [{file: tree}, ...]
      - str: single path, glob pattern, directory, or "file.root:tree"

    tree_path always takes priority over any tree name embedded in the path ("file.root:tree")
    or declared per-file in a dict spec ({"object_path": "tree"}). When tree_path is None,
    the embedded/per-file value is used as the fallback.

    The function is recursive: list elements and dict values are each resolved by delegating
    back here, so every branch ultimately reaches the str leaf which handles glob expansion.
    """
    # ── dict: {filepath: tree_spec} ─────────────────────────────────────────
    if isinstance(files, dict):
        result = {}
        for file_path, tree_spec in files.items():
            # Extract embedded tree from plain string or {"object_path": ...} dict
            embedded = (
                tree_spec.get("object_path") if isinstance(tree_spec, dict)
                else tree_spec
            )
            effective_tree = tree_path if tree_path is not None else embedded
            result[file_path] = effective_tree
        return result

    # ── list: [filepath, ...] or ["file:tree", ...] or [{file: tree}, ...] ──
    if isinstance(files, list):
        files = files[:max_files] if max_files > 0 else files
        result = {}
        for item in files:
            result.update(_format_input(item, tree_path, max_files))
        return result

    # ── str: single path, glob pattern, or "file.root:tree" ─────────────────
    if isinstance(files, str):
        glob_part = files
        effective_tree = tree_path
        if os.path.isdir(files):
            glob_part = os.path.join(files, "*.root")
        elif ":" in files:
            glob_part, embedded = files.rsplit(":", 1)
            effective_tree = tree_path if tree_path is not None else embedded

        if not effective_tree:
            raise ValueError(
                f"No tree name provided. Supply it via --tree TREEPATH, "
                "fileset 'treename:' key, or embedding it in the file(s) path(s) 'file.root:treepath'."
            )

        file_list = natsorted(_glob.glob(glob_part))
        if not file_list:
            raise FileNotFoundError(f"No .root files found in directory: {files}")
        if max_files > 0:
            file_list = file_list[:max_files]

        return {f: effective_tree for f in file_list}


def _is_parquet_input(input_path) -> bool:
    """Return True if *input_path* points at parquet data.

    Handles: single ``.parquet`` file, list of ``.parquet`` files, or a
    directory that contains at least one ``.parquet`` file.
    """
    if isinstance(input_path, str):
        if input_path.endswith(".parquet"):
            return True
        if os.path.isdir(input_path):
            try:
                return any(
                    f.endswith(".parquet")
                    for f in os.listdir(input_path)
                )
            except OSError:
                return False
        return False
    if isinstance(input_path, list):
        return bool(input_path) and all(
            str(p).endswith(".parquet") for p in input_path
        )
    return False


def _resolve_schema(config):
    """Resolve schema configuration for ROOT loading.

    Returns ``(schema_section, schema_cls, uproot_opts)``.
    """
    schema_section = getattr(config, "Schema", None)
    uproot_opts: dict = {}

    if schema_section is None:
        from coffea.nanoevents.schemas.base import BaseSchema
        schema_cls = BaseSchema
    elif isinstance(schema_section, str):
        import coffea.nanoevents.schemas as _schemas
        schema_cls = getattr(_schemas, schema_section, None)
        if schema_cls is None:
            raise ValueError(
                f"Unknown coffea schema class: '{schema_section}'. "
                "Check coffea.nanoevents.schemas for available names."
            )
    else:
        allowed = _branches_from_schema(schema_section)
        uproot_opts = {"filter_name": allowed}
        schema_cls = PatternSchema.with_config(schema_section)

    return schema_section, schema_cls, uproot_opts


def _partition_inputs(files, step_size: int):
    """Run ``coffea.dataset_tools.preprocess`` and return the files dict.

    Converts the internal ``{file: treepath}`` dict into a coffea fileset,
    preprocesses it (discovers entry counts and computes chunk boundaries),
    and returns the preprocessed files dict ready for
    ``NanoEventsFactory.from_root``.

    Parameters
    ----------
    files : str or dict
        A ``"file:tree"`` string or ``{file: treepath, ...}`` dict as
        produced by :func:`_format_input`.
    step_size : int
        Target number of entries per partition.

    Returns
    -------
    dict
        Coffea-style ``{filepath: {object_path, steps, ...}}`` dict.
    """
    from coffea.dataset_tools import preprocess

    # Normalise into a {file: {object_path: ...}} dict for coffea
    if isinstance(files, str):
        # "path/to/file.root:treepath" form
        file_part, obj_path = files.rsplit(":", 1)
        coffea_files = {file_part: {"object_path": obj_path}}
    elif isinstance(files, dict):
        coffea_files = {fpath: {"object_path": tree} for fpath, tree in files.items()}
    else:
        return files  # pass through unknown forms

    fileset = {"_dataset": {"files": coffea_files}}

    color_msg(
        f"Preprocessing {len(coffea_files)} file(s) with step_size={step_size}",
        "blue", 1,
    )
    available, _ = preprocess(fileset, step_size=step_size)

    return available["_dataset"]["files"]


def _load_from_root(
    files,
    treepath: str,
    maxfiles: int,
    config,
    step_size: int | None = None,
):
    """Build a lazy dask-awkward graph from ROOT input.

    Parameters
    ----------
    step_size : int or None
        If given, ``coffea.dataset_tools.preprocess`` is called first to
        discover file metadata and split each file into chunks of
        approximately *step_size* entries.  Each chunk becomes one dask
        partition.  When ``None`` (default) every file is one partition.
    """
    formated_files = _format_input(
        files,
        treepath,
        maxfiles,
    )

    schema_section, schema_cls, uproot_opts = _resolve_schema(config)

    # --- optional preprocessing for chunked reading ---
    if step_size is not None:
        formated_files = _partition_inputs(formated_files, step_size)

    events = NanoEventsFactory.from_root(
        formated_files,
        schemaclass=schema_cls,
        mode="dask",
        uproot_options=uproot_opts,
    ).events()

    if isinstance(schema_section, dict):
        events = _inject_constants(events, schema_section)

    return events


def _load_from_parquet(files, *, split_row_groups: bool | None = None):
    """Load parquet input as a lazy ``dask_awkward.Array``.

    Parameters
    ----------
    split_row_groups : bool or None
        If ``True``, each Parquet row group becomes its own dask
        partition.  If ``False``, each file is one partition.
        ``None`` (default) lets ``dask_awkward`` decide.
    """
    import dask_awkward as dak
    kwargs = {}
    if split_row_groups is not None:
        kwargs["split_row_groups"] = split_row_groups
    return dak.from_parquet(files, **kwargs)


def _resolve_tree_name(tree_name: str | None, extras: dict, config):
    """Resolve effective ROOT tree name.

    Priority:
    1. Explicit constructor/CLI ``tree_name`` (hard override)
    2. Fileset ``treename``
    3. Legacy config ``ntuple_tree_name``

    Returns
    -------
    str | None
        The resolved tree name, or ``None`` if not found.
    """
    return tree_name if tree_name is not None else (
        extras.get("treename")
        or getattr(config, "ntuple_tree_name", None)
    )

# ---------------------------------------------------------------------------
# Fileset resolution
# ---------------------------------------------------------------------------

def _resolve_fileset_input(inputs, dataset, config):
    """Resolve the effective input path and chunk parameters.

    Priority: explicit *inputs* (CLI) wins over config filesets.

    Parameters
    ----------
    inputs : str, list, dict, or None
        Explicit input passed by the caller (e.g. from CLI ``-i``).
    dataset : str or None
        Named dataset key inside ``config.filesets``.
    config : Config
        The run configuration object.

    Returns
    -------
    tuple[str | list | dict, dict]
        ``(input_path, extras)`` where *extras* is a dict that may
        contain ``step_size``, ``split_row_groups``, and ``metadata``
        extracted from the fileset definition.
    """
    extras: dict = {}

    # CLI path takes priority
    if inputs is not None:
        return inputs, extras

    # Fall back to config filesets
    filesets = getattr(config, "filesets", None)
    if not filesets:
        raise ValueError(
            "No input path provided and no 'filesets' section in config. "
            "Pass an input path or define filesets in your YAML."
        )

    if dataset is not None:
        if dataset not in filesets:
            raise KeyError(
                f"Dataset '{dataset}' not found in config filesets. "
                f"Available: {list(filesets.keys())}"
            )
        ds = filesets[dataset]
    else:
        # Single dataset — use the first (and only expected) one
        if len(filesets) > 1:
            warnings.warn(
                f"Multiple datasets in config filesets: {list(filesets.keys())}. "
                "Using the first one.  Pass dataset=<name> to be explicit."
            )
        dataset = next(iter(filesets))
        ds = filesets[dataset]

    color_msg(f"Using dataset '{dataset}' from config filesets", "blue", 1)

    files = ds.get("files")
    if not files:
        raise ValueError(f"Dataset '{dataset}' has no 'files' key.")

    # Extract optional chunk / metadata params
    for key in ("step_size", "split_row_groups", "metadata", "treename"):
        if key in ds:
            extras[key] = ds[key]

    return files, extras


# ---------------------------------------------------------------------------
# NTuple class
# ---------------------------------------------------------------------------

class NTuple:
    """
    NTuple loader.

    Loads ROOT files lazily via ``coffea.NanoEventsFactory`` and Parquet
    files lazily via ``dask_awkward.from_parquet``.  In both cases
    ``ntuple.events`` is a ``dask_awkward.Array``.

    Input resolution
    ~~~~~~~~~~~~~~~~
    1. If *inputs* is given (e.g. from CLI ``-i``), it is used directly.
    2. Otherwise, the ``filesets:`` section of the config YAML is consulted.
       Use *dataset* to select a named dataset; if omitted, the first (or
       only) dataset is used.

    Chunking
    ~~~~~~~~
        - *tree_name*: TTree path inside the ROOT files.  Also accepted as
      ``file.root:treepath`` embedded in the path, via ``--tree`` on the
      CLI, or as ``treename:`` in the fileset YAML block.
            Legacy ``TTREE`` and ``ntuple_tree_name`` config keys are still read
            as fallbacks.
    - *step_size*: (ROOT) entries per partition.  Triggers
      ``coffea.dataset_tools.preprocess`` to compute chunk boundaries.
    - *split_row_groups*: (Parquet) ``True`` → one partition per row group.

    These can be passed explicitly to the constructor **or** defined
    inside the ``filesets:`` YAML block.  Constructor args win.

    Parameters
    ----------
    inputs : str, list, dict, or None
        Explicit input path.  ``None`` → resolve from config filesets.
    maxfiles : int, optional
        Cap on number of files (``-1`` = all).
    dataset : str, optional
        Named dataset inside ``config.filesets``.
    tree_name : str, optional
        TTree path (e.g. ``'dtNtupleProducer/DTTREE'``).  Overrides
        fileset ``treename:`` and legacy ``ntuple_tree_name`` config key.
    step_size : int, optional
        ROOT: entries per partition (triggers ``preprocess``).
    split_row_groups : bool, optional
        Parquet: split per row group.
    CONFIG : Config, optional
        Defaults to global ``RUN_CONFIG``.
    """

    def __init__(
        self,
        inputs=None,
        maxfiles: int = -1,
        dataset: str | None = None,
        tree_name: str | None = None,
        step_size: int | None = None,
        split_row_groups: bool | None = None,
        CONFIG=None,
    ):
        self._maxfiles = maxfiles
        self.CONFIG = CONFIG if CONFIG is not None else RUN_CONFIG

        # ── resolve input: CLI path XOR config filesets ──
        files, extras = _resolve_fileset_input(
            inputs, dataset, self.CONFIG
        )

        # Tree name: explicit param > fileset treename > legacy config key
        self._tree_name = _resolve_tree_name(
            tree_name,
            extras,
            self.CONFIG,
        )
        if self._tree_name:
            self._tree_name = self._tree_name.lstrip("/")  # uproot doesn't like leading slashes

        # Constructor args override fileset-level settings
        self._step_size = step_size or extras.get("step_size")
        self._split_row_groups = (
            split_row_groups if split_row_groups is not None
            else extras.get("split_row_groups")
        )
        self.metadata = extras.get("metadata", {})

        self.events = self._load_events(files)

    # ------------------------------------------------------------------
    # Internal loading pre-steps
    # ------------------------------------------------------------------

    def _load_events(self, files):
        """Load events and apply the configured pre-steps.

        Both ROOT and Parquet inputs are loaded lazily and return
        ``dask_awkward.Array``.
        """
        color_msg(f"Loading events from: {files}", "blue", 1)

        if _is_parquet_input(files):
            events = _load_from_parquet(
                files,
                split_row_groups=self._split_row_groups,
            )
        else:
            events = _load_from_root(
                files, self._tree_name, self._maxfiles, self.CONFIG,
                step_size=self._step_size,
            )

        pre_steps = getattr(self.CONFIG, "pre-steps", None) or {}
        return execute_pipeline(events, pre_steps)


if __name__ == "__main__":
    import os as _os

    _ntuples = _os.path.abspath(
        _os.path.join(_os.path.dirname(__file__), "../../tests/ntuples")
    )
    _f1 = _os.path.join(_ntuples, "DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root")
    _f2 = _os.path.join(_ntuples, "DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_110.root")
    _tree = "dtNtupleProducer/DTTREE"

    def _show(label, ntuple, dump=False):
        print(f"\n{'─'*60}")
        print(f" {label}")
        print(f"   partitions : {ntuple.events.npartitions}")
        ev0 = ntuple.events[0].compute()
        print(f"   fields     : {ev0.fields[:4]}…")
        print(f"   digis[0]   : {ev0['digis'][0]}")

        if dump:
            from ..utils.io import dump_to_parquet
            # Write one file per partition into a directory so the partition
            # count is preserved on round-trip reload.
            dump_dir = f"/tmp/debug_{label.replace(' ', '_').replace('–', '-')}"
            dump_to_parquet(ntuple.events, dump_dir, per_partition=True)
            print(f"   dumped to  : {dump_dir}/")

    # ── Case 1: single file string (like CLI:  -i file.root  --tree tree) ───
    _show("Case 1 – single file str", NTuple(_f1, tree_name=_tree))

    # ── Case 2: list of files (tree via tree_name param) ───────────────────
    _show("Case 2 – list of files", NTuple([_f1, _f2], tree_name=_tree))

    # ── Case 3: list of files with tree embedded in each string ───────────
    _show("Case 3 – list of 'file:tree' strings",
          NTuple([f"{_f1}:{_tree}", f"{_f2}:{_tree}"]))

    # ── Case 4: uproot-native dict {file: treepath} ───────────────────────
    _show("Case 4 – dict {file: treepath}",
          NTuple({_f1: _tree, _f2: _tree}))

    # ── Case 5: dict with full coffea spec {file: {object_path, ...}} ─────
    _show("Case 5 – dict with object_path dicts",
          NTuple({_f1: {"object_path": _tree},
                  _f2: {"object_path": _tree}}))

    # ── Case 6: from YAML filesets (patch RUN_CONFIG at runtime) ──────────
    # This is what a user does when no -i is given and config has filesets:
    from .config import RUN_CONFIG
    RUN_CONFIG.filesets = {
        "MySample": {
            "files": {_f1: _tree, _f2: _tree},
            "metadata": {"year": 2024, "is_mc": True},
            "step_size": 500,           # → coffea.preprocess → 4 partitions
        }
    }
    _show("Case 6 – from config filesets (step_size=500)", NTuple())

    # ── Case 7: YAML fileset with single treename string (no per-file dict) ───────
    RUN_CONFIG.filesets = {
        "MySample": {
            "treename": _tree,  # treepath for all files (no per-file dict)
            "files": [_f1, _f2],  # no treepath → use config-level treepath
            "metadata": {"year": 2024, "is_mc": True},
        }
    }
    _show("Case 7 – fileset with treename and file list", NTuple())

    # ── Case 8: YAML fileset, but override step_size from constructor ──────
    _show("Case 8 – fileset + constructor step_size override",
          NTuple(step_size=500))        # fewer partitions than step_size=500

    # ── Case 9: YAML fileset without tree info (should raise) ──────
    RUN_CONFIG.filesets = {
        "MySample": {
            "files": [_f1, _f2],  # no treepath info at all → should raise
        }
    }
    try:
        NTuple()
    except ValueError as e:
        print(f"\nCase 9 – missing tree name → raised ValueError as expected: {e}")
    
    # ── Case 10: YAML fileset with tree info, but override tree_name with constructor (should win) ──────
    _show("Case 10 – fileset with tree + constructor tree_name override",
          NTuple(tree_name="dtNtupleProducer/DTTREE"), dump=True)  # same tree, but should still work and dump the file

    # ── Case 11: Parquet input — reload the per-partition directory dumped in Case 10.
    # Partition count should match Case 10.
    _show("Case 11 – loading from Parquet", NTuple(inputs="/tmp/debug_Case_10_-_fileset_with_tree_+_constructor_tree_name_override"))



