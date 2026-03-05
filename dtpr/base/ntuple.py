"""
NTuple — file loader using coffea + Awkward Arrays.

Loading
-------
**ROOT** (``.root`` files):

1. Parse ``Schema:`` from config (or per-dataset ``schema:`` key) →
   build allow-list + PatternSchema subclass.
2. Optionally partition the fileset (via ``coffea.dataset_tools``) when
   ``step_size`` is set.
3. Call ``NanoEventsFactory.from_root`` in ``mode='dask'`` (fully lazy).
4. Inject constant fields declared in the Schema.
5. Execute the ``pre-steps:`` pipeline.

**Parquet** (``.parquet`` files):

1. Read lazily with ``dask_awkward.from_parquet``.
2. Execute the ``pre-steps:`` pipeline.

Input resolution
----------------
1. *inputs* given → used directly → ``events`` is ``dak.Array``.
2. *datasets* given → each named entry in ``config.filesets`` loaded
   independently → ``events`` is ``dict[str, dak.Array]``.
3. Neither given → load **all** filesets from config, same as
   ``datasets=[]`` → ``events`` is ``dict[str, dak.Array]``.

Multiple datasets
-----------------
``events`` is a ``dict[str, dak.Array]`` so each dataset stays separate
and identifiable:

    ntuple = NTuple(datasets=["DY", "Zprime"])
    dy_events     = ntuple.events["DY"]
    zprime_events = ntuple.events["Zprime"]

    ntuple = NTuple(datasets=[])   # load ALL filesets defined in config

Per-dataset options (keys inside a ``filesets:`` block entry):
  - ``treename``        : tree path for this dataset
  - ``schema``          : schema override (dict, str, or None)
  - ``step_size``       : entries per partition
  - ``split_row_groups``: Parquet row-group splitting
  - ``metadata``        : arbitrary dict attached to ``ntuple.metadata[name]``

``tree_name`` in the constructor is a **global override** and may also be
a ``list[str]`` whose entries correspond one-to-one with *datasets*.

Public interface::

    ntuple = NTuple("/path/to/dir/", tree_name="dtNtuple/DTTREE")
    ntuple = NTuple(datasets=["DY", "Zprime"])
    ntuple = NTuple(datasets=[])

    ntuple.events                            # dak.Array or dict[str, dak.Array]
    ntuple.events["digis"]["BX"].compute()   # single dataset
    ntuple.events["DY"]["digis"]["BX"].compute()  # multi-dataset

Input formats (for *inputs* / fileset ``files:`` values):
  - ``"file.root"``                              single ROOT file
  - ``"/dir/*.root"``                            glob pattern
  - ``"/dir/"``                                  directory (``*.root`` or ``*.parquet``)
  - ``["a.root", "b.root"]``                     list of files
  - ``{"file.root": "tree", ...}``              dict (uproot-native)
  - ``"output.parquet"``                         single Parquet file
  - ``["a.parquet", "b.parquet"]``               list of Parquet files
"""

from __future__ import annotations

import glob as _glob
import os

from coffea.nanoevents import NanoEventsFactory
from natsort import natsorted

from .config import RUN_CONFIG
from .schema import PatternSchema, _branches_from_schema, _inject_constants
from .pipeline import execute_pipeline
from ..utils.functions import color_msg
from ..utils.paths import config_dir as _config_dir, resolve_file_paths, ensure_config_on_syspath

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


def _resolve_schema(schema_section):
    """Resolve a raw schema value into ``(schema_section, schema_cls, uproot_opts)``.

    Parameters
    ----------
    schema_section : None, str, or dict
        - ``None``  → coffea ``BaseSchema`` (reads all branches).
        - ``str``   → named coffea schema class (e.g. ``"NanoAODSchema"``).
        - ``dict``  → PatternSchema config (branch allow-list + constants).
    """
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
    available, _ = preprocess(fileset, step_size=step_size)

    return available["_dataset"]["files"]


def _load_from_root(
    formatted_files: dict,
    config,
    step_size: int | None = None,
    schema_section=None,
):
    """Build a lazy dask-awkward graph from an already-formatted ROOT fileset.

    Parameters
    ----------
    formatted_files : dict
        ``{abs_path: treepath}`` dict as produced by :func:`_format_input`.
    schema_section : None, str, or dict, optional
        Per-call schema override. ``None`` (default) falls back to
        ``config.Schema``.
    step_size : int or None
        If given, ``coffea.dataset_tools.preprocess`` splits each file
        into chunks of approximately *step_size* entries.
    """
    if schema_section is not None:
        # str → look up as named schema in config first, else treat as coffea class name
        # dict → use directly as PatternSchema config
        effective_schema = (
            getattr(config, schema_section, None) or schema_section
            if isinstance(schema_section, str)
            else schema_section
        )
    else:
        effective_schema = getattr(config, "Schema", None)  # global fallback; None → BaseSchema

    schema_sec, schema_cls, uproot_opts = _resolve_schema(effective_schema)

    load_files = formatted_files
    if step_size is not None:
        load_files = _partition_inputs(formatted_files, step_size)

    events = NanoEventsFactory.from_root(
        load_files,
        schemaclass=schema_cls,
        mode="dask",
        uproot_options=uproot_opts,
    ).events()

    if isinstance(schema_sec, dict):
        events = _inject_constants(events, schema_sec)

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


# ---------------------------------------------------------------------------
# Fileset inspector
# ---------------------------------------------------------------------------

def _print_files_block(loaded_files: dict, ds_treename: str = "", max_files: int = 3) -> None:
    """Render the indented file listing for one dataset block.

    *loaded_files* is always a ``{abs_path: tree_or_none}`` dict as stored in
    ``NTuple._loaded_files``.  Per-file tree annotations are shown only when
    they differ from *ds_treename*.
    """
    items  = list(loaded_files.items())
    show   = items if max_files < 0 else items[:max_files]
    hidden = len(items) - len(show)

    for i, (path, file_tree) in enumerate(show):
        is_last   = (i == len(show) - 1) and hidden == 0
        connector = "└" if is_last else "├"
        fname     = os.path.basename(path)
        tree_part = (
            color_msg(f"  →  {file_tree}", "purple", return_str=True)
            if file_tree and file_tree != ds_treename
            else ""
        )
        print(
            color_msg(f"      {connector}  ", "cyan", return_str=True)
            + color_msg(fname, "white", return_str=True)
            + tree_part
        )

    if hidden > 0:
        color_msg(f"      └  … +{hidden} more", "cyan")


def _print_dataset_block(ds_name: str, loaded_files: dict, ds_cfg: dict, max_files: int = 3, indent: int = 0) -> None:
    """Print the header, info line, and file listing for one dataset entry.

    *loaded_files* is the ``{abs_path: tree}`` dict from ``NTuple._loaded_files``.
    *ds_cfg* is the raw YAML entry, used only for metadata and display options.
    """
    meta        = ds_cfg.get("metadata") or {}
    ds_treename = ds_cfg.get("treename") or ds_cfg.get("tree_name") or ""
    if not ds_treename and loaded_files:
        trees = {t for t in loaded_files.values() if t}
        if len(trees) == 1:
            ds_treename = next(iter(trees))
    n       = len(loaded_files)
    label   = meta.get("label", "")
    version = meta.get("version", "")
    tag     = (f"  {label}" if label else "") + (f"  [v{version}]" if version else "")

    print(
        color_msg("  ▸ ", "cyan", return_str=True, indentLevel=indent)
        + color_msg(ds_name, "yellow", bold=True, return_str=True)
        + color_msg(f"  ({n} file{'s' if n != 1 else ''})", "white", return_str=True)
        + color_msg(tag, "white", return_str=True)
    )
    info_parts = [
        *([ f"tree: {ds_treename}"] if ds_treename else []),
        *([ f"step_size={ds_cfg['step_size']}"] if ds_cfg.get("step_size") else []),
        *([ "split_row_groups=True"] if ds_cfg.get("split_row_groups") else []),
    ]
    if info_parts:
        color_msg("      " + "  ·  ".join(info_parts), "purple", indentLevel=indent)
    _print_files_block(loaded_files, ds_treename, max_files)


# ---------------------------------------------------------------------------
# NTuple class
# ---------------------------------------------------------------------------

class NTuple:
    """
    NTuple loader.

    Loads ROOT files lazily via ``coffea.NanoEventsFactory`` and Parquet
    files lazily via ``dask_awkward.from_parquet``.

    Input resolution
    ~~~~~~~~~~~~~~~~
    - *inputs* given → explicit files → ``events`` is ``dak.Array``.
    - *datasets* given → named filesets from config → ``events`` is
      ``dict[str, dak.Array]`` (each dataset stays separate).
    - Neither given → load **all** filesets from config, same as
      ``datasets=[]`` → ``events`` is ``dict[str, dak.Array]``.

    Multiple datasets
    -----------------
    ``events`` is a ``dict[str, dak.Array]`` that keeps every dataset
    individually accessible:

        ntuple = NTuple(datasets=["DY", "Zprime"])
        ntuple.events["DY"]["digis"]["BX"].compute()

        ntuple = NTuple(datasets=[])   # load ALL filesets from config

    Per-dataset YAML keys (inside a ``filesets:`` block entry):
      - ``treename``        : tree path for this dataset
      - ``schema``          : schema override (dict, str, or None)
      - ``step_size``       : entries per partition
      - ``split_row_groups``: Parquet row-group splitting
      - ``metadata``        : arbitrary dict → ``ntuple.metadata[name]``

    Parameters
    ----------
    inputs : str, list, dict, or None
        Explicit input path. Mutually exclusive with *datasets*.
    maxfiles : int, optional
        Cap on files per dataset (``-1`` = all).
    datasets : str, list[str], or None, optional
        Named datasets to load.  A bare string is treated as a single-element
        list.  ``[]`` → all filesets in config.
        When given, ``events`` / ``metadata`` are dicts keyed by name.
    tree_name : str | list[str] | None, optional
        TTree path.  ``str`` → same for all datasets.  ``list[str]`` →
        one entry per dataset (must match *datasets* length).  Overrides
        fileset ``treename:`` keys.
    step_size : int, optional
        ROOT: entries per partition (triggers ``preprocess``).
    split_row_groups : bool, optional
        Parquet: one partition per row group.
    CONFIG : Config, optional
        Defaults to global ``RUN_CONFIG``.

    Attributes
    ----------
    events : dak.Array or dict[str, dak.Array]
        Single lazy array or per-dataset mapping.
    metadata : dict or dict[str, dict]
        Flat dict (single dataset) or ``{name: {…}, …}`` (multi).
    """

    def __init__(
        self,
        inputs: str | list | dict | None = None,
        maxfiles: int = -1,
        datasets: str | list[str] | None = None,
        tree_name: str | list[str] | None = None,
        step_size: int | None = None,
        split_row_groups: bool | None = None,
        CONFIG=None,
    ):
        self.CONFIG    = CONFIG if CONFIG is not None else RUN_CONFIG
        self._maxfiles = maxfiles

        if inputs is not None and datasets is not None:
            raise ValueError("'inputs' and 'datasets' are mutually exclusive.")

        # ── explicit inputs: bypass all dataset resolution ────────────────
        if inputs is not None:
            if isinstance(tree_name, list):
                raise ValueError(
                    "tree_name as a list is only supported with 'datasets'. "
                    "Pass a single string when using 'inputs'."
                )
            self.metadata      = {}
            self._loaded_files = {}
            ensure_config_on_syspath(self.CONFIG)
            self.events = self._load_events(inputs, tree_name, step_size, split_row_groups, name="inputs")
            self._loaded_files = self._loaded_files["inputs"]
            self.info()
            return

        # ── resolve filesets once ─────────────────────────────────────────
        filesets = getattr(self.CONFIG, "filesets", None) or {}
        if not filesets:
            raise ValueError(
                "No input path provided and no 'filesets' section in config."
            )

        # ── normalise datasets → always a non-empty list ──────────────────
        if isinstance(datasets, str):
            datasets = [datasets]

        if not datasets:  # None or []  →  load all
            datasets = list(filesets.keys())

        # ── validate dataset names ────────────────────────────────────────
        for name in datasets:
            if name not in filesets:
                raise KeyError(
                    f"Dataset '{name}' not found in config filesets. "
                    f"Available: {list(filesets.keys())}"
                )

        # ── per-dataset tree_name (list → one per dataset, else broadcast) ─
        if isinstance(tree_name, list) and len(tree_name) != len(datasets):
            raise ValueError(
                f"tree_name list length ({len(tree_name)}) must match "
                f"datasets length ({len(datasets)})."
            )
        tree_names = tree_name if isinstance(tree_name, list) else [tree_name] * len(datasets)

        # ── load ──────────────────────────────────────────────────────────
        ensure_config_on_syspath(self.CONFIG)
        base_dir = _config_dir(self.CONFIG)

        events_map:        dict = {}
        meta_map:          dict = {}
        self._loaded_files: dict = {}
        for ds_name, ds_tree in zip(datasets, tree_names):
            ds_cfg    = filesets[ds_name]
            raw_files = ds_cfg.get("files")
            if not raw_files:
                raise ValueError(f"Dataset '{ds_name}' has no 'files' key.")
            files = resolve_file_paths(raw_files, base_dir)
            # per-dataset fallbacks: constructor args win, then ds_cfg keys
            eff_tree     = ds_tree          if ds_tree is not None          else ds_cfg.get("treename")
            eff_step     = step_size        if step_size is not None        else ds_cfg.get("step_size")
            eff_split_rg = split_row_groups if split_row_groups is not None else ds_cfg.get("split_row_groups")
            eff_schema   = ds_cfg.get("schema")
            events_map[ds_name] = self._load_events(
                files, eff_tree, eff_step, eff_split_rg, schema=eff_schema, name=ds_name,
            )
            meta_map[ds_name] = ds_cfg.get("metadata", {})

        self.events   = events_map
        self.metadata = meta_map
        self.info()

    # ------------------------------------------------------------------
    # Internal loading
    # ------------------------------------------------------------------

    def _load_events(
        self,
        files,
        tree_name: str | None,
        step_size: int | None,
        split_row_groups: bool | None,
        schema=None,
        name: str = "inputs",
        verbose: bool = True,
    ):
        """Format files, load (ROOT or Parquet), apply pre-steps → ``dak.Array``.

        Side-effect: populates ``self._loaded_files[name]`` with the resolved
        ``{abs_path: tree_or_none}`` dict so callers never need a tuple return.
        """
        if tree_name:
            tree_name = tree_name.lstrip("/")

        if _is_parquet_input(files):
            # Normalize for display; pass original paths to loader
            if isinstance(files, str):
                self._loaded_files[name] = {files: None}
            elif isinstance(files, list):
                self._loaded_files[name] = {f: None for f in files}
            else:
                self._loaded_files[name] = {}
            events = _load_from_parquet(files, split_row_groups=split_row_groups)
        else:
            formatted = _format_input(files, tree_name, self._maxfiles)
            self._loaded_files[name] = formatted
            events = _load_from_root(
                formatted, self.CONFIG,
                step_size=step_size, schema_section=schema,
            )

        pre_steps = getattr(self.CONFIG, "pre-steps", None) or {}
        return execute_pipeline(events, pre_steps, dataset=name if name != "inputs" else None)

    def info(self, max_files: int = 3, indent: int = -1) -> None:
        """Print a human-readable summary of the loaded NTuple.

        **Inputs mode** (explicit ``inputs=`` path): shows the files passed
        in and the resulting partition count.

        **Filesets mode** (``datasets=`` or config-driven): shows only the
        datasets that were loaded, then appends per-dataset partition counts.

        Parameters
        ----------
        max_files : int, optional
            File paths to show per dataset / inputs block.  Default 3.
        """
        if isinstance(self.events, dict):
            # ── filesets mode — only show the datasets that were loaded ───
            filesets = getattr(self.CONFIG, "filesets", None) or {}
            color_msg(f"Loaded — {len(self.events)} dataset(s)", "cyan", bold=True, indentLevel=indent)
            for ds_name, evts in self.events.items():
                loaded = self._loaded_files.get(ds_name, {})
                ds_cfg = filesets.get(ds_name, {})
                _print_dataset_block(ds_name, loaded, ds_cfg, max_files, indent)
                try:
                    npart = evts.npartitions
                except Exception:
                    npart = "?"
                color_msg(f"{ds_name}  →  {npart} partition(s)", "green", indentLevel=indent+3)
        else:
            # ── inputs mode ───────────────────────────────────────────────
            loaded = self._loaded_files  # always {abs_path: tree}
            n      = len(loaded)
            trees  = {t for t in loaded.values() if t}
            tree   = next(iter(trees)) if len(trees) == 1 else ""
            try:
                npart = self.events.npartitions
            except Exception:
                npart = "?"

            print(
                color_msg("▸ ", "cyan", return_str=True, indentLevel=indent)
                + color_msg("inputs", "yellow", bold=True, return_str=True)
                + color_msg(f"  ({n} file{'s' if n != 1 else ''})", "white", return_str=True)
                + (color_msg(f"  →  {tree}", "purple", return_str=True) if tree else "")
            )
            _print_files_block(loaded, tree, max_files)
            color_msg(f"      {npart} partition(s)", "green")
