"""
NTuple — file loader using coffea + Awkward Arrays.

Loading pipeline
----------------
**ROOT** (``.root`` files):

1. Parse ``Schema:`` from config → build allow-list + PatternSchema subclass.
2. Call ``NanoEventsFactory.from_root`` in ``mode='dask'`` (fully lazy).
3. Inject constant fields declared in the Schema (numeric values).
4. Execute the ``pipeline:`` steps via :func:`~dtpr.base.pipeline.execute_pipeline`.

**Parquet** (``.parquet`` files):

1. Read lazily with ``dak.from_parquet`` → returns ``dask_awkward.Array``.
2. Execute the ``pipeline:`` steps (same as ROOT).

Public interface::

    ntuple = NTuple("/path/to/dir/")         # ROOT or Parquet
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
        return input_path

    if isinstance(input_path, list):
        files = input_path
        if maxfiles > 0:
            files = files[:maxfiles]
        return {f: treepath for f in files}

    if isinstance(input_path, str):
        if os.path.isdir(input_path):
            files = natsorted(_glob.glob(os.path.join(input_path, "*.root")))
            if not files:
                raise FileNotFoundError(f"No .root files found in directory: {input_path}")
            if maxfiles > 0:
                files = files[:maxfiles]
            return {f: treepath for f in files}

        if "*" in input_path or "?" in input_path:
            if ":" in input_path:
                glob_part, _embedded_tree = input_path.rsplit(":", 1)
            else:
                glob_part = input_path
            files = natsorted(_glob.glob(glob_part))
            if not files:
                raise FileNotFoundError(f"No .root files matched glob: {glob_part}")
            if maxfiles > 0:
                files = files[:maxfiles]
            return {f: treepath for f in files}

        if ":" not in input_path:
            return f"{input_path}:{treepath}"
        return input_path

    return input_path


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


def _load_from_root(input_path, treepath: str, maxfiles: int, config):
    """Build a lazy dask-awkward graph from ROOT input."""
    clean_treepath = treepath.lstrip("/")
    processed = _preprocess_input(input_path, clean_treepath, maxfiles)

    schema_section, schema_cls, uproot_opts = _resolve_schema(config)

    events = NanoEventsFactory.from_root(
        processed,
        schemaclass=schema_cls,
        mode="dask",
        uproot_options=uproot_opts,
    ).events()

    if isinstance(schema_section, dict):
        events = _inject_constants(events, schema_section)

    return events


def _load_from_parquet(input_path):
    """Load parquet input as a lazy ``dask_awkward.Array``."""
    import dask_awkward as dak
    return dak.from_parquet(input_path)


# ---------------------------------------------------------------------------
# NTuple class
# ---------------------------------------------------------------------------

class NTuple:
    """
    NTuple loader.

    Loads ROOT files lazily via ``coffea.NanoEventsFactory`` and Parquet
    files lazily via ``dask_awkward.from_parquet``.  In both cases
    ``ntuple.events`` is a ``dask_awkward.Array``.

    Parameters
    ----------
    inputFolder : str, list, or dict
        Accepted formats: single file/dir path, glob, list of paths,
        uproot-native ``{path: treepath}`` dict, or ``.parquet`` paths.
    maxfiles : int, optional
        Maximum number of files when *inputFolder* is a directory or list.
        ``-1`` (default) loads all files.  Ignored for single-file inputs.
    CONFIG : Config, optional
        A :class:`~dtpr.base.config.Config` instance.  Defaults to the global
        ``RUN_CONFIG``.
    """

    def __init__(
        self,
        inputFolder,
        maxfiles: int = -1,
        CONFIG=None,
    ):
        self._maxfiles = maxfiles
        self.CONFIG = CONFIG if CONFIG is not None else RUN_CONFIG

        _tree_name = getattr(self.CONFIG, "ntuple_tree_name", None)
        if _tree_name is None:
            warnings.warn("No ntuple_tree_name in CONFIG. Defaulting to '/TTREE'.")
            self._tree_name = "/TTREE"
        else:
            self._tree_name = _tree_name

        self.events = self._load_events(inputFolder)

    # ------------------------------------------------------------------
    # Internal loading pipeline
    # ------------------------------------------------------------------

    def _load_events(self, input_path):
        """Load events and apply the configured pipeline.

        Both ROOT and Parquet inputs are loaded lazily and return
        ``dask_awkward.Array``.
        """
        color_msg(f"Loading events from: {input_path}", "blue", 1)

        if _is_parquet_input(input_path):
            events = _load_from_parquet(input_path)
        else:
            events = _load_from_root(
                input_path, self._tree_name, self._maxfiles, self.CONFIG
            )

        pipeline_steps = getattr(self.CONFIG, "pipeline", None) or {}
        return execute_pipeline(events, pipeline_steps)


if __name__ == "__main__":
    import os as _os
    _input = _os.path.abspath(
        _os.path.join(
            _os.path.dirname(__file__),
            "../../tests/ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root",
        )
    )
    ntuple = NTuple(_input, maxfiles=1)
    ev0 = ntuple.events[0].compute()
    print(ev0)
    print("fields:", ev0.fields)
    print("digis[0]:", ev0["digis"][0])


