"""Utilities for filesystem and module-import path resolution.

Two concerns live here:

1. **Fileset path resolution** — ``files:`` values in YAML may contain paths
   relative to the config file.  :func:`resolve_file_paths` resolves them
   against a *base_dir* so the YAML works regardless of the working directory.

2. **sys.path seeding** — ``src:`` dotted paths in YAML (e.g.
   ``src: "utils.my_fn"``) are regular Python imports.  For them to work
   outside the CLI the config file's directory must be on ``sys.path``.
   :func:`ensure_config_on_syspath` does this idempotently from anywhere.
"""
from __future__ import annotations

import os
import sys


def config_dir(config) -> str:
    """Return the directory that contains *config*'s YAML file.

    Falls back to ``os.getcwd()`` when the config has no ``path`` attribute
    (e.g. a programmatically constructed config object).
    """
    path = getattr(config, "path", None)
    return os.path.dirname(os.path.abspath(path)) if path else os.getcwd()


def resolve_file_paths(raw_files, base_dir: str):
    """Resolve relative paths in a fileset ``files:`` value against *base_dir*.

    Handles all three YAML forms:

    * ``dict``  – ``{path: tree_or_spec, …}``
    * ``list``  – ``[path, …]``
    * ``str``   – single path or glob

    Absolute paths are returned unchanged (``os.path.join`` already handles
    this correctly — no explicit ``isabs`` check needed).
    """
    if isinstance(raw_files, dict):
        return {
            os.path.normpath(os.path.join(base_dir, k)): v
            for k, v in raw_files.items()
        }
    if isinstance(raw_files, list):
        return [
            os.path.normpath(os.path.join(base_dir, f)) if isinstance(f, str) else f
            for f in raw_files
        ]
    if isinstance(raw_files, str):
        return os.path.normpath(os.path.join(base_dir, raw_files))
    return raw_files  # unknown form – pass through


def ensure_config_on_syspath(config) -> None:
    """Add the config file's directory to ``sys.path`` (idempotently).

    This makes dotted ``src:`` module paths declared in the YAML importable
    both when running through the CLI *and* when constructing
    :class:`~dtpr.base.NTuple` (or calling analysis functions) directly in
    Python, without requiring the user to manipulate ``sys.path`` manually.
    """
    d = config_dir(config)
    if d not in sys.path:
        sys.path.insert(0, d)
