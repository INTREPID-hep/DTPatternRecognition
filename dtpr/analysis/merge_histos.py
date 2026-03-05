"""Merge per-partition histogram ROOT files — ``dtpr merge-histos``.

Intended as the manual merge step after a ``fill-histos --per-partition``
run (or to recover from a partially completed job, since existing partition
files are skipped on re-run).  Point this at the directory containing the
per-partition ``*.root`` files and it will sum them into a single ROOT file.

Typical workflow
----------------
::

    # Run fill-histos in per-partition mode:
    dtpr fill-histos -i /files/ -o results/ -c 8 --per-partition
    # → results/histograms/histograms_000.root … histograms_NNN.root

    # Job fails? Re-run — existing partition files are skipped automatically.
    # Merge when all partitions are done:
    dtpr merge-histos --checkpoint-dir results/histograms/ -o results/
"""

from __future__ import annotations

import glob
import os

from natsort import natsorted

from ..utils.functions import color_msg, create_outfolder


def merge_histos(
    checkpoint_dir: str,
    outfolder: str,
    tag: str = "",
) -> None:
    """Merge per-partition histogram ROOT files into a single ROOT file.

    Parameters
    ----------
    checkpoint_dir : str
        Directory containing per-partition ``*.root`` histogram files
        written by ``fill-histos --per-partition``.
    outfolder : str
        Output directory.  A ``histograms/`` sub-folder is created.
    tag : str
        String appended to the output filename,
        e.g. ``"_v2"`` → ``histograms_v2.root``.
    """
    import uproot

    color_msg("Running merge-histos...", "green")

    # ── Discover ROOT files ──────────────────────────────────────────────────
    pattern = os.path.join(checkpoint_dir, "*.root")
    root_files = natsorted(glob.glob(pattern))

    if not root_files:
        color_msg(
            f"No ROOT files found in {checkpoint_dir!r}.",
            color="red", indentLevel=1,
        )
        return

    color_msg(
        f"Found {len(root_files)} file(s) in {checkpoint_dir!r}.",
        color="blue", indentLevel=1,
    )

    # ── Load and sum histograms key-by-key ───────────────────────────────────
    merged: dict = {}
    for path in root_files:
        try:
            with uproot.open(path) as f:
                for key in f.keys(cycle=False):
                    h = f[key].to_hist()
                    merged[key] = (merged[key] + h) if key in merged else h
        except Exception as exc:
            color_msg(
                f"Skipping corrupt file {path!r}: {exc}",
                color="yellow", indentLevel=2,
            )

    if not merged:
        color_msg("No valid histograms to merge.", color="red", indentLevel=1)
        return

    color_msg(
        f"Merged {len(root_files)} file(s) × {len(merged)} histogram(s).",
        color="blue", indentLevel=1,
    )

    # ── Write merged ROOT file ───────────────────────────────────────────────
    out_path = os.path.join(outfolder, "histograms")
    create_outfolder(out_path)
    root_path = os.path.join(out_path, f"histograms{tag}.root")
    with uproot.recreate(root_path) as f:
        for key, h in merged.items():
            f[key] = h
    color_msg(f"Histograms saved → {root_path}", color="green", indentLevel=1)

    color_msg("Done!", color="green")
