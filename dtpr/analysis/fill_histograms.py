import os
import importlib
import warnings
import re
import ROOT as r
from tqdm import tqdm
from ..base import NTuple
from ..base.config import RUN_CONFIG
from ..utils.functions import (
    color_msg,
    error_handler,
    create_outfolder,
)
from more_itertools import collapse
from typing import Any, Dict, Optional, Tuple


def extract_histogram_name(draw_expr: str) -> Optional[str]:
    """
    Extract histogram name from ROOT Draw expression.

    Examples:
        "var >> h_name(100, 0, 1)" -> "h_name"
        "var >> h_name" -> "h_name"

    :param draw_expr: The Draw expression string
    :type draw_expr: str
    :return: Histogram name or None if extraction fails
    :rtype: Optional[str]
    """
    # Match >> followed by optional whitespace, then capture word characters
    match = re.search(r">>\s*(\w+)", draw_expr)
    if match:
        return match.group(1)
    return None


def classify_histograms(histograms: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Classify histograms into event-based and draw-based groups.

    :param histograms: Dictionary of histogram definitions
    :type histograms: Dict[str, Any]
    :return: Tuple of (event_histograms, draw_histograms)
    :rtype: Tuple[Dict[str, Any], Dict[str, Any]]
    """
    event_histograms = {}
    draw_histograms = {}

    for hname, histoinfo in histograms.items():
        htype = histoinfo.get("type", "")

        if htype == "root-draw":
            # Validate draw histogram has required "draw" key
            if "draw" not in histoinfo:
                warnings.warn(f"Draw histogram '{hname}' is missing required 'draw' key. Skipping.")
                continue
            draw_histograms[hname] = histoinfo
        else:
            # Event-based histogram
            event_histograms[hname] = histoinfo

    return event_histograms, draw_histograms


def execute_draw_histograms(tree: Any, draw_histograms: Dict[str, Any], maxentries: int) -> None:
    """
    Execute all Draw-based histograms and retrieve from ROOT gDirectory.

    :param tree: ROOT TChain or TTree to draw from
    :type tree: Any
    :param draw_histograms: Dictionary of draw histogram definitions
    :type draw_histograms: Dict[str, Any]
    :param maxentries: Number of entries to process (>0) or all entries (<=0)
    :type maxentries: int
    :return: None
    :rtype: None
    """
    for hname, histoinfo in draw_histograms.items():
        draw_expr = histoinfo["draw"]
        selection = histoinfo.get("selection", "")
        option = histoinfo.get("option", "goff")

        # Extract histogram name from draw expression
        hname_from_expr = extract_histogram_name(draw_expr)
        if not hname_from_expr:
            warnings.warn(
                f"Failed to extract histogram name from Draw expression for '{hname}': {draw_expr}. Skipping."
            )
            continue

        # Execute Draw respecting CLI maxevents.
        # ROOT uses kMaxEntries to process all entries.
        nentries = maxentries if maxentries > 0 else r.TTree.kMaxEntries
        firstentry = 0

        try:
            tree.Draw(draw_expr, selection, option, nentries, firstentry)
        except Exception as e:
            warnings.warn(f"Error executing Draw for '{hname}': {str(e)}. Skipping.")
            continue

        # Retrieve histogram from ROOT gDirectory
        h = r.gDirectory.Get(hname_from_expr)
        if h is None:
            warnings.warn(
                f"Draw did not produce histogram '{hname_from_expr}' for '{hname}'. Skipping."
            )
            continue

        # Ensure histogram survives until file write (set directory to null)
        h.SetDirectory(0)

        # Store retrieved histogram in histoinfo for later saving
        histoinfo["histo"] = h


def set_histograms_dict(CONFIG=None) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Sets up and classifies histograms to fill based on configuration.

    :return: Tuple of (event_histograms, draw_histograms)
    :rtype: Tuple[Dict[str, Any], Dict[str, Any]]
    """
    config = CONFIG if CONFIG is not None else RUN_CONFIG
    histos_to_fill = {}
    # Import histograms from each source in configuration
    for source in getattr(config, "histo_sources", []):
        module = importlib.import_module(source)
        module_histos = getattr(module, "histos", {})
        # Only include histograms specified in the configuration
        histos_to_fill.update(
            {k: v for k, v in module_histos.items() if k in getattr(config, "histo_names", [])}
        )

    # Warn about any missing histograms
    missing_histos = set(getattr(config, "histo_names", [])) - set(histos_to_fill.keys())
    if missing_histos:
        warnings.warn(
            f"The following histograms could not be found in any of the sources: {', '.join(missing_histos)}"
        )

    return classify_histograms(histos_to_fill)


def _execute_histo_function(func: Any, event: Any, histo_key: str) -> Optional[Any]:
    """
    Execute histogram function with error handling.

    :param func: The function to execute on the event
    :type func: Any
    :param event: The event data to process
    :type event: Any
    :param histo_key: Histogram key for error reporting
    :type histo_key: str
    :return: The result of the function or None if an error occurred
    :rtype: Optional[Any]
    """
    try:
        return func(event)
    except Exception as e:
        error_handler(
            type(e),
            f"Error in function for histogram {histo_key}: {str(e)}",
            exc_traceback=None,
        )
        return None


def fill_histograms(ev: Any, histos_to_fill: Dict[str, Any]) -> None:
    """
    Fill predefined histograms with event data.

    :param ev: The event object containing data (instance of dtpr.base.Event)
    :type ev: Any
    :param histos_to_fill: Dictionary defining histograms to fill
    :type histos_to_fill: Dict[str, Any]
    :return: None
    :rtype: None
    """
    # Skip processing if event is None
    if ev is None:
        return

    for histo_key, histoinfo in histos_to_fill.items():
        # Skip draw-based histograms (they were filled before event loop)
        if histoinfo.get("type") == "root-draw":
            continue

        hType = histoinfo["type"]
        func = histoinfo["func"]

        # Get values from the event
        val = _execute_histo_function(func, ev, histo_key)
        if val is None:
            continue

        # Handle different histogram types
        # Distribution histograms (1D)
        if hType == "distribution":
            h = histoinfo["histo"]
            if isinstance(val, (list, tuple)):
                # Handle multi-value results
                for ival in collapse(val):
                    h.Fill(ival)
            elif val:
                h.Fill(val)

        # Efficiency histograms
        elif hType == "eff":
            num = histoinfo["histoNum"]
            den = histoinfo["histoDen"]

            # Get which values pass the criteria
            numPasses = _execute_histo_function(histoinfo["numdef"], ev, histo_key)
            if numPasses is None:
                continue

            # Fill denominator for all values, numerator only for passing values
            for v, passes in zip(val, numPasses):
                den.Fill(v)
                if passes:
                    num.Fill(v)

        # Multi-dimensional distributions (2D, 3D)
        elif hType in ("distribution2d", "distribution3d"):
            h = histoinfo["histo"]
            if isinstance(val, list):
                # Handle multiple points
                for ival in collapse(val, base_type=tuple):
                    h.Fill(*ival)
            else:
                h.Fill(*val)


def save_histograms(outfolder: str, tag: str, histos_to_save: Dict[str, Any]) -> None:
    """
    Store histograms in a ROOT file.

    :param outfolder: The output folder path
    :type outfolder: str
    :param tag: Tag to append to the filename
    :type tag: str
    :param histos_to_save: Dictionary of histograms to save
    :type histos_to_save: Dict[str, Any]
    :return: None
    :rtype: None
    """
    outname = os.path.join(outfolder, f"histograms{tag}.root")
    with r.TFile.Open(os.path.abspath(outname), "RECREATE") as f:
        for histo_key, histoinfo in histos_to_save.items():
            hType = histoinfo.get("type", "")

            # Write histograms to file based on type
            if hType == "root-draw":
                # Draw-based histogram
                if "histo" in histoinfo:
                    histoinfo["histo"].Write()
                else:
                    warnings.warn(
                        f"Draw histogram '{histo_key}' has no 'histo' object to write. Skipping."
                    )
            elif "distribution" in hType:
                if "histo" in histoinfo:
                    histoinfo["histo"].Write()
                else:
                    warnings.warn(
                        f"Histogram '{histo_key}' of type '{hType}' has no 'histo' object to write. Skipping."
                    )
            elif hType == "eff":
                has_num = "histoNum" in histoinfo
                has_den = "histoDen" in histoinfo
                if has_num and has_den:
                    histoinfo["histoNum"].Write()
                    histoinfo["histoDen"].Write()
                else:
                    missing_key = "histoNum" if not has_num else "histoDen"
                    warnings.warn(
                        f"Efficiency histogram '{histo_key}' is missing '{missing_key}'. Skipping."
                    )


def fill_histos(
    inpath: str,
    outfolder: str,
    tag: str,
    maxfiles: int,
    maxevents: int,
) -> None:
    """
    Fill histograms based on NTuples information.

    :param inpath: Path to the input folder containing NTuples
    :type inpath: str
    :param outfolder: Path to the output folder for histograms
    :type outfolder: str
    :param tag: Tag to identify the output histograms
    :type tag: str
    :param maxfiles: Maximum number of files to process
    :type maxfiles: int
    :param maxevents: Maximum number of events to process (0 = all)
    :type maxevents: int
    :return: None
    :rtype: None
    """
    color_msg("Running program to fill histograms...", "green")

    # Create the Ntuple object and set maxevents
    ntuple = NTuple(inputFolder=inpath, maxfiles=maxfiles)
    _maxevents = min(maxevents if maxevents > 0 else len(ntuple.events), len(ntuple.events)) - 1

    # Set up histograms to fill from configured sources
    event_histograms, draw_histograms = set_histograms_dict()
    if not event_histograms and not draw_histograms:
        color_msg("No histograms to fill.", color="red", indentLevel=2)
        return

    color_msg("Histograms to be filled:", color="blue", indentLevel=1)

    # Display draw histogram names (limited to 6 for readability)
    draw_keys = list(draw_histograms.keys())
    color_msg("Draw (ROOT):", color="blue", indentLevel=2)
    if draw_keys:
        if len(draw_keys) > 6:
            displayed_draw_msg = f"{', '.join(draw_keys[:6])} and {len(draw_keys) - 6} more..."
        else:
            displayed_draw_msg = f"{', '.join(draw_keys)}"
        color_msg(displayed_draw_msg, color="yellow", indentLevel=3)
    else:
        color_msg("None", color="yellow", indentLevel=3)

    # Display event-loop histogram names (limited to 6 for readability)
    event_keys = list(event_histograms.keys())
    color_msg("event loop:", color="blue", indentLevel=2)
    if event_keys:
        if len(event_keys) > 6:
            displayed_event_msg = f"{', '.join(event_keys[:6])} and {len(event_keys) - 6} more..."
        else:
            displayed_event_msg = f"{', '.join(event_keys)}"
        color_msg(displayed_event_msg, color="yellow", indentLevel=3)
    else:
        color_msg("None", color="yellow", indentLevel=3)

    # Execute draw-based histograms before event loop
    if draw_histograms:
        color_msg("Executing Draw-based histograms...", color="cyan", indentLevel=1)
        execute_draw_histograms(ntuple.tree, draw_histograms, _maxevents + 1)

    # Process events with progress bar
    with tqdm(
        total=_maxevents + 1,
        desc=color_msg("Executing Event Loop", color="purple", indentLevel=1, return_str=True),
        ncols=100,
        ascii=True,
        unit=" event",
    ) as pbar:

        # Sequential processing
        each_print = (_maxevents + 1) // 10 if (_maxevents + 1) > 10 else 1
        for i, ev in enumerate(ntuple.events):
            if i > _maxevents:
                pbar.update(_maxevents + 1 - pbar.n)
                break
            if i > 0 and i % each_print == 0:
                pbar.update(each_print)
            fill_histograms(ev, event_histograms)

    # Save histograms to output directory
    color_msg("Saving histograms...", color="purple", indentLevel=1)
    outpath = os.path.join(outfolder, "histograms")
    create_outfolder(outpath)

    all_histograms = {**event_histograms, **draw_histograms}
    save_histograms(outpath, tag, all_histograms)

    color_msg("Done!", color="green")
