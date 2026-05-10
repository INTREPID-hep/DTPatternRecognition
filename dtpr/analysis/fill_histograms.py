import os
import importlib
import warnings
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
from typing import Any, Dict, Optional


def set_histograms_dict() -> Dict[str, Any]:
    """
    Sets up the histograms dictionary to fill based on configuration.

    :return: Dictionary of histograms to fill
    :rtype: Dict[str, Any]
    """
    histos_to_fill = {}
    # Import histograms from each source in configuration
    for source in RUN_CONFIG.histo_sources:
        module = importlib.import_module(source)
        module_histos = getattr(module, "histos", {})
        # Only include histograms specified in the configuration
        histos_to_fill.update(
            {k: v for k, v in module_histos.items() if k in RUN_CONFIG.histo_names}
        )

    # Warn about any missing histograms
    missing_histos = set(RUN_CONFIG.histo_names) - set(histos_to_fill.keys())
    if missing_histos:
        warnings.warn(
            f"The following histograms could not be found in any of the sources: {', '.join(missing_histos)}"
        )

    return histos_to_fill


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
        for histoinfo in histos_to_save.values():
            hType = histoinfo["type"]

            # Write histograms to file based on type
            if "distribution" in hType:
                histoinfo["histo"].Write()
            elif hType == "eff":
                histoinfo["histoNum"].Write()
                histoinfo["histoDen"].Write()


def fill_histos(
    inpath: str, outfolder: str, tag: str, maxfiles: int, maxevents: int,
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
    histograms_to_fill = set_histograms_dict()
    color_msg("Histograms to be filled:", color="blue", indentLevel=1)

    if not histograms_to_fill:
        color_msg("No histograms to fill.", color="red", indentLevel=2)
        return

    # Display histogram names (limited to 6 for readability)
    histo_keys = list(histograms_to_fill.keys())
    if len(histo_keys) > 6:
        displayed_msg = f"{', '.join(histo_keys[:6])} and {len(histo_keys) - 6} more..."
    else:
        displayed_msg = f"{', '.join(histo_keys)}"
    color_msg(displayed_msg, color="yellow", indentLevel=2)

    # Process events with progress bar
    with tqdm(
        total=_maxevents + 1,
        desc=color_msg("Processing events", color="purple", indentLevel=1, return_str=True),
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
            fill_histograms(ev, histograms_to_fill)


    # Save histograms to output directory
    color_msg("Saving histograms...", color="purple", indentLevel=1)
    outpath = os.path.join(outfolder, "histograms")
    create_outfolder(outpath)

    save_histograms(outpath, tag, histograms_to_fill)

    color_msg("Done!", color="green")
