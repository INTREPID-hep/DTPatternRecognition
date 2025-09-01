import os
import importlib
import warnings
import ROOT as r
from tqdm import tqdm
from dtpr.base import NTuple
from dtpr.base.config import RUN_CONFIG
from dtpr.utils.functions import (
    color_msg,
    error_handler,
    create_outfolder,
)
from more_itertools import collapse
from multiprocess import Pool, cpu_count
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


def process_event_chunk(
    index: int, start_idx: int, end_idx: int, events: Any, histos_to_fill: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Process a chunk of events for parallel execution.

    :param index: Worker index for naming cloned histograms
    :type index: int
    :param start_idx: Starting event index
    :type start_idx: int
    :param end_idx: Ending event index
    :type end_idx: int
    :param events: List of all events
    :type events: Any
    :param histos_to_fill: Dictionary of histograms to fill
    :type histos_to_fill: Dict[str, Any]
    :return: Dictionary of filled histograms for this chunk
    :rtype: Dict[str, Any]
    """
    # Clone histograms for this worker to avoid thread safety issues
    c_histos_to_fill = {}
    for key, val in histos_to_fill.items():
        if isinstance(val, (r.TH1, r.TH2, r.TH3)):
            _val = val.Clone(val.GetName() + f"_{index}")
        else:
            _val = val
        c_histos_to_fill[key] = _val

    # Process all events in this chunk
    for ev in events[start_idx:end_idx]:
        if ev is None:
            continue
        fill_histograms(ev, c_histos_to_fill)

    return c_histos_to_fill


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
    inpath: str, outfolder: str, tag: str, maxfiles: int, maxevents: int, ncores: int
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
    :param ncores: Number of CPU cores to use (1 = sequential, >1 = parallel)
    :type ncores: int
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

    # Determine number of cores for processing
    _ncores = min(ncores, cpu_count()) if ncores > 1 else None

    # Process events with progress bar
    with tqdm(
        total=_maxevents + 1,
        desc=color_msg("Processing events", color="purple", indentLevel=1, return_str=True),
        ncols=100,
        ascii=True,
        unit=" event",
    ) as pbar:
        if _ncores is None:
            # Sequential processing
            each_print = (_maxevents + 1) // 10 if (_maxevents + 1) > 10 else 1
            for i, ev in enumerate(ntuple.events):
                if i > _maxevents:
                    pbar.update(_maxevents + 1 - pbar.n)
                    break
                if i > 0 and i % each_print == 0:
                    pbar.update(each_print)
                fill_histograms(ev, histograms_to_fill)

            histograms_result = histograms_to_fill
        else:
            # Parallel processing with worker pool
            chunk_size = (_maxevents + 1) // _ncores
            with Pool(_ncores) as pool:
                results = []
                for i in range(_ncores):
                    start_idx = i * chunk_size
                    end_idx = min((i + 1) * chunk_size, _maxevents + 1)

                    results.append(
                        pool.apply_async(
                            process_event_chunk,
                            args=(i, start_idx, end_idx, ntuple.events, histograms_to_fill),
                            callback=lambda _, i=i: pbar.write(f"Processed events chunk {i}")
                            or pbar.update(chunk_size),
                        )
                    )

                # Gather results from all workers
                histograms_results = [r.get() for r in results]

    # Save histograms to output directory
    color_msg("Saving histograms...", color="purple", indentLevel=1)
    outpath = os.path.join(outfolder, "histograms")
    create_outfolder(outpath)

    if _ncores is None:
        # Direct save for sequential processing
        save_histograms(outpath, tag, histograms_result)
    else:
        # For parallel processing, save temporary files and merge them
        import subprocess as bash

        tmp_path = os.path.join(outpath, "_tmp")
        create_outfolder(tmp_path)

        histo_files = []
        for i, histograms_i in enumerate(histograms_results):
            file_path = os.path.join(tmp_path, f"histograms_{i}.root")
            save_histograms(tmp_path, f"_{i}", histograms_i)
            histo_files.append(file_path)

        # Merge all temporary files with hadd
        color_msg("Merging histograms...", color="purple", indentLevel=1)
        output_file = os.path.join(outpath, f"histograms{tag}.root")
        bash.call(f"hadd -f -j {_ncores} {output_file} {' '.join(histo_files)}", shell=True)
        color_msg("Cleaning up temporary files...", color="purple", indentLevel=1)
        bash.call(f"rm -rf {tmp_path}", shell=True)
    color_msg("Done!", color="green")
