import os
import importlib
import warnings
import time
from array import array
from functools import partial
from itertools import islice
from ..core.engine import run_chunks, split_entries, job_range
from ..utils.dumper import dump_events
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

# The progress bar refreshes about PROGRESS_UPDATES times over a run; when one
# refresh would take longer than PROGRESS_MAX_GAP seconds the blocks shrink, so
# a slow range never leaves the bar still.
PROGRESS_UPDATES = 10
PROGRESS_MAX_GAP = 5.0


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
    if ">>" not in draw_expr:
        return None

    right_side = draw_expr.split(">>", 1)[1].strip()
    if not right_side:
        return None

    hist_name = right_side.split("(", 1)[0].strip()
    return hist_name or None


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


def execute_draw_histograms(
    tree: Any, draw_histograms: Dict[str, Any], maxentries: int, firstentry: int = 0
) -> None:
    """
    Execute all Draw-based histograms and retrieve from ROOT gDirectory.

    :param tree: ROOT TChain or TTree to draw from
    :type tree: Any
    :param draw_histograms: Dictionary of draw histogram definitions
    :type draw_histograms: Dict[str, Any]
    :param maxentries: Number of entries to process (>0) or all entries (<=0)
    :type maxentries: int
    :param firstentry: First entry to draw (the chunks of a parallel run each
        draw their own range and the merge adds them)
    :type firstentry: int
    :return: None
    :rtype: None
    """
    r.gSystem.Load("libPhysics")
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

        try:
            draw_result = tree.Draw(draw_expr, selection, option, nentries, firstentry)
            if draw_result < 0:
                warnings.warn(
                    f"ROOT Draw for '{hname}' returned error code {draw_result}. Skipping."
                )
                continue
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
    import fnmatch

    config = CONFIG if CONFIG is not None else RUN_CONFIG
    histos_to_fill = {}

    # Import histograms from each source in configuration
    for source in getattr(config, "histo_sources", []):
        module = importlib.import_module(source)
        module_histos = getattr(module, "histos", {})

        # Get requested names/patterns
        requested = getattr(config, "histo_names", [])

        # Match each loaded histogram against the patterns
        for hname, histoinfo in module_histos.items():
            for pattern in requested:
                if fnmatch.fnmatch(hname, pattern):
                    histos_to_fill[hname] = histoinfo
                    break  # No need to match against other patterns

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


def distribution_values(val: Any) -> list:
    """
    Values a 1D distribution histogram receives for one event.

    :param val: Result of the histogram function
    :type val: Any
    :return: The values to fill
    :rtype: list
    """
    if isinstance(val, (list, tuple)):
        return list(collapse(val))
    return [val] if val else []


def fill_histograms(
    ev: Any, histos_to_fill: Dict[str, Any], buffers: Optional[Dict[str, list]] = None
) -> None:
    """
    Fill predefined histograms with event data.

    When ``buffers`` is given, 1D values are appended to it instead of being
    filled one by one; ``flush_histograms`` then fills them with ``FillN``. This
    is worth it: ROOT digests ~25 values per ``Fill`` call but a ``FillN`` call
    takes an array, and a production run fills millions of values.

    :param ev: The event object containing data (instance of dtpr.base.Event)
    :type ev: Any
    :param histos_to_fill: Dictionary defining histograms to fill
    :type histos_to_fill: Dict[str, Any]
    :param buffers: Optional per-histogram value buffers
    :type buffers: Dict[str, list], optional
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
            values = distribution_values(val)
            if buffers is not None:
                buffers.setdefault(histo_key, []).extend(values)
                continue
            h = histoinfo["histo"]
            for ival in values:
                h.Fill(ival)

        # Efficiency histograms
        elif hType == "eff":
            # Get which values pass the criteria
            numPasses = _execute_histo_function(histoinfo["numdef"], ev, histo_key)
            if numPasses is None:
                continue

            if buffers is not None:
                buffers.setdefault(histo_key, []).extend(zip(val, numPasses))
                continue

            num = histoinfo["histoNum"]
            den = histoinfo["histoDen"]
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


def fill_n(histogram: Any, values: list) -> None:
    """
    Fill a 1D histogram with one array, in a single ROOT call.

    :param histogram: The histogram to fill
    :type histogram: Any
    :param values: The values to fill
    :type values: list
    :return: None
    :rtype: None
    """
    if not values:
        return
    weights = array("d", [1.0]) * len(values)
    histogram.FillN(len(values), array("d", values), weights, 1)


def flush_histograms(buffers: Dict[str, list], histos_to_fill: Dict[str, Any]) -> None:
    """
    Fill every buffered histogram with ``FillN`` and empty the buffers.

    :param buffers: Per-histogram value buffers filled by fill_histograms
    :type buffers: Dict[str, list]
    :param histos_to_fill: Dictionary defining histograms to fill
    :type histos_to_fill: Dict[str, Any]
    :return: None
    :rtype: None
    """
    for histo_key, values in buffers.items():
        if not values:
            continue
        histoinfo = histos_to_fill[histo_key]
        if histoinfo.get("type") == "eff":
            fill_n(histoinfo["histoDen"], [value for value, _ in values])
            fill_n(histoinfo["histoNum"], [value for value, passes in values if passes])
        else:
            fill_n(histoinfo["histo"], values)
        values.clear()


def fill_histograms_range(
    events: Any, histos_to_fill: Dict[str, Any], progress: bool = False
) -> None:
    """
    Fill the histograms over an event range, buffering the 1D values.

    The progress bar is advanced a fixed number of times over the range instead
    of once per event: a tqdm call per event is Python overhead that adds up on
    long runs. A block that takes longer than ``PROGRESS_MAX_GAP`` seconds is
    split, so a slow range still shows progress.

    :param events: The events to process
    :type events: Iterable
    :param histos_to_fill: Dictionary defining histograms to fill
    :type histos_to_fill: Dict[str, Any]
    :param progress: Show a progress bar (only for a serial run)
    :type progress: bool
    :return: None
    :rtype: None
    """
    buffers: Dict[str, list] = {}

    if not progress:
        for ev in events:
            fill_histograms(ev, histos_to_fill, buffers)
        flush_histograms(buffers, histos_to_fill)
        return

    total = len(events)
    # The bar refreshes about ten times over the range, between blocks of
    # events, instead of paying a tqdm call for every single event
    step = max(1, total // PROGRESS_UPDATES)
    bar = tqdm(
        total=total,
        desc=color_msg("Executing Event Loop", color="purple", indentLevel=1, return_str=True),
        ncols=100,
        ascii=True,
        unit=" event",
    )
    stream = iter(events)
    shown = 0
    while shown < total:
        size = min(step, total - shown)
        started = time.perf_counter()
        for ev in islice(stream, size):
            fill_histograms(ev, histos_to_fill, buffers)
        shown += size
        bar.update(size)
        # A block slower than the gap is split, so the bar never sits still
        elapsed = time.perf_counter() - started
        if elapsed > PROGRESS_MAX_GAP:
            step = max(1, int(size * PROGRESS_MAX_GAP / elapsed))
    bar.close()

    flush_histograms(buffers, histos_to_fill)


def write_histograms_file(outpath: str, histos_to_save: Dict[str, Any]) -> None:
    """
    Store histograms in one ROOT file.

    :param outpath: Path of the ROOT file to write
    :type outpath: str
    :param histos_to_save: Dictionary of histograms to save
    :type histos_to_save: Dict[str, Any]
    :return: None
    :rtype: None
    """
    create_outfolder(os.path.dirname(os.path.abspath(outpath)))
    with r.TFile.Open(os.path.abspath(outpath), "RECREATE") as f:
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


def save_histograms(outfolder: str, tag: str, histos_to_save: Dict[str, Any]) -> None:
    """
    Store histograms in a ROOT file named after the tag.

    :param outfolder: The output folder path
    :type outfolder: str
    :param tag: Tag to append to the filename
    :type tag: str
    :param histos_to_save: Dictionary of histograms to save
    :type histos_to_save: Dict[str, Any]
    :return: None
    :rtype: None
    """
    write_histograms_file(os.path.join(outfolder, f"histograms{tag}.root"), histos_to_save)


def write_histograms_chunk(
    chunk: Tuple[int, int],
    outpath: str,
    inpath: str,
    maxfiles: int = -1,
    progress: bool = False,
) -> None:
    """
    Fill the histograms of one entry range and write them to one ROOT file.

    The draw-based histograms are drawn over the same range, so the merge of
    the chunk files adds up to the histogram of the full range.

    :param chunk: Pair ``(begin, end)`` of entries to process
    :type chunk: Tuple[int, int]
    :param outpath: Path of the ROOT file to write for this chunk
    :type outpath: str
    :param inpath: Path to the input folder containing NTuples
    :type inpath: str
    :param maxfiles: Maximum number of files to process
    :type maxfiles: int
    :param progress: Show the event progress bar (only for a serial run)
    :type progress: bool
    :return: None
    :rtype: None
    """
    begin, end = chunk
    ntuple = NTuple(inputFolder=inpath, maxfiles=maxfiles, entry_range=(begin, end))

    event_histograms, draw_histograms = set_histograms_dict()
    if draw_histograms:
        execute_draw_histograms(ntuple.tree, draw_histograms, end - begin, firstentry=begin)
    if event_histograms:
        fill_histograms_range(ntuple.events, event_histograms, progress=progress)

    write_histograms_file(outpath, {**event_histograms, **draw_histograms})


def fill_histos(
    inpath: str,
    outfolder: str,
    tag: str,
    maxfiles: int,
    maxevents: int,
    also_dump: bool = False,
    remove_nbranches: bool = False,
    ncores: int = -1,
    merge: bool = True,
    job_id: int = 0,
    job_count: int = 1,
) -> None:
    """
    Fill histograms based on NTuples information.

    The entries are split into one chunk per core; each chunk fills its own
    histograms (draw-based ones over its own range) and writes a ROOT file, and
    the files are merged with ROOT. With ``merge=False`` the chunk files are
    kept for a manual ``hadd``.

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
    :param ncores: Number of worker processes; <= 1 runs serially, 0 uses every core
    :type ncores: int
    :param merge: Merge the chunk files into the final histogram file
    :type merge: bool
    :param job_id: Index of this job when the run is split across machines
    :type job_id: int
    :param job_count: Number of jobs the entries are split across
    :type job_count: int
    :return: None
    :rtype: None
    """
    color_msg("Running program to fill histograms...", "green")

    # Create the Ntuple object and set maxevents
    ntuple = NTuple(inputFolder=inpath, maxfiles=maxfiles)
    total = min(maxevents if maxevents > 0 else len(ntuple.events), len(ntuple.events))

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

    # Split the entries: one slice per job, one chunk per core inside the job
    job_begin, job_end = job_range(total, job_id, job_count)
    ncores = os.cpu_count() if ncores == 0 else ncores
    chunks = [
        (job_begin + begin, job_begin + end)
        for begin, end in split_entries(job_end - job_begin, max(1, ncores))
    ]
    suffix = f"_p{job_id}" if job_count > 1 else ""
    outpath = os.path.join(outfolder, "histograms", f"histograms{tag}{suffix}.root")

    if ncores > 1:
        color_msg(
            f"Filling {len(chunks)} chunks on {min(ncores, len(chunks))} cores...",
            color="cyan",
            indentLevel=1,
        )

    writer = partial(
        write_histograms_chunk,
        inpath=inpath,
        maxfiles=maxfiles,
        progress=ncores <= 1,
    )
    run_chunks(writer, chunks, ncores, outpath, merge=merge, desc="histograms")

    color_msg("Histograms saved in:", color="purple", indentLevel=1)
    color_msg(outpath, color="yellow", indentLevel=2)

    if also_dump:
        color_msg("Also dumping events...", color="purple", indentLevel=1)
        dump_events(
            ntuple.events[job_begin:job_end],
            outpath=os.path.join(outfolder, "roots"),
            tag=tag + suffix,
            fRNTuple=False,
            dump_yaml_schema=True,
            force_overwrite_yaml=False,
            remove_nbranches=remove_nbranches,
        )

    color_msg("Done!", color="green")
