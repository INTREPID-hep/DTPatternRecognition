import os
import importlib
import warnings
import ROOT as r
from tqdm import tqdm
from dtpr.utils.functions import (
    color_msg,
    error_handler,
    create_outfolder,
    flatten,
    init_ntuple_from_config,
)
from dtpr.utils.config import RUN_CONFIG


def set_histograms_dict():
    """
    Sets the histograms dictionary to fill.

    Returns:
        dict: The dictionary of histograms to fill.
    """
    histos_to_fill = {}
    for source in RUN_CONFIG.histo_sources:
        module = importlib.import_module(source)
        module_histos = getattr(module, "histos", {})
        for key, value in module_histos.items():
            if key in RUN_CONFIG.histo_names:
                histos_to_fill[key] = value

    missing_histos = set(RUN_CONFIG.histo_names) - set(histos_to_fill.keys())
    if missing_histos:
        warnings.warn(
            f"The following histograms could not be found in any of the sources: {', '.join(missing_histos)}"
        )

    return histos_to_fill


def fill_histograms(ev, histos_to_fill):
    """
    Apply selections and fill histograms.

    Args:
        ev (Event): The event object containing data to fill histograms.
        histos_to_fill (dict, optional): The histograms to fill. Default is None.
    """
    for histo_key, histoinfo in histos_to_fill.items():
        hType = histoinfo["type"]

        # Distributions
        if hType == "distribution":
            h = histoinfo["histo"]
            func = histoinfo["func"]
            try:
                val = func(ev)
            except Exception as e:
                error_handler(
                    type(e),
                    f"Error in function for histogram {histo_key}:" + str(e),
                    exc_traceback=None,
                )
                continue

            # In case a function returns multiple results
            # and we want to fill for everything
            if isinstance(val, (list, tuple)):
                val = flatten(val)
                for ival in val:
                    h.Fill(ival)
            elif val:
                h.Fill(val)

        # Efficiencies
        elif hType == "eff":
            func = histoinfo["func"]
            num = histoinfo["histoNum"]
            den = histoinfo["histoDen"]
            numdef = histoinfo["numdef"]

            try:
                val = func(ev)
                numPasses = numdef(ev)
            except Exception as e:
                error_handler(
                    type(e),
                    f"Error in function for histogram {histo_key}:" + str(e),
                    exc_traceback=None,
                )
                continue

            for val, passes in zip(val, numPasses):
                den.Fill(val)
                if passes:
                    num.Fill(val)

        # 2D-3D Distributions
        elif hType == "distribution2d" or hType == "distribution3d":
            h = histoinfo["histo"]
            func = histoinfo["func"]
            try:
                val = func(ev)
            except Exception as e:
                error_handler(
                    type(e),
                    f"Error in function for histogram {histo_key}:" + str(e),
                    exc_traceback=None,
                )
                continue
            if isinstance(val, list):
                for ival in val:
                    h.Fill(*ival)
            else:
                h.Fill(*val)


def save_histograms(outfolder, tag, histos_to_save):
    """
    Stores histograms in a rootfile.

    Args:
        outfolder (str): The output folder to save the rootfile.
        tag (str): The tag to append to the rootfile name.
        histograms_to_save (dict): The histograms to save.
    """
    outname = os.path.join(outfolder, "histograms%s.root" % (tag))
    with r.TFile.Open(os.path.abspath(outname), "RECREATE") as f:
        for hname, histoinfo in histos_to_save.items():
            hType = histoinfo["type"]
            if "distribution" in hType:
                histo = histoinfo["histo"]
                histo.Write(histo.GetName())

            elif hType == "eff":
                histoNum = histoinfo["histoNum"]
                histoDen = histoinfo["histoDen"]
                histoNum.Write(histoNum.GetName())
                histoDen.Write(histoDen.GetName())


def fill_histos(inpath, outfolder, tag, maxfiles, maxevents):
    """
    Fill histograms based on DtNTuples information.

    Parameters:
    inpath (str): Path to the input folder containing the ntuples.
    outfolder (str): Path to the output folder where histograms will be saved.
    tag (str): Tag to identify the output histograms.
    maxfiles (int): Maximum number of files to process.
    maxevents (int): Maximum number of events to process.
    """

    # Start of the analysis
    color_msg(f"Running program to fill histogrmas...", "green")

    # Create the Ntuple object
    ntuple = init_ntuple_from_config(
        inputFolder=inpath,
        maxfiles=maxfiles,
        config=RUN_CONFIG,
    )

    # set maxevents
    _maxevents = (
        maxevents
        if maxevents > 0 and maxevents < len(ntuple.events)
        else len(ntuple.events)
    )

    # setting up which histograms will be fill
    histograms_to_fill = set_histograms_dict()
    color_msg(f"Histograms to be filled:", color="blue", indentLevel=1)
    if not histograms_to_fill:
        color_msg(f"No histograms to fill.", color="red", indentLevel=2)
        return

    size = len(histograms_to_fill)
    if size > 6:
        displayed_histos = ", ".join(list(histograms_to_fill.keys())[:6])
        color_msg(
            f"{displayed_histos} and {size - 6} more...", color="yellow", indentLevel=2
        )
    else:
        color_msg(
            f"{', '.join(histograms_to_fill.keys())}", color="yellow", indentLevel=2
        )

    with tqdm(
        total=_maxevents,
        desc=color_msg(
            f"Processing events", color="purple", indentLevel=1, return_str=True
        ),
        ncols=100,
        ascii=True,
        unit=" event",
    ) as pbar:
        for ev in ntuple.events:
            if not ev:
                continue
            if ev.index % (_maxevents // 10) == 0:
                pbar.update(_maxevents // 10)
            if ev.index >= _maxevents:
                break
            fill_histograms(ev, histograms_to_fill)

    color_msg(f"Saving histograms...", color="purple", indentLevel=1)
    create_outfolder(os.path.join(outfolder, "histograms"))
    save_histograms(os.path.join(outfolder, "histograms"), tag, histograms_to_fill)

    color_msg(f"Done!", color="green")
