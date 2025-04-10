import os
import importlib
import warnings
import ROOT as r
from tqdm import tqdm
from dtpr.base import NTuple
from dtpr.utils.functions import (
    color_msg,
    error_handler,
    create_outfolder,
    flatten,
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
    Fill predefined histograms with event data.

    This function processes an event and fills histograms based on the provided dictionary. 
    Histograms can represent distributions, efficiencies, or multi-dimensional distributions.

    
    :param ev: The event object containing data to fill histograms. It should be an instance of ``dtpr.base.Event``.
    :type ev: Event
    :param histos_to_fill: A dictionary defining the histograms to fill. Each entry in the 
        dictionary specifies the histogram type, the ROOT histogram object, and the function 
        to compute the values to fill.
    :type histos_to_fill: dict

    :raises Exception: If the function for computing histogram values fails, an error is logged, 
        and the histogram filling for that entry is skipped.
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

    :param outfolder: The output folder to save the rootfile.
    :type outfolder: (str)
    :param tag: The tag to append to the rootfile name.
    :type tag: (str)
    :param histograms_to_save: The histograms to save.
    :type histograms_to_save: (dict)
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
    Fill histograms based on NTuples information.

    :param inpath: Path to the input folder containing the ntuples.
    :type inpath: (str)
    :param outfolder: Path to the output folder where histograms will be saved.
    :type outfolder: (str)
    :param tag: Tag to identify the output histograms.
    :type tag: (str)
    :param maxfiles: Maximum number of files to process.
    :type maxfiles: (int)
    :param maxevents: Maximum number of events to process.
    :type maxevents: (int)
    """

    # Start of the analysis
    color_msg(f"Running program to fill histograms...", "green")

    # Create the Ntuple object
    ntuple = NTuple(
        inputFolder=inpath,
        maxfiles=maxfiles
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
        each_print = _maxevents // 10 if _maxevents > 10 else 1
        for ev in ntuple.events:
            if not ev:
                continue
            if ev.index % each_print == 0:
                pbar.update(each_print)
            if ev.index >= _maxevents:
                break

            fill_histograms(ev, histograms_to_fill)

    color_msg(f"Saving histograms...", color="purple", indentLevel=1)
    create_outfolder(os.path.join(outfolder, "histograms"))
    save_histograms(os.path.join(outfolder, "histograms"), tag, histograms_to_fill)

    color_msg(f"Done!", color="green")
