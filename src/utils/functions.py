""" Miscelaneous """
import os
import ROOT as r
import numpy as np
from copy import deepcopy
from types import LambdaType
from pandas import DataFrame, concat
from dtpr.utils.functions import color_msg

r.gStyle.SetOptStat(0)

# Make Iterators for when we want to iterate over different subdetectors
wheels = range(-2, 3)
sectors = range(1, 15)
stations = range(1, 5)
superlayers = range(1, 4)

def flatten(lst):
    """
    Flattens a nested list. If the input is not a list, returns the single value as a list.

    Args:
        lst (list): The nested list to flatten.

    Returns:
        list: The flattened list or the single value as a list.
    """
    if not isinstance(lst, list):
        return [lst]
    
    result = []
    for i in lst:
        if isinstance(i, list):
            result.extend(flatten(i))
        else:
            result.append(i)
    return result

def wrap_lambda(func):
    if isinstance(func, LambdaType) and func.__name__ == "<lambda>":
        def wrapped_func(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapped_func
    return func

def create_outfolder(outname):
    """
    Creates an output directory if it does not exist.

    Args:
        outname (str): The path of the output directory.
    """
    if not (os.path.exists(outname)):
        os.system("mkdir -p %s" % outname)

def save_mpl_canvas(fig, name, path = "./results", dpi=500):
    """
    Save the given matplotlib figure to the specified path in SVG format.

    Parameters:
    fig (matplotlib.figure.Figure): The figure to save.
    name (str): The name of the file (without extension).
    path (str): The directory where the file will be saved. Default is "./results".
    """
    if not os.path.exists(path):
        os.system("mkdir -p %s"%(path))
    fig.savefig(path + "/" + name+".svg", dpi=dpi)
    return

def get_unique_locs(particles, loc_ids=["wh", "sc", "st"]):
    """
    Returns the unique locations of the specified particle types.

    Args:
        reader (object): The reader object containing the specified particle types.
        particle_types (list): The list of particle types to consider.
        loc_ids (list, optional): The location IDs. Default is ["wh", "sc", "st"].

    Returns:
        list: The unique locations of the specified particle types in tuple format.
    """
    locs = []

    if particles:
        for particle in particles:
            try:
                locs.append(tuple([getattr(particle, loc_id) for loc_id in loc_ids]))
            except AttributeError as er:
                raise ValueError(f"Location Id attribute not found in particle object: {er}")

    return set(locs)

def get_best_matches(reader, station=1):
    """
    Returns the best matching segments for each generator muon.

    Args:
        reader (object): The reader object containing generator muons.
        station (int): The station number. Default is 1.

    Returns:
        list: The best matching segments.
    """

    genmuons = reader.genmuons

    bestMatches = [None for igm in range(len(genmuons))]

    # This is what's done in Jaime's code: https://github.com/jaimeleonh/DTNtuples/blob/unifiedPerf/test/DTNtupleTPGSimAnalyzer_Efficiency.C#L181-L208
    # Basically: get the best matching segment to a generator muon per MB chamber

    # color_msg(f"[FUNCTIONS::GET_BEST_MATCHES] Debugging with station {station}", color = "red", indentLevel = 0)
    for igm, gm in enumerate(genmuons):
        # color_msg(f"[FUNCTIONS::GET_BEST_MATCHES] igm {igm}", indentLevel = 1)
        # gm.summarize(indentLevel = 2)
        for bestMatch in gm.matches:
            if bestMatch.st == station:
                bestMatches[igm] = bestMatch

    # Remove those that are None which are simply dummy values
    bestMatches = filter(lambda key: key is not None, bestMatches)
    return bestMatches
