""" Miscelaneous """
from functools import partial
import importlib
import os
import math
import matplotlib.pyplot as plt
from copy import deepcopy
from types import LambdaType
from importlib import import_module
import numpy as np
from mpldts.geometry import Station
from mpldts.patches import DTStationPatch
from dtpr.utils.config import RUN_CONFIG

# Make Iterators for when we want to iterate over different subdetectors
wheels = range(-2, 3)
sectors = range(1, 15)
stations = range(1, 5)
superlayers = range(1, 4)

def color_msg(msg, color="none", indentLevel=-1, return_str=False, bold=False, underline=False, bkg_color="none"):
    """
    Prints a message with ANSI coding so it can be printed with colors.

    :param msg: The message to print.
    :type msg: str
    :param color: The color to use for the message. Default is "none".
    :type color: str
    :param indentLevel: The level of indentation. Default is -1.
    :type indentLevel: int
    :param return_str: If True, returns the formatted message as a string. Default is False.
    :type return_str: bool
    :param bold: If True, makes the text bold. Default is False.
    :type bold: bool
    :param underline: If True, underlines the text. Default is False.
    :type underline: bool
    :param bkg_color: The background color. Default is "none".
    :type bkg_color: str
    :return: The formatted message if return_str is True.
    :rtype: str
    """
    style_digit = "0"
    if bold and underline:
        style_digit = "1;4"
    elif bold:
        style_digit = "1"
    elif underline:
        style_digit = "4"

    colors = ["black", "red", "green", "yellow", "blue", "purple", "cyan", "white"]
    font_colors = {color: f";{30 + i}" for i, color in enumerate(colors)}
    background_colors = {color: f";{40 + i}" for i, color in enumerate(colors)}
    font_colors["none"] = ""
    background_colors["none"] = ""

    try:
        ansi_code = f"{style_digit}{font_colors[color]}{background_colors[bkg_color]}m"
    except KeyError:
        ansi_code = f"{style_digit}{font_colors['none']}{background_colors['none']}m"

    indentStr = ""
    if indentLevel == 0:
        indentStr = ">>"
    if indentLevel == 1:
        indentStr = "+"
    if indentLevel == 2:
        indentStr = "*"
    if indentLevel == 3:
        indentStr = "-->"
    if indentLevel >= 4:
        indentStr = "-"

    formatted_msg = "\033[%s%s %s\033[0m" % (
        ansi_code,
        "  " * indentLevel + indentStr,
        msg,
    )

    if return_str:
        return formatted_msg
    else:
        print(formatted_msg)


def warning_handler(message, category, filename, lineno, file=None, line=None):
    """
    Handles warnings by printing them with color formatting.

    :param message: The warning message.
    :type message: str
    :param category: The category of the warning.
    :type category: Warning
    :param filename: The name of the file where the warning occurred.
    :type filename: str
    :param lineno: The line number where the warning occurred.
    :type lineno: int
    :param file: The file object. Default is None.
    :type file: file object, optional
    :param line: The line of code where the warning occurred. Default is None.
    :type line: str, optional
    """
    print(
        "".join(
            [
                color_msg(
                    f"{category.__name__} in:",
                    color="yellow",
                    return_str=True,
                    indentLevel=-1,
                ),
                color_msg(
                    f"{filename}-{lineno} :",
                    color="purple",
                    return_str=True,
                    indentLevel=-1,
                ),
                color_msg(f"{message}", return_str=True, indentLevel=-1),
            ]
        )
    )


def error_handler(exc_type, exc_value, exc_traceback):
    """
    Handles errors by printing them with color formatting.

    :param exc_type: The type of the exception.
    :type exc_type: type
    :param exc_value: The exception instance.
    :type exc_value: Exception
    :param exc_traceback: The traceback object.
    :type exc_traceback: traceback
    """
    import traceback

    print(
        "".join(
            [
                color_msg(
                    f"{exc_type.__name__}:",
                    color="red",
                    return_str=True,
                    indentLevel=-1,
                ),
                color_msg(
                    f"{exc_value}", color="yellow", return_str=True, indentLevel=-1
                ),
                color_msg(
                    (
                        "Traceback (most recent call last):"
                        + "".join(traceback.format_tb(exc_traceback))
                        if exc_traceback
                        else ""
                    ),
                    return_str=True,
                    indentLevel=-1,
                ),
            ]
        )
    )

def get_callable_from_src(src_str: str):
    """
    Returns the callable object from the given source string.

    :param src_str: The source string containing the callable.
    :type src: str
    :return: The callable object.
    """
    callable = None
    try:
        _module_name, _callable_name = src_str.rsplit(".", 1)
        _module = import_module(_module_name)
        callable = getattr(_module, _callable_name)
    except AttributeError as e:
        raise AttributeError(f"{_callable_name} callable not found: {e}")
    except ImportError as e:
        raise ImportError(f"Error importing {src_str}: {e}")

    return callable


def flatten(lst):
    """
    Flattens a nested list. If the input is not a list, returns the single value as a list.

    :param lst: The nested list to flatten.
    :type lst: list
    :return: The flattened list or the single value as a list.
    :rtype: list
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

    :param outname: The path of the output directory.
    :type outname: str
    """
    if not (os.path.exists(outname)):
        os.system("mkdir -p %s" % outname)

def save_mpl_canvas(fig, name, path = "./results", dpi=500):
    """
    Save the given matplotlib figure to the specified path in SVG format.

    :param fig: The figure to save.
    :type fig: matplotlib.figure.Figure
    :param name: The name of the file (without extension).
    :type name: str
    :param path: The directory where the file will be saved. Default is "./results".
    :type path: str
    :param dpi: The resolution of the saved figure. Default is 500.
    :type dpi: int
    """
    if not os.path.exists(path):
        os.system("mkdir -p %s"%(path))
    fig.savefig(path + "/" + name+".svg", dpi=dpi)
    return

def append_to_matched_list(obj, matched_list_name, item):
    """
    Append an item to a matched list attribute of an object if it doesn't already exist.

    :param obj: The object containing the matched list.
    :type obj: Any
    :param matched_list_name: The name of the matched list attribute.
    :type matched_list_name: str
    :param item: The item to append.
    :type item: Any
    """
    if not hasattr(obj, matched_list_name):
        setattr(obj, matched_list_name, [])
    if item not in getattr(obj, matched_list_name):
        getattr(obj, matched_list_name).append(item)

def get_unique_locs(particles, loc_ids=["wh", "sc", "st"]):
    """
    Returns the unique locations of the specified particle types.

    :param particles: The list of particle objects.
    :type particles: list
    :param loc_ids: The location IDs. Default is ["wh", "sc", "st"].
    :type loc_ids: list, optional
    :return: The unique locations of the specified particle types in tuple format.
    :rtype: set
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

    :param reader: The reader object containing generator muons.
    :type reader: object
    :param station: The station number. Default is 1.
    :type station: int
    :return: The best matching segments.
    :rtype: list
    """

    genmuons = reader.genmuons

    bestMatches = [None for igm in range(len(genmuons))]

    # This is what's done in Jaime's code: https://github.com/jaimeleonh/DTNtuples/blob/unifiedPerf/test/DTNtupleTPGSimAnalyzer_Efficiency.C#L181-L208
    # Basically: get the best matching segment to a generator muon per MB chamber

    # color_msg(f"[FUNCTIONS::GET_BEST_MATCHES] Debugging with station {station}", color = "red", indentLevel = 0)
    for igm, gm in enumerate(genmuons):
        # color_msg(f"[FUNCTIONS::GET_BEST_MATCHES] igm {igm}", indentLevel = 1)
        # gm.summarize(indentLevel = 2)
        for bestMatch in getattr(gm, 'matched_segments', []):
            if bestMatch.st == station:
                bestMatches[igm] = bestMatch

    # Remove those that are None which are simply dummy values
    bestMatches = filter(lambda key: key is not None, bestMatches)
    return bestMatches

def deltaPhi(phi1, phi2):
    """
    Calculates the difference in phi between two angles.

    :param phi1: The first angle in radians.
    :type phi1: float
    :param phi2: The second angle in radians.
    :type phi2: float
    :return: The difference in phi.
    :rtype: float
    """
    res = phi1 - phi2
    while res > math.pi:
        res -= 2 * math.pi
    while res <= -math.pi:
        res += 2 * math.pi
    return res


def deltaR(p1, p2):
    """
    Calculates the delta R between two particles. Particles must have attributes eta and phi.

    :param p1: The first particle with attributes eta and phi.
    :type p1: object
    :param p2: The second particle with attributes eta and phi.
    :type p2: object
    :return: The delta R value.
    :rtype: float
    """
    dEta = abs(p1.eta - p2.eta)
    dPhi = deltaPhi(p1.phi, p2.phi)
    return math.sqrt(dEta * dEta + dPhi * dPhi)


def phiConv(phi):
    """
    Converts a phi value.

    :param phi: The phi value to convert.
    :type phi: float
    :return: The converted phi value.
    :rtype: float
    """
    return 0.5 * phi / 65536.0

def correct_g4digi_time(g4digi):
    """
    Correct the time of the digi by simulating the drift time.
    """
    # ----- mimic the Javi's Code ----
    # simulate drift time

    mean, stddev = 175, 75
    time_offset = 400
    delay = np.random.normal(loc=mean, scale=stddev)
    return g4digi._time + abs(delay) + time_offset # why abs ?


def format_event_attribute_str(key, value, indent):
    return (
        color_msg(f"{key.capitalize()}:", color="green", indentLevel=indent, return_str=True)
        + color_msg(f"{value}", color="none", indentLevel=-1, return_str=True)
    )

def format_event_particles_str(ptype, particles, indent):
    summary = [
        color_msg(f"{ptype.capitalize()}", color="green", indentLevel=indent, return_str=True),
        color_msg(
            f"Number of {ptype}: {len(particles)}", color="purple", indentLevel=indent + 1,
            return_str=True
        ),
    ]

    if ptype == "genmuons":
        for gm in particles:
            summary.append(
                gm.__str__(indentLevel=indent + 1, color="cyan", exclude=["matched_tps", "matched_segments"]) + "\n"
                + color_msg(
                    f"Matched offline - segments: {len(gm.matched_segments)}", color="none", indentLevel=indent + 2, return_str=True
                ) + "\n"
                + color_msg(
                    f"Matched AM TPs: {len(gm.matched_tps)}", color="none", indentLevel=indent + 2, return_str=True
                )
            )

    elif ptype == "segments":
        matches_segments = [seg for seg in particles if seg.matched_tps]
        if matches_segments:
            summary.append(color_msg("Segs which match an AM-TP:", color="cyan", indentLevel=indent + 1, return_str=True))
            summary.extend(
                seg.__str__(indentLevel=indent + 2, color="cyan", include=["wh", "sc", "st", "phi", "eta"])
                for seg in matches_segments[:2]
            )
            if len(matches_segments) > 2:
                summary.append(color_msg("...", color="cyan", indentLevel=indent + 2, return_str=True))

    return summary

_stations_cached = {}

def get_cached_station(wh, sc, st, dt_info=None):
    """
    Returns a DT station object for the given wheel, sector, and station.

    :param wh: The wheel number.
    :type wh: int
    :param sc: The sector number.
    :type sc: int
    :param st: The station number.
    :type st: int
    :param dt_info: Optional DataFrame containing DT info. Default is None.
    :type dt_info: pandas.DataFrame, optional
    :return: The DT station object.
    :rtype: DT
    """
    key = (wh, sc, st)
    
    if key not in _stations_cached:
        try:
            _stations_cached[key] = Station(wheel=wh, sector=sc, station=st, dt_info=dt_info)
        except ValueError:
            # If the station cannot be created, we return None
            _stations_cached[key] = None
            return None
    else:
        # If the station is already cached, we can return it directly
        # This avoids unnecessary re-creation of the Station object
        if dt_info is not None:
            _stations_cached[key].set_cell_attrs(dt_info)

    return _stations_cached[key]

def cast_cmaps(kargs_list):
    if not isinstance(kargs_list, dict) or not all(isinstance(v, dict) for v in kargs_list.values()):
        return
    from matplotlib import colors
    from matplotlib.pyplot import get_cmap
    for kargs in kargs_list.values():
        if 'cmap' in kargs:
            cmap = kargs['cmap']
            if isinstance(cmap, colors.ListedColormap):
                pass # Nothing to do
            elif isinstance(cmap, str):
                cmap=get_cmap(cmap)
            elif isinstance(cmap, dict):
                cmap=get_cmap(cmap['name'], cmap.get('N'))
            elif isinstance(cmap, list):
                cmap=colors.ListedColormap(cmap)
            else:
                raise ValueError(f"Unsupported colormap format: {cmap}")
            cmap.set_under('None')
            kargs.update(cmap=cmap)
        if 'norm' in kargs:
            norm = kargs['norm']
            if isinstance(norm, dict):
                class_name = norm.pop('class', 'Normalize')
                kargs.update(norm=getattr(colors, class_name)(**norm))

def parse_plot_configs():
    """
    Parse DT plot configurations from RUN_CONFIG.
    
    :return: A tuple containing (mplhep_style, figure_configs, dt_cell_info, bounds_kwargs, cells_kwargs)
    :rtype: tuple
    """
    if not hasattr(RUN_CONFIG, "plot_configs"):
        raise ValueError("RUN_CONFIG does not contain 'plot_configs'.")

    plot_configs = deepcopy(RUN_CONFIG.plot_configs)

    mplhep_style = plot_configs.get("mplhep-style", None)
    figure_configs = plot_configs.get("figure-configs", {})
    artist = {}

    for artist_name, artist_configs in plot_configs.get("artists", {}).items():
        src = artist_configs.pop("src", None)
        if not src:
            raise ValueError(f"Artist '{artist_name}' does not have a 'src' defined in RUN_CONFIG.")
        artist_builder = get_callable_from_src(src)
        rep_info = artist_configs.pop("rep-info", {})
        cast_cmaps(artist_configs)
        artist[artist_name] = partial(artist_builder, **rep_info, **artist_configs)

    return {
        "mplhep_style": mplhep_style,
        "figure_configs": figure_configs,
        "artist": artist
    }


def parse_dt_plot_configs():
    """
    Parse DT plot configurations from RUN_CONFIG.
    
    :return: A tuple containing (mplhep_style, figure_configs, dt_cell_info, bounds_kwargs, cells_kwargs)
    :rtype: tuple
    """
    mplhep_style = None
    figure_configs = {}
    dt_cell_info = None
    bounds_kwargs = None
    cells_kwargs = None

    # use RUN_CONFIG to:
    # - Determine which event info to use as dt_info
    # - Get the style and configurations for the plots

    if hasattr(RUN_CONFIG, "dt_plots_configs"):
        mplhep_style = RUN_CONFIG.dt_plots_configs.get("mplhep-style", None)
        figure_configs = RUN_CONFIG.dt_plots_configs.get("figure-configs", {})
        dt_cell_info = RUN_CONFIG.dt_plots_configs.get("dt-cell-info", None)
        bounds_kwargs = deepcopy(RUN_CONFIG.dt_plots_configs.get("bounds-kwargs", None))
        cells_kwargs = deepcopy(RUN_CONFIG.dt_plots_configs.get("cells-kwargs", None))

        cmap_configs = deepcopy(RUN_CONFIG.dt_plots_configs.get("cmap-configs", None))
        if cmap_configs is not None:
            _cmap = cmap_configs.get("cmap", "viridis")
            if isinstance(_cmap, str):
                cmap = plt.get_cmap(_cmap).copy()
            else:
                cmap = plt.get_cmap(**_cmap).copy()

            cmap.set_under(cmap_configs.get("cmap_under", None))

            norm_module, norm_name = cmap_configs["norm"].pop("class").rsplit('.', 1)
            module = importlib.import_module(norm_module)
            norm = getattr(module, norm_name)(**cmap_configs["norm"])
            cells_kwargs = cells_kwargs or {}
            cells_kwargs.update({"cmap": cmap, "norm": norm})

    return {
        "mplhep_style": mplhep_style,
        "figure_configs": figure_configs,
        "dt_cell_info": dt_cell_info,
        "bounds_kwargs": bounds_kwargs,
        "cells_kwargs": cells_kwargs,
    }

def parse_filter_text_4gui(filter_text):
    filter_kwargs = {}
    if filter_text:
        try:
            for part in filter_text.split(";"):
                if not part:
                    continue
                key, value = part.split("=")
                filter_kwargs[key.strip()] = eval(value.strip())
        except:
            pass
    return filter_kwargs