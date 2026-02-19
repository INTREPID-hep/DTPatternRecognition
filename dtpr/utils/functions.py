"""Miscelaneous"""

from functools import partial
import os
import math
import matplotlib.pyplot as plt
from copy import deepcopy
from importlib import import_module
import numpy as np
from mpldts.geometry import Station
from typing import Any, Callable, Dict, List, Optional, Set, Tuple


def color_msg(
    msg: str,
    color: Optional[str] = "none",
    indentLevel: Optional[int] = -1,
    return_str: Optional[bool] = False,
    bold: Optional[bool] = False,
    underline: Optional[bool] = False,
    bkg_color: Optional[str] = "none",
) -> Optional[str]:
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
    :rtype: Optional[str]
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
        return None


def warning_handler(
    message: str,
    category: type,
    filename: str,
    lineno: int,
    file: Optional[Any] = None,
    line: Optional[str] = None,
) -> None:
    """
    Handles warnings by printing them with color formatting.

    :param message: The warning message.
    :type message: str
    :param category: The category of the warning.
    :type category: type
    :param filename: The name of the file where the warning occurred.
    :type filename: str
    :param lineno: The line number where the warning occurred.
    :type lineno: int
    :param file: The file object. Default is None.
    :type file: Any, optional
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


def error_handler(exc_type: type, exc_value: Exception, exc_traceback: Any) -> None:
    """
    Handles errors by printing them with color formatting.

    :param exc_type: The type of the exception.
    :type exc_type: type
    :param exc_value: The exception instance.
    :type exc_value: Exception
    :param exc_traceback: The traceback object.
    :type exc_traceback: Any
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
                color_msg(f"{exc_value}", color="yellow", return_str=True, indentLevel=-1),
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


# Cache for callable functions to avoid repeated imports
_CALLABLE_CACHE = {}

def get_callable_from_src(src_str: str) -> Callable:
    """
    Returns the callable object from the given source string.
    Uses a cache to avoid repeated imports of the same callable.

    :param src_str: The source string containing the callable.
    :type src_str: str
    :return: The callable object.
    :rtype: Callable
    """
    # Check cache first
    if src_str in _CALLABLE_CACHE:
        return _CALLABLE_CACHE[src_str]
    
    callable = None
    try:
        _module_name, _callable_name = src_str.rsplit(".", 1)
        _module = import_module(_module_name)
        callable = getattr(_module, _callable_name)
        # Cache the callable
        _CALLABLE_CACHE[src_str] = callable
    except AttributeError as e:
        raise AttributeError(f"{_callable_name} callable not found: {e}")
    except ImportError as e:
        raise ImportError(f"Error importing {src_str}: {e}")

    return callable


def create_outfolder(outname: str) -> None:
    """
    Creates an output directory if it does not exist.

    :param outname: The path of the output directory.
    :type outname: str
    """
    if not (os.path.exists(outname)):
        os.system("mkdir -p %s" % outname)


def save_mpl_canvas(fig: plt.Figure, name: str, path: str = "./results", dpi: int = 500) -> None:
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
    create_outfolder(path)
    fig.savefig(path + "/" + name + ".svg", dpi=dpi)
    return


def append_to_matched_list(obj: Any, matched_list_name: str, item: Any) -> None:
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


def get_unique_locs(particles: List[Any], loc_ids: List[str] = ["wh", "sc", "st"]) -> Set[Tuple]:
    """
    Returns the unique locations of the specified particle types.

    :param particles: The list of particle objects.
    :type particles: List[Any]
    :param loc_ids: The location IDs. Default is ["wh", "sc", "st"].
    :type loc_ids: List[str], optional
    :return: The unique locations of the specified particle types in tuple format.
    :rtype: Set[Tuple]
    """
    locs = []

    if particles:
        for particle in particles:
            try:
                locs.append(tuple([getattr(particle, loc_id) for loc_id in loc_ids]))
            except AttributeError as er:
                raise ValueError(f"Location Id attribute not found in particle object: {er}")

    return set(locs)


def format_event_attribute_str(key: str, value: Any, indent: int) -> str:
    """
    Format an event attribute as a colored string.

    :param key: The attribute key
    :type key: str
    :param value: The attribute value
    :type value: Any
    :param indent: The indentation level
    :type indent: int
    :return: The formatted string
    :rtype: str
    """
    return color_msg(
        f"{key.capitalize()}:", color="green", indentLevel=indent, return_str=True
    ) + color_msg(f"{value}", color="none", indentLevel=-1, return_str=True)


def format_event_particles_str(ptype: str, particles: List[Any], indent: int) -> List[str]:
    """
    Format a list of particle objects as colored strings.

    :param ptype: The type of particles
    :type ptype: str
    :param particles: The list of particles
    :type particles: List[Any]
    :param indent: The indentation level
    :type indent: int
    :return: List of formatted strings
    :rtype: List[str]
    """
    summary = [
        color_msg(f"{ptype.capitalize()}", color="green", indentLevel=indent, return_str=True),
        color_msg(
            f"Number of {ptype}: {len(particles)}",
            color="purple",
            indentLevel=indent + 1,
            return_str=True,
        ),
    ]

    if ptype == "genmuons":
        for gm in particles:
            summary.append(
                gm.__str__(
                    indentLevel=indent + 1,
                    color="cyan",
                    exclude=["matched_tps", "matched_segments"],
                )
                + "\n"
                + color_msg(
                    f"Matched offline - segments: {len(gm.matched_segments)}",
                    color="none",
                    indentLevel=indent + 2,
                    return_str=True,
                )
                + "\n"
                + color_msg(
                    f"Matched AM TPs: {len(gm.matched_tps)}",
                    color="none",
                    indentLevel=indent + 2,
                    return_str=True,
                )
            )

    elif ptype == "segments":
        matches_segments = [seg for seg in particles if seg.matched_tps]
        if matches_segments:
            summary.append(
                color_msg(
                    "Segs which match an AM-TP:",
                    color="cyan",
                    indentLevel=indent + 1,
                    return_str=True,
                )
            )
            summary.extend(
                seg.__str__(
                    indentLevel=indent + 2, color="cyan", include=["wh", "sc", "st", "phi", "eta"]
                )
                for seg in matches_segments[:2]
            )
            if len(matches_segments) > 2:
                summary.append(
                    color_msg("...", color="cyan", indentLevel=indent + 2, return_str=True)
                )

    return summary


def cast_cmaps(kargs_list: Dict[str, Dict[str, Any]]) -> None:
    """
    Convert colormap specifications to matplotlib colormap objects.

    :param kargs_list: Dictionary of keyword arguments dictionaries
    :type kargs_list: Dict[str, Dict[str, Any]]
    """
    if not isinstance(kargs_list, dict) or not all(
        isinstance(v, dict) for v in kargs_list.values()
    ):
        return
    from matplotlib import colors
    from matplotlib.pyplot import get_cmap

    for kargs in kargs_list.values():
        if "cmap" in kargs:
            cmap = kargs["cmap"]
            if isinstance(cmap, colors.ListedColormap):
                pass  # Nothing to do
            elif isinstance(cmap, str):
                cmap = get_cmap(cmap)
            elif isinstance(cmap, dict):
                cmap = get_cmap(cmap["name"], cmap.get("N"))
            elif isinstance(cmap, list):
                cmap = colors.ListedColormap(cmap)
            else:
                raise ValueError(f"Unsupported colormap format: {cmap}")
            cmap.set_under("None")
            kargs.update(cmap=cmap)
        if "norm" in kargs:
            norm = kargs["norm"]
            if isinstance(norm, dict):
                class_name = norm.pop("class", "Normalize")
                kargs.update(norm=getattr(colors, class_name)(**norm))


def parse_plot_configs() -> Dict[str, Any]:
    """
    Parse DT plot configurations from RUN_CONFIG.

    :return: A dictionary containing plot configuration elements
    :rtype: Dict[str, Any]
    """
    from ..base.config import RUN_CONFIG

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

    return {"mplhep_style": mplhep_style, "figure_configs": figure_configs, "artist": artist}


def parse_filter_text_4gui(filter_text: Optional[str]) -> Dict[str, Any]:
    """
    Parse filter text into a dictionary of filter arguments.

    :param filter_text: The filter text to parse
    :type filter_text: Optional[str]
    :return: Dictionary of filter arguments
    :rtype: Dict[str, Any]
    """
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


def deltaPhi(phi1: float, phi2: float) -> float:
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


def deltaEta(eta1: float, eta2: float) -> float:
    """
    Calculates the difference in eta between two pseudorapidity values.

    :param eta1: The first eta value.
    :type eta1: float
    :param eta2: The second eta value.
    :type eta2: float
    :return: The difference in eta.
    :rtype: float
    """
    return abs(eta1 - eta2)


def deltaR(p1: Any, p2: Any) -> float:
    """
    Calculates the delta R between two particles. Particles must have attributes eta and phi.

    :param p1: The first particle with attributes eta and phi.
    :type p1: Any
    :param p2: The second particle with attributes eta and phi.
    :type p2: Any
    :return: The delta R value.
    :rtype: float
    """
    dEta = deltaEta(p1.eta, p2.eta)
    dPhi = deltaPhi(p1.phi, p2.phi)
    return math.sqrt(dEta * dEta + dPhi * dPhi)
