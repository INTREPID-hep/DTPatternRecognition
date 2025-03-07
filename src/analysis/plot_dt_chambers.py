import os
import gc
import importlib
import matplotlib.pyplot as plt
from pandas import DataFrame
from mplhep import style
from copy import deepcopy

from dtpr.base import Event
from dtpr.utils.functions import color_msg
from dtpr.geometry.station import Station as DT
from dtpr.patches.dt_patch import DTPatch
from src.utils.functions import save_mpl_canvas
from src.utils.config import RUN_CONFIG

def embed_dtwheel2axes(ev, wheel, ax=None, bounds_kwargs=None, cells_kwargs=None):
    """
    Create the axes for a specific wheel for the given event.

    Parameters:
    ev (Event): The event containing DT data.
    wheel (int): The wheel number (-2 to 2).
    ax (matplotlib.axes._subplots.AxesSubplot): The axes to plot on. Default is None.

    Returns:
    matplotlib.axes._subplots.AxesSubplot: The axes for the specified wheel.
    """
    if not ax:
        _, ax = plt.subplots(figsize=(8, 6))

    # use RUN_CONFIG to determine which evetn info to use as dt_info
    particle_type, cmap_var = RUN_CONFIG.dt_plots_configs["dt-cell-info"].values()
    infoDts = DataFrame([particle.to_dict() for particle in ev.filter_particles(particle_type, wh=wheel)])

    if infoDts.empty:
        ax.text(
            0.5,
            0.5, 
            f"No digis in wheel {wheel}",
            horizontalalignment='center',
            verticalalignment='center',
            transform=ax.transAxes
        )
        return ax
    else:
        if any( not col_name in infoDts.columns for col_name in ["sl", "l", "w", cmap_var]):
            raise ValueError(f"Columns 'sl', 'l', 'w', and '{cmap_var}' must be present in {particle_type} data")

        infoDts["time"] = infoDts[cmap_var]
        infoDts = infoDts.groupby(["wh", "sc", "st"])

        # add the circle representing the CMS Wheel
        circle = plt.Circle((0, 0), 800, edgecolor='black', facecolor="gray", alpha=0.1, linewidth=0.5)
        ax.add_patch(circle)
        ax.set_aspect("equal")
        ax.set_xlim(-800, 800)
        ax.set_ylim(-800, 800)
        ax.set_xlabel("x [cm]")
        ax.set_ylabel("y [cm]")
        ax.set_title(f"wheel {wheel}")

        for (wh, sc, st), dt_info in infoDts:
            _dt_chamber = DT(wheel=wh, sector=sc, station=st, dt_info=dt_info[["sl", "l", "w", "time"]])

            DTPatch(
                station=_dt_chamber,
                axes=ax,
                local=False,
                faceview="phi",
                bounds_kwargs=bounds_kwargs,
                cells_kwargs=cells_kwargs,
            )

    return ax

def _make_dt_plots(
        ev: Event,
        name="test_dt_plot",
        path="./results",
        save=False):
    """
    Create the global DT plots for the given event.

    Parameters:
    ev (Event): The event containing DT data.
    name (str): The name of the plot file.
    path (str): The directory where the plot file will be saved. Default is ".results".
    save (bool): Whether to save the plot to disk. Default is False.
    """
    fig, axs = plt.subplots(2, 3, figsize=(50,30), layout="constrained")
    axs = axs.flat
    fig.suptitle(f"Event #{ev.iev}")

    bounds_kwargs = deepcopy(RUN_CONFIG.dt_plots_configs["bounds-kwargs"])
    cells_kwargs = deepcopy	(RUN_CONFIG.dt_plots_configs["cells-kwargs"])

    cmap_configs = deepcopy(RUN_CONFIG.dt_plots_configs["cmap-configs"])

    cmap = plt.get_cmap(cmap_configs["cmap"]).copy()
    cmap.set_under(cmap_configs["cmap_under"])

    norm_module, norm_name = cmap_configs["norm"].pop("class").rsplit('.', 1)
    module = importlib.import_module(norm_module)
    norm = getattr(module, norm_name)(**cmap_configs["norm"])

    cells_kwargs.update({"cmap": cmap, "norm": norm})

    for iaxs in range(-2, 3):
        _ = embed_dtwheel2axes(ev, iaxs, axs[iaxs + 2], bounds_kwargs=bounds_kwargs, cells_kwargs=cells_kwargs)

    axs[-1].remove()

    if save:
        save_mpl_canvas(fig, name, path, dpi=RUN_CONFIG.dt_plots_configs["figure-configs"]["figure.dpi"])
    else:
        plt.show()
    plt.close(fig)
    del fig  # Explicitly delete the figure object
    gc.collect()

def plot_dt_chambers(
        inpath: str,
        outfolder: str,
        tag: str,
        maxfiles: int,
        event_number: int,
        save: bool,
    ):
    """
    Generate DT plots from DTNTuples and save them to the specified output folder.

    Parameters:
    inpath (str): The input directory containing DTNTuples.
    outfolder (str): The output directory where the plots will be saved.
    filter_type (str): The type of filter to apply to events.
    tag (str): A tag to append to the plot filenames.
    maxfiles (int): The maximum number of files to process.
    event_number (int): The event number to plot.
    save (bool): Whether to save the plots to disk.
    """

    # Start of the analysis 
    color_msg(f"Running program to produce DT plots based on DTNTuples", "green")

    # Create the Ntuple object
    _ntuple_module, _ntuple_class = RUN_CONFIG.ntuple_source.rsplit(".", 1)
    _ntuple_module = importlib.import_module(_ntuple_module)
    _NTUPLE =  getattr(_ntuple_module, _ntuple_class)

    ntuple = _NTUPLE(
        inputFolder=inpath,
        maxfiles=maxfiles,
    )

    color_msg(f"Making plots...", color="purple", indentLevel=1)

    ev = ntuple.events[event_number]
    if not ev:
        color_msg(f"Event not pass filter: {ev}", color="red")
        return
    with plt.style.context(getattr(style,RUN_CONFIG.dt_plots_configs["mplhep-style"])):
        plt.rcParams.update(RUN_CONFIG.dt_plots_configs["figure-configs"]) 
        _make_dt_plots(ev, name=f"dt_plot{tag}_ev{ev.iev}", path=os.path.join(outfolder, "dt_plots"), save=save)

    color_msg(f"Done!", color="green")

if __name__ == "__main__":
    plot_dt_chambers(
        inpath = "../ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_Simulation_99.root",
        outfolder="./results",
        tag="test",
        maxfiles = -1,
        event_number = 0,
        save=False
    )