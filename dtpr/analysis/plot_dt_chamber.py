import os
import gc
import importlib
import matplotlib.pyplot as plt
from pandas import DataFrame
from mplhep import style
from copy import deepcopy

from dtpr.base import Event, NTuple
from dtpr.utils.functions import color_msg, save_mpl_canvas
from mpldts.geometry import Station as DT
from mpldts.patches import DTStationPatch
from dtpr.utils.config import RUN_CONFIG

def embed_dt2axes(station, faceview, ax=None, bounds_kwargs=None, cells_kwargs=None):
    """
    Add a DT patch to the axes for the specified faceview of the station.

    :param station: The mpldts.geometry.Station object containing DT chamber data
    :type station: Station.
    :param faceview: The faceview to plot ('phi' or 'eta').
    :type faceview: str
    :param ax: The axes to plot on. Default is None, which creates a new figure.
    :type ax: matplotlib.axes._subplots.AxesSubplot
    :param bounds_kwargs: Additional keyword arguments for the bounds of the patch.
    :type bounds_kwargs: dict
    :param cells_kwargs: Additional keyword arguments for the cells of the patch.
    :type cells_kwargs: dict

    :return: The axes for the specified Station and faceview, and the resulting DTPatch instance.
    :return type:  tuple
    """
    if not ax:
        _, ax = plt.subplots(figsize=(8, 6))

    patch = DTStationPatch(
        station=station,
        axes=ax,
        local=True,
        faceview=faceview,
        bounds_kwargs=bounds_kwargs,
        cells_kwargs=cells_kwargs,
    )

    width, height, length = station.bounds
    x0, y0, z0 = station.local_center

    if faceview == "phi":
        ax.set_xlim(x0 - width/2 - 10, x0 + width/2 + 10)
        ax.set_ylim(z0 - height/2 - 10, z0 + height/2 + 10)
        ax.set_title(r"$\phi$ view")
    elif faceview == "eta":
        ax.set_xlim(y0 - length/2 - 10, y0 + length/2 + 10)
        ax.set_ylim(z0 - height/2 - 10, z0 + height/2 + 10)
        ax.set_title(r"$\eta$ view")

    ax.set_xlabel("x [cm]")
    ax.set_ylabel("z [cm]")

    return ax, patch

def make_dt_plot(ev: Event, wh, sc, st, name="test_dt_plot", path=".", save=False):
    """
    Create and display a DT plot for the specified wheel, sector, and station in the given event.

    :param ev: The event containing DT data.
    :type ev: Event
    :param wh: The wheel number.
    :type wh: int
    :param sc: The sector number.
    :type sc: int
    :param st: The station number.
    :type st: int
    :param name: The name of the plot. Default is "test_dt_plot".
    :type name: str
    :param path: The path to save the plot. Default is ".".
    :type path: str
    :param save: Whether to save the plot to disk. Default is False.
    :type save: bool
    """
    # use RUN_CONFIG to determine which evetn info to use as dt_info
    particle_type, cmap_var = RUN_CONFIG.dt_plots_configs["dt-cell-info"].values()
    dt_info = DataFrame([particle.__dict__ for particle in ev.filter_particles(particle_type, wh=wh, sc=sc, st=st)])

    if dt_info.empty:
        color_msg(f"No {particle_type} found for the chamber {wh}/{sc}/{st} in the event {ev.index}", "red")
        dt_info = None
    else:
        if any( not col_name in dt_info.columns for col_name in ["sl", "l", "w", cmap_var]):
            raise ValueError(f"Columns 'sl', 'l', 'w', and '{cmap_var}' must be present in {particle_type} data")

    _dt_chamber = DT(wheel=wh, sector=sc, station=st, dt_info=dt_info[["sl", "l", "w", cmap_var]] if dt_info is not None else dt_info)

    fig, axs = plt.subplots(1, 2, figsize=(15, 8), sharey=True)
    axs = axs.flat

    bounds_kwargs = deepcopy(RUN_CONFIG.dt_plots_configs["bounds-kwargs"])
    cells_kwargs = deepcopy(RUN_CONFIG.dt_plots_configs["cells-kwargs"])

    cmap_configs = deepcopy(RUN_CONFIG.dt_plots_configs["cmap-configs"])

    cmap = plt.get_cmap(cmap_configs["cmap"]).copy()
    cmap.set_under(cmap_configs["cmap_under"])

    norm_module, norm_name = cmap_configs["norm"].pop("class").rsplit('.', 1)
    module = importlib.import_module(norm_module)
    norm = getattr(module, norm_name)(**cmap_configs["norm"])

    cells_kwargs.update({"cmap": cmap, "norm": norm})

    axs[0], patch = embed_dt2axes(_dt_chamber, "phi", axs[0], bounds_kwargs, cells_kwargs)
    axs[1], _ = embed_dt2axes(_dt_chamber, "eta", axs[1], bounds_kwargs, cells_kwargs)

    fig.colorbar(patch.cells_collection, ax=axs[1], label=f"{cmap_var}")

    plt.suptitle(f"{_dt_chamber.name}", y=.9)

    plt.tight_layout()
    if save:
        save_mpl_canvas(fig, name, path, dpi=RUN_CONFIG.dt_plots_configs["figure-configs"]["figure.dpi"])
    else:
        plt.show()
    plt.close(fig)
    del fig  # Explicitly delete the figure object
    gc.collect()

def plot_dt_chamber(
        inpath: str,
        outfolder: str,
        tag: str,
        maxfiles: int,
        event_number: int,
        wheel: int,
        sector: int,
        station: int,
        save: bool,
    ):
    """
    Produce a single DT chamber plot based on DTNTuples.

    
    :param inpath: The input directory containing DTNTuples.
    :type inpath: str
    :param outfolder: The output directory where the plots will be saved.
    :type outfolder: str
    :param tag: A tag to append to the plot filenames.
    :type tag: str
    :param maxfiles: The maximum number of files to process.
    :type maxfiles: int
    :param event_number: The event number to plot.
    :type event_number: int
    :param wheel: The wheel number.
    :type wheel: int
    :param sector: The sector number.
    :type sector: int
    :param station: The station number.
    :type station: int
    :param save: Whether to save the plots to disk.
    :type save: bool
    """

    # Start of the analysis 
    color_msg(f"Running program to produce a DT plot based on DTNTuples", "green")

    # Create the Ntuple object
    ntuple = NTuple(
        inputFolder=inpath,
        maxfiles=maxfiles,
    )

    color_msg(f"Making plot...", color="purple", indentLevel=1)

    ev = ntuple.events[event_number]
    if not ev:
        color_msg(f"Event did not pass filter: {ev}", color="red")
        return
    with plt.style.context(getattr(style, RUN_CONFIG.dt_plots_configs["mplhep-style"])):
        plt.rcParams.update(RUN_CONFIG.dt_plots_configs["figure-configs"]) 
        make_dt_plot(ev, wh=wheel, sc=sector, st=station, name=f"dt_plot{tag}_ev{ev.index}", path=os.path.join(outfolder, "dt_plots"), save=save)

    color_msg(f"Done!", color="green")

if __name__ == "__main__":
    plot_dt_chamber(
        inpath = "../../test/ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root",
        # inpath="../ntuples/g4DTSimNtuple_muonTest_refactored.root",
        outfolder="./results",
        tag="test",
        maxfiles = -1,
        event_number = 23,
        wheel = -1,
        sector = 6,
        station = 2,
        save=True
    )