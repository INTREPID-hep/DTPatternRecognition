import os
import gc
import importlib
import matplotlib.pyplot as plt
from pandas import DataFrame
from mplhep import style
from copy import deepcopy

from dtpr.base import Event
from dtpr.utils.functions import color_msg, init_ntuple_from_config, save_mpl_canvas
from mpldts.geometry.station import Station as DT
from mpldts.patches.dt_patch import DTPatch
from dtpr.utils.config import RUN_CONFIG

def embed_dt2axes(station, faceview, ax=None, bounds_kwargs=None, cells_kwargs=None):
    """
    Create the axes for the specified faceview of the station.

    Parameters:
    station (Station): The station containing DT data.
    faceview (str): The faceview to plot ('phi' or 'eta').
    ax (matplotlib.axes._subplots.AxesSubplot): The axes to plot on. Default is None.

    Returns:
    matplotlib.axes._subplots.AxesSubplot: The axes for the specified faceview.
    """
    if not ax:
        _, ax = plt.subplots(figsize=(8, 6))

    patch = DTPatch(
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

def _make_dt_plot(
        ev: Event,
        wh,
        sc,
        st,
        name="test_dt_plot",
        path="./results",
        save=False):
    """
    Create and display a DT plot for the specified wheel, sector, and station in the given event.

    Parameters:
    ev (Event): The event containing DT data.
    wh (int): The wheel number.
    sc (int): The sector number.
    st (int): The station number.
    name (str): The name of the plot. Default is "test_dt_plot".
    path (str): The path to save the plot. Default is "./results".
    save (bool): Whether to save the plot to disk. Default is False.
    """
    # use RUN_CONFIG to determine which evetn info to use as dt_info
    particle_type, cmap_var = RUN_CONFIG.dt_plots_configs["dt-cell-info"].values()
    dt_info = DataFrame([particle.__dict__ for particle in ev.filter_particles(particle_type, wh=wh, sc=sc, st=st)])

    print(dt_info)
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
    Produce a single DT chamber plot.

    Parameters:
    inpath (str): The input directory containing DTNTuples.
    outfolder (str): The output directory where the plots will be saved.
    tag (str): A tag to append to the plot filenames.
    maxfiles (int): The maximum number of files to process.
    event_number (int): The event number to plot.
    wheel (int): The wheel number.
    sector (int): The sector number.
    station (int): The station number.
    save (bool): Whether to save the plots to disk.
    """

    # Start of the analysis 
    color_msg(f"Running program to produce a DT plot based on DTNTuples", "green")

    # Create the Ntuple object
    ntuple = init_ntuple_from_config(
        inputFolder=inpath,
        maxfiles=maxfiles,
        config=RUN_CONFIG
    )

    color_msg(f"Making plot...", color="purple", indentLevel=1)

    ev = ntuple.events[event_number]
    if not ev:
        color_msg(f"Event not pass filter: {ev}", color="red")
        return
    with plt.style.context(getattr(style, RUN_CONFIG.dt_plots_configs["mplhep-style"])):
        plt.rcParams.update(RUN_CONFIG.dt_plots_configs["figure-configs"]) 
        _make_dt_plot(ev, wh=wheel, sc=sector, st=station, name=f"dt_plot{tag}_ev{ev.index}", path=os.path.join(outfolder, "dt_plots"), save=save)

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