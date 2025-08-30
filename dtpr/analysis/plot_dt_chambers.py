import os
import gc
from typing import List, Union, Dict, Callable, Optional
from dtpr.base import Event, NTuple
from dtpr.utils.functions import color_msg, save_mpl_canvas, parse_plot_configs
import matplotlib.pyplot as plt
from mplhep import style
import matplotlib

def make_plots(
    ev: Event,
    artist_builders: Dict[str, Callable],
    name: Optional[str] = "test_dt_plot",
    path: Optional[str] = ".",
    save: Optional[bool] = False
) -> None:
    """
    Create and display or save DT plots for a given event.

    :param ev: The event to plot.
    :type ev: Event
    :param artist_builders: Dictionary of artist builder functions for plotting.
    :type artist_builders: Dict[str, Callable]
    :param name: Name for the saved plot file.
    :type name: Optional[str]
    :param path: Directory to save the plot.
    :type path: Optional[str]
    :param save: If True, save the plot to disk; otherwise, display it.
    :type save: Optional[bool]
    :return: None
    :rtype: None
    """
    fig, axs = plt.subplots(2, 3, figsize=(14, 8), layout="constrained")
    axs = axs.flat
    fig.suptitle(f"Event #{ev.index}")

    def setup_axs(ax, wh):
        ax.autoscale()
        ax.set_xlabel("x [cm]")
        ax.set_ylabel("y [cm]")
        ax.set_title(f"Wheel {wh}")

    patches = {}
    for iaxs in range(-2, 3):
        patches[iaxs] = {}
        for ploter_name, ploter in artist_builders.items():
            patches[iaxs][ploter_name] = ploter(ev=ev, wheel=iaxs, ax=axs[iaxs + 2])
        setup_axs(axs[iaxs + 2], wh=iaxs)

    list(patches[-2]["dt-am-tps-global"].values())[0].segments_collection.remove()

    axs[-1].remove()
    if save:
        save_mpl_canvas(fig, name, path)
    else:
        plt.show()
    plt.close(fig)
    del fig
    gc.collect()

def plot_dt_chambers(
    inpath: str,
    outfolder: str,
    tag: str,
    maxfiles: int,
    event_number: int,
    save: bool,
    wheel: Optional[int] = None,
    sector: Optional[int] = None,
    artist_names: Optional[Union[List[str], str]] = None
) -> None:
    """
    Generate and save or display DT plots from DTNTuples for a specific event.

    :param inpath: Input directory or file containing DTNTuples.
    :type inpath: str
    :param outfolder: Output directory for saving plots.
    :type outfolder: str
    :param tag: Tag to append to plot filenames.
    :type tag: str
    :param maxfiles: Maximum number of files to process.
    :type maxfiles: int
    :param event_number: Index of the event to plot.
    :type event_number: int
    :param save: If True, save plots to disk; otherwise, display them.
    :type save: bool
    :param wheel: Wheel number to plot (optional).
    :type wheel: Optional[int]
    :param sector: Sector number to plot (optional).
    :type sector: Optional[int]
    :param artist_names: List of artist names or a single artist name to use for plotting (optional).
    :type artist_names: Optional[Union[List[str], str]]
    :return: None
    :rtype: None
    """
    color_msg(f"Running program to produce DT plots based on DTNTuples", "green")

    ntuple = NTuple(
        inputFolder=inpath,
        maxfiles=maxfiles,
    )

    color_msg(f"Making plots...", color="purple", indentLevel=1)

    ev = ntuple.events[event_number]
    if not ev:
        color_msg(f"Event did not pass filter: {ev}", color="red")
        return

    mplhep_style, figure_configs, artist_builders = parse_plot_configs().values()

    if artist_names is None:
        artist_names = ["dt-station-global", "cms-shadow-global"]

    if isinstance(artist_names, str):
        artist_names = [artist_names]

    if "dt-station-global" not in artist_names:
        artist_names.append("dt-station-global")
    if "cms-shadow-global" not in artist_names:
        artist_names.append("cms-shadow-global")

    artist_builders_filtered = {name: artist_builders.get(name, None) for name in artist_names}

    for name, builder in artist_builders_filtered.items():
        if builder is None:
            raise ValueError(f"Artist builder '{name}' not found in the configuration file.")

    with plt.style.context(getattr(style, mplhep_style) if mplhep_style else "default"):
        plt.rcParams.update(figure_configs)
        make_plots(
            ev,
            artist_builders_filtered,
            name=f"dt_plots{tag}_ev{ev.index}",
            path=os.path.join(outfolder, "dt_plots"),
            save=save,
        )

    color_msg(f"Done!", color="green")

if __name__ == "__main__":
    plot_dt_chambers(
        inpath="../../test/ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root",
        outfolder="./results",
        tag="test",
        maxfiles=-1,
        event_number=11,
        save=False
    )