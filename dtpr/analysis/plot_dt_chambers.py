import os
import gc
from typing import List, Union
from dtpr.base import Event, NTuple
from dtpr.utils.functions import color_msg, save_mpl_canvas, parse_plot_configs
import matplotlib.pyplot as plt
from mplhep import style
import matplotlib

def make_plots(ev: Event, artist_builders, name="test_dt_plot", path=".", save=False):
    fig, axs = plt.subplots(2, 3, figsize=(14,8), layout="constrained")
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
    del fig  # Explicitly delete the figure object
    gc.collect()

def plot_dt_chambers(
        inpath: str,
        outfolder: str,
        tag: str,
        maxfiles: int,
        event_number: int,
        save: bool,
        wheel: int=None,
        sector: int=None,
        artist_names: Union[List[str], str]=None
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

    # Now execute the plotting function with the specified parameters
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
        inpath = "../../test/ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root",
        outfolder="./results",
        tag="test",
        maxfiles = -1,
        event_number = 11,
        save=False
    )