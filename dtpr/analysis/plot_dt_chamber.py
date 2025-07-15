import os
import gc
from dtpr.base import Event, NTuple
from dtpr.utils.functions import color_msg, save_mpl_canvas
from dtpr.utils.functions import parse_plot_configs
from mplhep import style
import matplotlib.pyplot as plt
from typing import Optional, List, Dict, Union

def make_dt_plot(
        ev: Event,
        wh: int,
        sc: int,
        st: int,
        artist_builders: Dict[str, callable],
        name: Optional[str] = "test_dt_plot",
        path: Optional[str] = ".",
        save: Optional[bool] = False
    ) -> None:
    fig, axs = plt.subplots(1, 2, figsize=(14, 8))
    axs = axs.flat

    patches = {}
    for ploter_name, ploter in artist_builders.items():
        patches[ploter_name] = ploter(ev, wheel=wh, sector=sc, station=st, ax_phi=axs[0], ax_eta=axs[1])

    patch_phi, patch_eta = patches.get("dt-station-local", (None, None))
    patch2cbar = patch_phi if patch_phi else patch_eta

    if patch_phi is not None:
        axs[0].autoscale()
        axs[0].set_xlabel("x [cm]")
        axs[0].set_title(r"$\phi$ view")
        patch2cbar = patch_phi
    else:
        axs[0].remove()
    if patch_eta is not None:
        axs[1].autoscale()
        axs[1].set_xlabel("x [cm]")
        axs[1].set_ylabel("z [cm]")
        axs[1].set_title(r"$\eta$ view")
        if patch2cbar is None:
            patch2cbar = patch_eta
    else:
        axs[1].remove()

    plt.suptitle(f"Wheel {wh}, Sector {sc}, Station {st}", y=.9)

    fig.colorbar(patch2cbar.cells_collection, ax=axs[1], label=f"{patch2cbar.vmap}")
    plt.tight_layout()

    if save:
        save_mpl_canvas(fig, name, path)
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
        artist_names: Union[List[str], str]
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

    mplhep_style, figure_configs, artist_builders = parse_plot_configs().values()

    if artist_names is None:
        artist_names = ["dt-station-local"]
    elif isinstance(artist_names, str):
        artist_names = [artist_names]

    if "dt-station-local" not in artist_names:
        artist_names.append("dt-station-local")

    artist_builders_filtered = {name: artist_builders.get(name, None) for name in artist_names}

    for name, builder in artist_builders_filtered.items():
        if builder is None:
            raise ValueError(f"Artist builder '{name}' not found in the configuration file.")

    # Now execute the plotting function with the specified parameters
    with plt.style.context(getattr(style, mplhep_style) if mplhep_style else "default"):
        plt.rcParams.update(figure_configs)
        make_dt_plot(
            ev,
            wheel,
            sector,
            station,
            artist_builders_filtered,
            name=f"dt_plot{tag}_ev{ev.index}",
            path=os.path.join(outfolder, "dt_plots"),
            save=save
        )

    color_msg(f"Done!", color="green")

if __name__ == "__main__":
    plot_dt_chamber(
        inpath = "../../test/ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root",
        outfolder="./results",
        tag="test",
        maxfiles = -1,
        event_number = 11,
        wheel = -2,
        sector = 10,
        station = 1,
        save=False
    )