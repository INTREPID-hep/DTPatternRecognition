
from dtpr.utils.functions import color_msg
from dtpr.utils.root_plot_functions import *

def main():
    # --- Files to be used --- #
    folder = "."

    color_msg("Plotting rates", color="blue")

    # ------------------------------ all bx plots ------------------------------ #
    make_plots(
        info_for_plots=[
            { 
                "file_name": folder + "/histograms/histograms_rate_thr6.root",
                "histos_names": ["Rate_allBX_MBX_AM", "Rate_goodBX_MBX_AM"],
                "legends": ["AM All-BX", "AM Good-BX"],
            }
        ],
        type="histo",
        titleY="DT Local Trigger Rate (Hz)",
        maxY=None,
        output_name = "AM_rates",
        outfolder=folder + "/plots", 
        logy=True,
        scaling=(1/1600) * 2760 * 11246
    )

if __name__ == "__main__":
    main()