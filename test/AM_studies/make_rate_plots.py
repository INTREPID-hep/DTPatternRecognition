
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
                "file_name": folder + "/histograms/histograms_thr12_minbias.root",
                "histos_names": ["Rate_AllBX_MBX_AM", "Rate_goodBX_MBX_AM"],
                "legends": ["AM All-BX", "AM Good-BX"],
            }
        ],
        type="histo",
        titleY="DT Local Trigger Rate",
        maxY=None,
        output_name = "AM_rates",
        outfolder=folder + "/rate_plots", 
        logy=True,
        scaling=1
    )

if __name__ == "__main__":
    main()