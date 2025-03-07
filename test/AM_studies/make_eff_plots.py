"""" Plotting script """
from dtpr.utils.functions import color_msg
from dtpr.utils.root_plot_functions import *


def main():    
    folder = "."

    color_msg("Plotting segment matching efficiency", color="blue")

    make_plots( 
        info_for_plots=[
            {
                "file_name": folder + "/histograms/histograms_nf_thr6.root", 
                "histos_names": "Eff_MBX_AM",
                "legends": "AM"
            },
            {
                "file_name": folder + "/histograms/histograms_f_thr6.root", 
                "histos_names": "Eff_MBX_AM",
                "legends": "AM - shower filter"
            }
        ],
        output_name = "eff_Am",
        outfolder=folder+"/eff_plots",
    )

    color_msg("Done!", color="green")


if __name__ == "__main__":
    main()
