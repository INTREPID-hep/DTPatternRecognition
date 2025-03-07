
from dtpr.utils.functions import color_msg
from src.utils.root_plot_functions import *

def main():
    # --- Files to be used --- #
    folder = "./thrscan_rate_results"

    color_msg("Plotting rates", color="blue")

    # ------------------------------ all bx plots ------------------------------ #
    make_plots(
        info_for_plots=[
            { 
                "file_name": folder + "/histograms/histograms_thr12.root",
                "histos_names": ["Rate_AllBX_MBX_AM", "Rate_AllBX_MBX_EmuShower", "Rate_AllBX_MBX_AM_EmuShower"],
                "legends": ["AM", "Showers", "AM + Showers"],
            }
        ],
        type="histo",
        titleY="DT Local Trigger Rate",
        maxY=None,
        output_name = "AM_Emu_rate",
        outfolder=folder + "/rate_plots_allbx", 
        logy=True,
        scaling=1
    )

    make_plots(
        info_for_plots=[
            { 
                "file_name": folder + "/histograms/histograms_thr12.root",
                "histos_names": ["Rate_AllBX_MBX_AM", "Rate_AllBX_MBX_FwShower", "Rate_AllBX_MBX_AM_FwShower"],
                "legends": ["AM", "Showers", "AM + Showers"],
            }
        ],
        type="histo",
        titleY="DT Local Trigger Rate",
        maxY=None,
        output_name = "AM_Fw_rate",
        outfolder=folder + "/rate_plots_allbx", 
        logy=True,
        scaling=1
    )

    # ------------------------------ good bx plots ------------------------------ #
    make_plots(
        info_for_plots=[
            { 
                "file_name": folder + "/histograms/histograms_thr12.root",
                "histos_names": ["Rate_goodBX_MBX_AM", "Rate_goodBX_MBX_EmuShower", "Rate_goodBX_MBX_AM_EmuShower"],
                "legends": ["AM", "Showers", "AM + Showers"],
            }
        ],
        type="histo",
        titleY="DT Local Trigger Rate",
        maxY=None,
        output_name = "AM_Emu_rate",
        outfolder=folder + "/rate_plots_goodbx", 
        logy=True,
        scaling=1
    )

    make_plots(
        info_for_plots=[
            { 
                "file_name": folder + "/histograms/histograms_thr12.root",
                "histos_names": ["Rate_goodBX_MBX_AM", "Rate_goodBX_MBX_FwShower", "Rate_goodBX_MBX_AM_FwShower"],
                "legends": ["AM", "Showers", "AM + Showers"],
            }
        ],
        type="histo",
        titleY="DT Local Trigger Rate",
        maxY=None,
        output_name = "AM_Fw_rate",
        outfolder=folder + "/rate_plots_goodbx", 
        logy=True,
        scaling=1
    )

if __name__ == "__main__":
    main()