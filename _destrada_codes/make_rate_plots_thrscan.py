from dtpr.utils.functions import color_msg
from src.utils.root_plot_functions import *

def main():
    # --- Files to be used --- #
    folder = "./thrscan_rate_results"

    color_msg("Plotting rates", color="blue")

    # ------------------------------ all bx plot ------------------------------ #
    thresholds = [12, 16, 20, 24, 28, 32]
    info_for_plots_allbx = [
        { 
            "file_name": f"{folder}/histograms/histograms_thr{thr}.root",
            "histos_names": "Rate_AllBX_MBX_EmuShower",
            "legends": f"thr{thr}",
        }
        for thr in thresholds
    ]

    make_plots(
        info_for_plots=info_for_plots_allbx,
        type="histo",
        titleY="DT Local Trigger Rate (Hz)",
        maxY=None,
        output_name = "Fw_rate_allbx",
        outfolder=folder + "/rate_plots_thrscan", 
        logy=True,
        scaling= (1/10000) * 2760 * 11246
    )

    # ------------------------------ good bx plot ------------------------------ #
    info_for_plots_goodbx = [
        { 
            "file_name": f"{folder}/histograms/histograms_thr{thr}.root",
            "histos_names": "Rate_goodBX_MBX_EmuShower",
            "legends": f"thr{thr}",
        }
        for thr in thresholds
    ]

    make_plots(
        info_for_plots=info_for_plots_goodbx,
        type="histo",
        titleY="DT Local Trigger Rate (Hz)",
        maxY=None,
        output_name = "Fw_rate_goodbx",
        outfolder=folder + "/rate_plots_thrscan", 
        logy=True,
        scaling= (1/10000) * 2760 * 11246
    )

if __name__ == "__main__":
    main()