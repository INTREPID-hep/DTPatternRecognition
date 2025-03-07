from dtpr.utils.functions import color_msg
from dtpr.utils.root_plot_functions import *

def main():
    # --- Files to be used --- #
    folder = "."

    color_msg("Plotting rates", color="blue")

    # ------------------------------ all bx plot ------------------------------ #
    thresholds = [6, 12, 14, 24]
    
    info_for_plots_allbx = [
        {
            "file_name": f"{folder}/histograms/histograms_thr6.root",
            "histos_names": "Rate_allBX_MBX_AM",
            "legends": f"AM",
        }
    ]
    info_for_plots_allbx.extend([
        { 
            "file_name": f"{folder}/histograms/histograms_thr{thr}.root",
            "histos_names": "Rate_allBX_MBX_FwShower",
            "legends": f"Showers thr{thr}",
        }
        for thr in thresholds
    ])

    make_plots(
        info_for_plots=info_for_plots_allbx,
        type="histo",
        titleY="DT Local Trigger Rate (Hz)",
        maxY=None,
        output_name = "showerrate_allbx",
        outfolder=folder + "/rate_plots_thrscan", 
        logy=True,
        scaling= (1/1565520) * 2760 * 11246,
        # repite_color_each=2,
        legend_pos=(0.70, 0.38, 0.9, 0.56),
    )

    # # # ------------------------------ good bx plot ------------------------------ #
    # info_for_plots_goodbx = [
    #     { 
    #         "file_name": f"{folder}/histograms/histograms_thr{thr}.root",
    #         "histos_names": ["Rate_goodBX_MBX_AM", "Rate_goodBX_MBX_EmuShower"],
    #         "legends": [f"AM thr{thr}", f"Showers thr{thr}"],
    #     }
    #     for thr in thresholds
    # ]

    # make_plots(
    #     info_for_plots=info_for_plots_goodbx,
    #     type="histo",
    #     titleY="DT Local Trigger Rate (Hz)",
    #     maxY=None,
    #     output_name = "Fw_rate_goodbx",
    #     outfolder=folder + "/rate_plots_thrscan", 
    #     logy=True,
    #     scaling= (1/10000) * 2760 * 11246,
    #     repite_color_each=2,
    # )

if __name__ == "__main__":
    main()