"""" Plotting script """
from dtpr.utils.functions import color_msg
from dtpr.utils.root_plot_functions import *


def main():    
    # --- Files to be used --- #
    folder = "."
    color_msg("Plotting fwShower efficiency", color="blue")


    thresholds = [6, 8, 12, 16]
    info_for_plots = [
        { 
            "file_name": f"{folder}/histograms/histograms_thr_rsfix{thr}.root",
            "histos_names": "Fwshower_eff_MBX",
            "legends": f" Threshold - {thr}",
        }
        for thr in thresholds
    ]
    # info_for_plots = [
    #     { 
    #         "file_name": f"./histograms/histograms_nf_thr6.root",
    #         "histos_names": "Fwshower_eff_MBX",
    #         "legends": f"(TP + TN) / (TP + TN + FP + FN)",
    #     },
    # ]

    make_plots(
        info_for_plots=info_for_plots,
        output_name = "eff_fwshower_rsfix_poster",
        outfolder=folder+"/eff_plots",
        legend_pos=(0.7, 0.48, 0.74, 0.6),
        titleY="Shower Trigger Efficiency #scale[0.7]{#left[#frac{TP + TN}{TP + TN + FP + FN}#right]}",
        # aditional_notes=[("(TP + TN) / (TP + TN + FP + FN)", (.44, .38, .5, .47), 0.03)]
    )

    color_msg("Done!", color="green")


if __name__ == "__main__":
    main()
