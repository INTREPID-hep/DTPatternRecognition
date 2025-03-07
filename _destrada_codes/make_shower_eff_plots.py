"""" Plotting script """
from dtpr.utils.functions import color_msg
from src.utils.root_plot_functions import *


def main():    
    # --- Files to be used --- #
    folder = "./results_5"
    color_msg("Plotting fwShower efficiency", color="blue")


    # thresholds = [6, 9, 12, 15]
    # info_for_plots = [
    #     { 
    #         "file_name": f"{folder}/histograms/histograms_thr{thr}.root",
    #         "histos_names": "Fwshower_eff_2_MBX",
    #         "legends": f" thr - {thr}",
    #     }
    #     for thr in thresholds
    # ]
    info_for_plots = [
        { 
            "file_name": f"./results_5/histograms/histograms_thr6_ac.root",
            "histos_names": "Fwshower_eff_MBX",
            "legends": f"(TP + TN) / (TP + TN + FP + FN)",
        },
        { 
            "file_name": f"./results_5/histograms/histograms_thr6_ac.root",
            "histos_names": "Fwshower_eff_2_MBX",
            "legends": f"TP / (TP + FN)",
        }
    ]


    make_plots(
        info_for_plots=info_for_plots,
        output_name = "eff_fwshower-two_methods_ac",
        outfolder=folder+"/eff_plots",
    )

    # make_plots(
    #     info_for_plots=[
    #         {
    #             "file_name": folder + "/histograms/histograms_thr12.root", 
    #             "histos_names": "Fwshower_eff_MBX",
    #             "legends": "#frac{TP showers}{Real showers}"
    #         }
    #     ],
    #     type="div",
    #     output_name = "div_eff_fwshower",
    #     outfolder=folder+"/eff_plots",
    # )


    color_msg("Done!", color="green")


if __name__ == "__main__":
    main()
