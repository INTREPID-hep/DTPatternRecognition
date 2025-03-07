"""" Plotting script """
from dtpr.utils.functions import color_msg
from src.utils.root_plot_functions import *


def main():    
    # --- Files to be used --- #
    folder = "./results_141124"

    color_msg("Plotting segment matching efficiency", color="blue")

    info_for_plots = [
        {
            "file_name": folder + "/histograms/histograms_AM_withShowers_thr12.root",
            "histos_names": "Eff_MBX_AM",
            "legends": "AM",
        },
        {
            "file_name": folder + "/histograms/histograms_AM_vetoShower_thr12.root",
            "histos_names": "Eff_MBX_AM",
            "legends": "AM (with showers veto)",
        },
        {
            "file_name": folder + "/histograms/histograms_AM_plus_hitsc_thr12.root",
            "histos_names": "Eff_MBX_AM",
            "legends": "AM (tagging showers)",
        },
    ]

    make_plots(
        info_for_plots=info_for_plots,
        output_name = "eff_segment_AM",
        outfolder=folder+"/eff_plots",
    )

    color_msg("Plotting emuShower efficiency", color="blue")

    make_plots( 
        info_for_plots=[
            {
                "file_name": folder + "/histograms/histograms_emuShowers.root", 
                "histos_names": "Seg_m_emushower_tprgm_MBX",
                "legends": "#frac{showered genmuon's segments that match with any shower}{showered genmuon's segments}"
            }
        ],
        output_name = "eff_seg_emushower_tprgm",
        outfolder=folder+"/eff_plots",
    )

    make_plots(
        info_for_plots=[
            {
                "file_name": folder + "/histograms/histograms_emuShowers.root", 
                "histos_names": "Seg_m_emushower_fprgm_MBX",
                "legends": "#frac{showered genmuon's segments that match with any shower}{showered genmuon's segments}"
            }
        ],
        output_name = "eff_seg_emushower_fprgm",
        outfolder=folder+"/eff_plots",
    )

    color_msg("Plotting fwShower efficiency", color="blue")

    make_plots(
        info_for_plots=[
            {
                "file_name": folder + "/histograms/histograms_fwShowers.root", 
                "histos_names": "Seg_m_fwshower_tprgm_MBX",
                "legends": "#frac{showered genmuon's segments that match with any fwshower}{showered genmuon's segments}"
            }
        ],
        output_name = "eff_seg_fwshower_tprgm",
        outfolder=folder+"/eff_plots",
    )

    make_plots(
        info_for_plots=[
            {
                "file_name": folder + "/histograms/histograms_fwShowers.root", 
                "histos_names": "Seg_m_fwshower_fprgm_MBX",
                "legends": "#frac{showered genmuon's segments that match with any fwshower}{showered genmuon's segments}"
            }
        ],
        output_name = "eff_seg_fwshower_fprgm",
        outfolder=folder+"/eff_plots",
    )

    color_msg("Done!", color="green")


if __name__ == "__main__":
    main()
