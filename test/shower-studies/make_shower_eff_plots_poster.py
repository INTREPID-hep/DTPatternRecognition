"""" Plotting script """
from dtpr.utils.functions import color_msg
from dtpr.utils.root_plot_functions import *


def make_plots( 
        info_for_plots,
        output_name,
        outfolder=".",
        type="eff",
        titleY="DT Local Trigger Efficiency",
        titleX="Wheel",
        maxY=1.05,
        logy=False,
        logx=False,
        scaling=1,
        aditional_notes=[],
        legend_pos=(0.43, 0.48, 0.5, 0.56),
        repite_color_each=1
    ):

    graphs = []

    for info in info_for_plots:
        print(info_for_plots)
        file_name = info["file_name"]
        histo_names = info["histos_names"]
        legends = info["legends"]

        if not isinstance(histo_names, list):
            histo_names = [histo_names]

        if not isinstance(legends, list):
            legends = [legends]

        for idx in range(len(histo_names)):
            histo_name = histo_names[idx]
            legend = legends[idx]
            plotter = make_eff_plot_allWheels if type == "eff" else (make_hist_plot_allWheels if type == "histo" else make_hist_div_allWheels)
            grph = plotter(
                file=file_name,
                histo_name=histo_name,
                scaling=scaling,
            )
            graphs.append((grph, legend))

    nBins = 20
    binFirst = 0
    binLast = 20

    if not maxY:
        maxY = max([grph.GetMaximum() for grph, _ in graphs]) * 1.2

    # Now plot the graphs
    plot_graphs(
        graphs = graphs, 
        name = output_name,
        nBins = nBins, 
        firstBin = binFirst, 
        lastBin = binLast,
        xMin = -0.1, 
        xMax = 20.1,
        labels = ["-2", "-1", "0", "+1", "+2"]*4,
        maxY = maxY,
        notes =  [
            ("Private work (#bf{CMS} Phase-2 Simulation)", (.08, .90, .5, .95), 0.03),
            ("200 PU", (.75, .90, .89, .95), 0.03),
            ("MB1",    (.14, .1,  .29, .50), 0.05),
            ("MB2",    (.34, .1,  .49, .50), 0.05),
            ("MB3",    (.54, .1,  .69, .50), 0.05),
            ("MB4",    (.74, .1,  .89, .50), 0.05),      
        ] + aditional_notes,
        lines = [
            (5, 0, 5, maxY),
            (10, 0, 10, maxY),
            (15, 0, 15, maxY)
        ],
        legend_pos=legend_pos,
        drawOption="p same" if type == "div" else "pe1 same",
        titleX = titleX, 
        titleY = titleY,
        logx = logx,
        logy = logy,
        outfolder = outfolder,
        repit_color_each=repite_color_each,
    )


def make_eff_plot_allWheels(
        file,
        histo_name,
        scaling=1
    ):
    """
    Make a plot of the efficiency per wheel - file should contain histograms with pattern name MB[X],
    X going from 1 to 4
    """

    total_num = r.TH1D( f"", "", 20, 0, 20)
    total_den = r.TH1D( f"", "", 20, 0, 20)

    f = r.TFile.Open( file )
    pre, pos = histo_name.split("MB")
    nums = [ deepcopy( f.Get( f"{pre}MB{ist}{pos[1:]}_num")) for ist in range(1, 5) ]    
    dens = [ deepcopy( f.Get(f"{pre}MB{ist}{pos[1:]}_total")) for ist in range(1, 5) ]    

    f.Close()

    for iWheel in range(1, 6):
        for iStation in range(1, 5):
            iBin = 4 * (iStation-1) + iWheel + (iStation != 1)*(iStation - 1) 
            total_num.SetBinContent( iBin, nums[ iStation - 1 ].GetBinContent(iWheel))
            total_den.SetBinContent( iBin, dens[ iStation - 1 ].GetBinContent(iWheel))

    eff = r.TEfficiency(total_num,  total_den)
    effgr = eff.CreateGraph()
    for ibin in range(effgr.GetN()):
        effgr.SetPointEXhigh(ibin, 0)
        effgr.SetPointEXlow(ibin, 0)

    return effgr

def main():    
    # --- Files to be used --- #
    folder = "."
    color_msg("Plotting fwShower efficiency", color="blue")

    histonames = [
        "Fwshower_eff_MBX",
        "Fwshower_eff_MBX_2",
        "Fwshower_eff_MBX_3",
        "Fwshower_eff_MBX_4",
    ]

    thresholds = [6, 8, 10, 12, 14, 16, 24]
    info_for_plots = [
        { 
            "file_name": f"{folder}/histograms/histograms_thr_rsfix{thr}.root",
            "histos_names": "Fwshower_eff_MBX",
            "legends": f" thr - {thr}",
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
        output_name = "eff_fwshower_rsfix",
        outfolder=folder+"/eff_plots",
        legend_pos=(0.43, 0.48, 0.5, 0.66),
        aditional_notes=[("(TP + TN) / (TP + TN + FP + FN)", (.44, .38, .5, .47), 0.03)]
    )

    color_msg("Done!", color="green")


if __name__ == "__main__":
    main()
