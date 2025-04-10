import ROOT as r
from copy import deepcopy
import os

r.gStyle.SetOptStat(0)
_noDelete = {}

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

def make_hist_div_allWheels(
        file,
        histo_name,
        scaling=1
    ):
    total_num = r.TH1D( f"", "", 20, 0, 20)
    total_den = r.TH1D( f"", "", 20, 0, 20)

    f = r.TFile.Open( file )
    pre, pos = "fwshower_tp_MBX".split("MB")
    nums = [ deepcopy( f.Get( f"{pre}MB{ist}{pos[1:]}")) for ist in range(1, 5) ]    
    pre, pos = "reals_shower_MBX".split("MB")
    dens = [ deepcopy( f.Get(f"{pre}MB{ist}{pos[1:]}")) for ist in range(1, 5) ]    

    f.Close()

    for iWheel in range(1, 6):
        for iStation in range(1, 5):
            iBin = 4 * (iStation-1) + iWheel + (iStation != 1)*(iStation - 1) 
            total_num.SetBinContent( iBin, nums[ iStation - 1 ].GetBinContent(iWheel))
            total_den.SetBinContent( iBin, dens[ iStation - 1 ].GetBinContent(iWheel))

    eff = r.TH1D( f"", "", 20, 0, 20)
    eff = total_num.Clone()
    eff.Divide(total_den)
    r.gStyle.SetErrorX(0.0001)

    return eff

def make_hist_plot_allWheels(
        file,
        histo_name,
        scaling=1
    ):
    """"
    Make a histogram per wheel - file should contain histograms with pattern name MB[X],
    X going from 1 to 4
    """

    histo= r.TH1D( f"", "", 20, 0, 20)

    f = r.TFile.Open( file )
    pre, pos = histo_name.split("MB")
    histos = [ deepcopy( f.Get( f"{pre}MB{ist}{pos[1:]}")) for ist in range(1, 5) ]

    f.Close()

    for iWheel in range(1, 6):
        for iStation in range(1, 5):
            iBin = 4 * (iStation-1) + iWheel + (iStation != 1)*(iStation - 1) 
            histo.SetBinContent( iBin, histos[ iStation - 1 ].GetBinContent(iWheel))

    if scaling != 1:
        histo.Scale(scaling)

    r.gStyle.SetErrorX(0.0001)

    return histo


def plot_graphs(
    graphs,
    name,
    nBins,
    firstBin,
    lastBin,
    xMin=None,
    xMax=None,
    maxY=None,
    titleX=None,
    titleY=None,
    labels=[],
    notes=[],
    lines=[],
    legend_pos=(0.62, 0.37, 0.70, 0.45),
    drawOption="pe1 same",
    logx=False,
    logy=False,
    outfolder="results/plots",
    repit_color_each=1,
):
    """
    Plots and save a set of graphs.

    Args:
        graphs (list): The list of graphs to plot.
        name (str): The name of the plot.
        nBins (int): The number of bins.
        firstBin (float): The first bin value.
        lastBin (float): The last bin value.
        xMin (float, optional): The minimum x-axis value. Default is None.
        xMax (float, optional): The maximum x-axis value. Default is None.
        maxY (float, optional): The maximum y-axis value. Default is None.
        titleX (str, optional): The x-axis title. Default is None.
        titleY (str, optional): The y-axis title. Default is None.
        labels (list, optional): The list of labels for the x-axis. Default is [].
        notes (list, optional): The list of notes to add to the plot. Default is [].
        lines (list, optional): The list of lines to add to the plot. Default is [].
        legend_pos (tuple, optional): The position of the legend. Default is (0.62, 0.37, 0.70, 0.45).
        outfolder (str, optional): The output folder to save the plot. Default is "results/plots".
    """
    # --- Create canvas --- #
    c = r.TCanvas("c_%s" % (name), "", 1200, 1200)

    # --- Create legend --- #
    x0leg, y0leg, x1leg, y1leg = legend_pos
    legend = r.TLegend(x0leg, y0leg, x1leg, y1leg)
    legend.SetName("l_%s" % (name))
    legend.SetBorderSize(0)
    legend.SetFillColor(0)
    legend.SetTextFont(42)
    legend.SetTextSize(0.028)
    legend.SetNColumns(1)

    # --- Create a frame metadata --- #
    frame = r.TH1D(name, "", nBins, firstBin, lastBin)
    frame.GetXaxis().SetTitleFont(42)
    frame.GetXaxis().SetTitleSize(0.04)
    frame.GetXaxis().SetLabelFont(42)
    frame.GetXaxis().SetLabelSize(0.03)
    frame.GetYaxis().SetTitleFont(42)
    frame.GetYaxis().SetTitleSize(0.04)
    frame.GetYaxis().SetLabelFont(42)
    frame.GetYaxis().SetLabelSize(0.023)

    if xMin and xMax:
        frame.GetXaxis().SetRangeUser(xMin, xMax)
    if maxY:
        frame.GetYaxis().SetRangeUser(0.001, maxY)
    if titleX:
        frame.GetXaxis().SetTitle(titleX)
    if titleY:
        frame.GetYaxis().SetTitle(titleY)
    if labels != []:
        for iBin in range(frame.GetNbinsX()):
            frame.GetXaxis().SetBinLabel(iBin + 1, labels[iBin])
    frame.Draw("axis")

    # open the root files
    color = 1
    marker_style = 20
    for igr, grInfo in enumerate(graphs):
        effgr, legendName = grInfo
        effgr.SetMarkerColor(color)
        effgr.SetLineColor(color)
        effgr.SetMarkerSize(1.5)
        effgr.SetMarkerStyle(marker_style)
        legend.AddEntry(effgr, legendName, "p")

        marker_style += 2
        if (igr + 1) % repit_color_each == 0:
            color += 1
            if color == 10 or color == 18:
                color += 1  # skip white colors
            marker_style = 20  # reset marker style when color changes
        effgr.Draw(drawOption)

    # Now add texts and lines
    for note in notes:
        text = note[0]
        x1, y1, x2, y2 = note[1]
        textSize = note[2]
        align = 12
        texnote = deepcopy(r.TPaveText(x1, y1, x2, y2, "NDC"))
        texnote.SetTextSize(textSize)
        texnote.SetFillColor(0)
        texnote.SetFillStyle(0)
        texnote.SetLineWidth(0)
        texnote.SetLineColor(0)
        texnote.SetTextAlign(align)
        texnote.SetTextFont(42)
        texnote.AddText(text)
        texnote.Draw("same")
        _noDelete[texnote] = texnote  # So it does not get deleted by ROOT

    for line in lines:
        xpos0, ypos0, xpos1, ypos1 = line
        texline = deepcopy(r.TLine(xpos0, ypos0, xpos1, ypos1))
        texline.SetLineWidth(3)
        texline.Draw("same")
        _noDelete[texline] = texline  # So it does not get deleted by ROOT

    legend.Draw("same")

    if logx:
        r.gPad.SetLogx()
    if logy:
        r.gPad.SetLogy()

    outpath = os.path.join(outfolder, name)
    if not os.path.exists(outpath):
        os.system("mkdir -p %s" % outpath)
    c.SaveAs(outpath + "/%s.svg" % name)
    c.SaveAs(outpath + "/%s.pdf" % name)