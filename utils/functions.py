""" Miscelaneous """
import math
from particles.segment import segment 
import ROOT as r
r.gStyle.SetOptStat(0)
import os
from copy import deepcopy

_noDelete = { "lines" : []}
def color_msg(msg, color = "none", indentLevel = 0):
    """ Prints a message with ANSI coding so it can be printout with colors """
    codes = {
        "none" : "0m",
        "green" : "1;32m",
        "red" : "1;31m",
        "blue" : "1;34m",
        "yellow" : "1;35m"
    }

    indentStr = ""
    if indentLevel == 0: indentStr = ">>"
    if indentLevel == 1: indentStr = "+"
    if indentLevel == 2: indentStr = "*"
    if indentLevel == 3: indentStr = "-->"

    
    print("\033[%s%s %s \033[0m"%(codes[color], "  "*indentLevel + indentStr, msg))
    return

def get_best_matches( reader, station = 1 ):
    """ Return the bin for the best matching segments of each generator muon """
    # Fill with dummy segments   
    genmuons = reader.genmuons
    bestMatches = [ None for igm in range(len(genmuons)) ]

    # This is what's done in Jaime's code: https://github.com/jaimeleonh/DTNtuples/blob/unifiedPerf/test/DTNtupleTPGSimAnalyzer_Efficiency.C#L181-L208
    # Basically: get the best matching segment to a generator muon per MB chamber

    #color_msg(f"[FUNCTIONS::GET_BEST_MATCHES] Debugging with station {station}", color = "red", indentLevel = 0)
    for igm, gm in enumerate(genmuons):
        #color_msg(f"[FUNCTIONS::GET_BEST_MATCHES] igm {igm}", indentLevel = 1)
        #gm.summarize(indentLevel = 2)
        for bestMatch in gm.matches:
            if bestMatch.st == station:
                bestMatches[ igm ] =  bestMatch
            
    # Remove those that are None which are simply dummy values
    bestMatches = filter( lambda key: key is not None, bestMatches )
    return bestMatches

def deltaPhi(phi1, phi2):
    res = phi1 - phi2
    while res > math.pi: res -= 2*math.pi
    while res <= -math.pi: res += 2*math.pi
    return res

def deltaR(p1, p2):
    dEta = abs(p1.eta-p2.eta)
    dPhi = deltaPhi(p1.phi, p2.phi)
    return math.sqrt(dEta*dEta + dPhi*dPhi)

def plot_graphs(graphs, name, nBins, firstBin, lastBin, 
                xMin = None, xMax = None, maxY = None, titleX = None, titleY = None, 
                labels = [], notes = [], lines = [],
                legend_pos = (0.62, 0.37, 0.70, 0.45),
                outfolder = "results/plots"):
    
    """ Plot a set of graphs """
    # --- Create canvas --- #
    c = r.TCanvas("c_%s"%(name), "", 800, 800)
    
    # --- Create legend --- #
    x0leg, y0leg, x1leg, y1leg = legend_pos
    legend = r.TLegend(x0leg, y0leg, x1leg, y1leg)
    legend.SetName("l_%s"%(name))
    legend.SetBorderSize(0)
    legend.SetFillColor(0)
    legend.SetTextFont(42)
    legend.SetTextSize(0.028)
    legend.SetNColumns(1)
    
    # --- Create a frame metadata --- #
    frame = r.TH1D(name, "", nBins, firstBin, lastBin)
    frame.GetXaxis().SetTitleFont(42)
    frame.GetXaxis().SetTitleSize(0.03)
    frame.GetXaxis().SetLabelFont(42)
    frame.GetXaxis().SetLabelSize(0.04)
    frame.GetYaxis().SetTitleFont(42)
    frame.GetYaxis().SetTitleSize(0.03)
    frame.GetYaxis().SetLabelFont(42)
    frame.GetYaxis().SetLabelSize(0.04)
    
    if xMin and xMax:
        frame.GetXaxis().SetRangeUser(xMin, xMax)
    if maxY:
        frame.GetYaxis().SetRangeUser(0, maxY)
    if titleX: 
        frame.GetYaxis().SetTitle(titleX)
    if titleY:
        frame.GetXaxis().SetTitle(titleY)
    
    if labels != []:
        for iBin in range(frame.GetNbinsX()):
            frame.GetXaxis().SetBinLabel(iBin + 1, labels[iBin])
    frame.Draw("axis")
    
    # open the root files
    color = 1
    for igr, grInfo in enumerate( graphs ):
        effgr, legendName = grInfo
        effgr.SetMarkerColor( color )
        effgr.SetLineColor( color )
        effgr.SetMarkerSize( 1 )
        effgr.SetMarkerStyle( 20 )
        legend.AddEntry( effgr, legendName, "p")
        if color == [10, 18]: color += 1 # skip white colors
        effgr.Draw("pe1 same")
        color += 1

    
    # Now add texts and lines
    for note in notes:
        text = note[0]
        x1, y1, x2, y2 = note[1]
        textSize = note[2]
        align=12 
        texnote = deepcopy(r.TPaveText(x1, y1, x2, y2,"NDC"))
        texnote.SetTextSize(textSize)
        texnote.SetFillColor(0)
        texnote.SetFillStyle(0)
        texnote.SetLineWidth(0)
        texnote.SetLineColor(0)
        texnote.SetTextAlign(align)
        texnote.SetTextFont(42)
        texnote.AddText(text)
        texnote.Draw("same")
        _noDelete[texnote] = texnote # So it does not get deleted by ROOT

        
    for line in lines:
        xpos0, ypos0, xpos1, ypos1 = line
        texline = deepcopy(r.TLine(xpos0, ypos0, xpos1, ypos1))
        texline.SetLineWidth(3)
        texline.Draw("same") 
        _noDelete[texline] = texline # So it does not get deleted by ROOT


    legend.Draw("same")
    
    outpath = os.path.join( outfolder, name)
    if not os.path.exists(outpath):    
        os.system("mkdir -p %s"%outpath)
    c.SaveAs(outpath+"/%s.png"%name)
    c.SaveAs(outpath+"/%s.pdf"%name)