import numpy as np
from dtpr.utils.baseCanvas import baseCanvas
from copy import deepcopy 
import ROOT as r
from dtpr.utils.functions import color_msg
import sys 

def create_frame(name, nbins, binFirst, binLast, labels = []):
    """ Creates axis frames for plotting """  
    frame = r.TH1D(name, "", nbins, binFirst, binLast )
    frame.GetXaxis().SetTitleFont(42)
    frame.GetXaxis().SetTitleSize(0.03)
    frame.GetXaxis().SetLabelFont(42)
    frame.GetXaxis().SetLabelSize(0.04)
    frame.GetYaxis().SetTitleFont(42)
    frame.GetYaxis().SetTitleSize(0.03)
    frame.GetYaxis().SetLabelFont(42)
    frame.GetYaxis().SetLabelSize(0.04)
    
    if labels != []:
        for iBin in range( 1, frame.GetNbinsX()+1 ):
            frame.GetXaxis().SetBinLabel(iBin, labels[iBin-1])    
    return frame 

def make_eff_plot( files, name, numVar, denVar, nbins, binFirst, binLast, labels = [], titleX = "", titleY = "", notes = [], lines = [], save = False):
    """ Plot for segment matching efficiency per wheel and MB """        
    bC = baseCanvas( name, notes, lines )
    bC.make_legend(0.62, 0.37, 0.70, 0.45)
    
    # Make a dummy histo for the axis
    frame = create_frame( name + "_frame", nbins, binFirst, binLast, labels )
    frame.GetXaxis().SetRangeUser(binFirst, binLast)
    frame.GetYaxis().SetRangeUser(0, 1.05)
    frame.GetYaxis().SetTitle(titleX)
    frame.GetXaxis().SetTitle(titleY)

    bC.add_frame( frame )
    # open the root files
    for procName, file_info  in files.items():
        fileName = file_info["file"]
        
        f = r.TFile.Open( fileName )
        # Get the numerator histograms
        hNum = deepcopy(f.Get(numVar))
        hDen = deepcopy(f.Get(denVar))
        f.Close()
 
        # Now compute the efficiency
        eff = r.TEfficiency(hNum,  hDen)
        effgr = eff.CreateGraph()
        for ibin in range(effgr.GetN()):
            effgr.SetPointEXhigh(ibin, 0)
            effgr.SetPointEXlow(ibin, 0)
        effgr.SetMarkerColor( file_info["color"] )
        effgr.SetLineColor( file_info["color"] )
        effgr.SetMarkerStyle( file_info["marker"] )
        effgr.SetMarkerSize( 1 )
        bC.add_graph(effgr, procName)
        
    # Now add text
    bC.doSpam()
    for note in notes:
        bC.add_text(note[0], note[1], note[2])
    for line in lines:
        bC.add_line(line[0], line[1], line[2], line[3])

    bC.draw()
    if save:
        bC.save_canvas("results")
    return bC
