""" 
Module to read ntuples and produce flat root files from which to plot performance
"""

# -- Import libraries -- #
import ROOT as r
import os, re, time
import json
import math
from copy import deepcopy
import numpy as np
import itertools

from utils.functions import color_msg
from geometry.cmsdt import dt
from particles.segment import segment
from particles.gen_muon import gen_muon
from particles.shower import shower
from particles.ph2TriggerPrimitives import ph2tpg
from particles.digis import digi

class ntuple(object):
  def __init__(self, 
               inputFolder, 
               selectors,
               histograms,
               outfolder = "results", 
               maxevents = -1, 
               maxfiles = -1, 
               postfix = ""):
    
    # Save in attributes
    self.inputFolder = inputFolder
    self.selectors = selectors
    self.histograms = histograms
    self.outfolder = outfolder
    self.maxevents = maxevents
    self.maxfiles = maxfiles
    self.postfix = postfix
    
    # Make Iterators for when we want to iterate over different subdetectors
    self.wheels   = np.arange(-2, 3)
    self.sectors  = np.arange(1, 15)
    self.stations = np.arange(1, 5)
    
    # Prepare input
    self.load_tree(inputFolder)
    
    # Prepare output
    self.create_outfolder(outfolder)
    return
      
  def run(self):
    """ Main workflow """
    t = self.tree    
    maxevents = self.maxevents if self.maxevents > 0 and self.maxevents < t.GetEntries() else t.GetEntries()
    for iev, ev in enumerate(t):
      self.event = ev
      if iev >= maxevents: break # A programmer cries when seeing this :)

      if iev%(maxevents/10) == 0:
        color_msg("Event: %d"%iev, "yellow", indentLevel = 0)      
      
      # First, get all the needded objects
      self.analyze_topology(ev)
      
      # Match muons to segments in a broad dPhi/dEta window
      
      for gm in self.genmuons:
          # Match segments to generator muons
          for seg in self.segments:
            #gm.match_segment(seg, math.pi / 6., 0.8)
            gm.match_segment(seg, 0.1, 0.3)

          # Now re-match with TPs
          matched_segments = gm.matches
          for seg in matched_segments:
            for tp in self.tps:
              seg.match_offline_to_AM( tp, max_dPhi = 0.1 )
        
      #self.summarize(iev)
      
      # Apply global selection
      if not self.passes_event(): continue      
      self.fill_histograms()

  def passes_event(self):
    """ The selector is used to apply global cuts on the events """
    #print(self.selector( self ))
    return all(selector(self) for selector in self.selectors)
  
  def summarize(self, iev):
    """ Method to show a small description of what has been recorded in this event """
    segments = self.segments
    digis = self.digis
    tps = self.tps
    
    color_msg( f"Generator muons", color = "green", indentLevel = 1)
    for igm, gm in enumerate(self.genmuons):
      color_msg( f"Muon {igm}", indentLevel = 2)
      gm.summarize(3)
    
    color_msg( f"Offline segments", color = "green", indentLevel = 1)
    color_msg( f"Number of segments: {len(self.segments)}", indentLevel = 2) # There might be a lot of segments so don't print everything
    phiseg = [f"({seg.index:.2f}, {seg.phi:.2f}, {seg.eta:.2f})" for seg in self.segments]
    color_msg( f"(iSeg, Phi, eta): {phiseg}", indentLevel = 2) # There might be a lot of segments so don't print everything
    color_msg( f"Trigger primitives", color = "green", indentLevel = 1)
    color_msg( f"Number of TPs: {len(self.tps)}", indentLevel = 2) # There might be a lot of segments so don't print everything  
                
  def analyze_topology(self, ev):
    """
    ---------------------------------------------------------
            Event reconstruction method
    ---------------------------------------------------------
    This method reconstructs the information of the different
    DT related objects: digis, offline segments and TPs.
    ---------------------------------------------------------
    """
   
    # Clear info from previous events
    self.clear()
      
    # --------- Digis --------- #
    ndigis = ev.digi_nDigis
    for idigi in range(ndigis):
      self.digis.append( digi(ev, idigi) )
   
    # --------- Offline segments --------- #
    nsegments = ev.seg_nSegments
    for iseg in range(nsegments):
      if ev.seg_phi_t0[iseg] > -500: # keep only good segments
        self.segments.append( segment(ev, iseg) )
    
    # --------- Ph2 TPs --------- #
    ntps = ev.ph2TpgPhiEmuAm_nTrigs
    for itp in range(ntps):
        self.tps.append( ph2tpg(ev, itp) )
    
    # --------- Showers --------- #
    nShowerObj = len(ev.ph2Shower_station)
    for iShower in range(nShowerObj):
      self.showers.append( shower(ev, iShower) )
          
    # --------- Generator level muons --------- #
    for m in range(ev.gen_nGenParts):
      if abs(ev.gen_pdgId[m]) == 13:
        self.genmuons.append( gen_muon(m, ev.gen_pt[m], ev.gen_eta[m], ev.gen_phi[m], ev.gen_charge[m]) )
    
    # Sort generator muons by pT
    self.genmuons.sort( key = lambda m : m.pt, reverse = True) 
    
  def load_tree(self, inpath):
    """ Simple function to retrieve a chain with all the trees to be analyzed """
    self.tree = r.TChain()

    if "root" in inpath:
      color_msg(f"Opening input file {inpath}", "blue", 1)
      self.tree.Add(inpath + "/dtNtupleProducer/DTTREE")
    else:
      color_msg(f"Opening input files from {inpath}", "blue", 1)
      allFiles = os.listdir(inpath)
      nFiles = min(len(allFiles), self.maxfiles)
      for iF in range( nFiles ):
        if "root" not in allFiles[iF]: continue
        color_msg(f"File {allFiles[iF]} added", indentLevel=2)
        self.tree.Add( os.path.join(inpath, allFiles[iF]) + "/dtNtupleProducer/DTTREE")
  
  def create_outfolder(self, outname):
    """ Create an output path where to store the histograms """
    if not(os.path.exists(outname)): 
      os.system("mkdir -p %s"%outname)
      os.system("cp utils/index.php %s/"%outname)
     
  def clear(self):
    """ Clean metadata """
    self.genmuons = []
    self.trueShowerMuons = []
    self.digis = []
    self.segments = []
    self.tps = []
    self.showers = []
    
  def fill_histograms(self):
    """ Apply selections and fill histograms """
    for histo, histoinfo in self.histograms.items():
      hType = histoinfo["type"]
      
      # Distributions
      if hType == "distribution":
        h = histoinfo["histo"]
        func = histoinfo["func"]
        val = func(self)
        
        # In case a function returns multiple results
        # and we want to fill for everything (e.g. there are multiple muons,
        # each of them with a matching segment. And we want to account for everything)
        if isinstance(val, list):
          for ival in val: 
            h.Fill( ival )
        else:
          h.Fill(val)
      
      # Efficiencies
      elif hType == "eff":
        func = histoinfo["func"]
        num = histoinfo["histoNum"]
        den = histoinfo["histoDen"]
        numdef = histoinfo["numdef"]
        
        val = func(self)
        numPasses = numdef(self)
        for val, passes in zip(val, numPasses):
          den.Fill(val)
          if passes:
            num.Fill(val)  
  
  def save_histograms(self):
    """ Method to store histograms in a rootfile """
    outname = os.path.join(self.outfolder, "histograms%s.root"%self.postfix)
    f = r.TFile.Open(outname, "RECREATE")
    for hname, histoinfo in self.histograms.items():
      hType = histoinfo["type"]
      if hType == "distribution":
        histo = histoinfo["histo"]
        histo.Write(histo.GetName())
      elif hType == "eff":
        histoNum = histoinfo["histoNum"]
        histoDen = histoinfo["histoDen"]
        histoNum.Write(histoNum.GetName())
        histoDen.Write(histoDen.GetName())

    f.Close()
    
    
    
