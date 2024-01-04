""" 
Module to read ntuples and produce flat root files from which to plot performance
"""

# -- Import libraries -- #
import ROOT as r
import os, re, time
import json
from utils.functions import color_msg
from geometry.cmsdt import dt
from particles.segment import segment
from particles.gen_muon import gen_muon
from particles.shower import shower
from copy import deepcopy
import numpy as np
import itertools
  
class ntuple(object):
  def __init__(self, inputFolder, outfolder = "results", maxevents = -1, maxfiles = -1, postfix = ""):
    # Save in attributes
    self.inputFolder = inputFolder
    self.outfolder = outfolder
    self.maxevents = maxevents
    self.maxfiles = maxfiles
    self.postfix = postfix
    
    # Make Iterators for when we want to iterate over different subdetectors
    self.wheels   = np.arange(-2, 3)
    self.sectors  = np.arange(1, 15)
    self.stations = np.arange(1, 5)


    # Create the dictionary to store results
    self.dts     = {f"Wh{wh}_Sc{sc}_MB{st}" : dt(wh, sc, st) for wh, sc, st in itertools.product(self.wheels, self.sectors, self.stations)}

    # Prepare input
    self.load_tree(inputFolder)
    
    # Prepare output
    self.create_outfolder(outfolder)
    self.create_histograms()
    return

  
  def create_histograms(self):
    """ Histograms to be saved in the rootfiles for further plotting """
    self.histograms = {}
    
    # -- Control plots for the tag muon
    self.histograms["tag_muon_pt"] = \
      r.TH1D("tag_muon_pt", r';p_T; Events', 20, 0 , 1000) 
    self.histograms["tag_muon_eta"] = \
      r.TH1D("tag_muon_eta", r';#eta; Events', 20, -4 , 4) 
    self.histograms["tag_muon_dPhi_closest_segment"] = \
      r.TH1D("tag_muon_dPhi_closest_segment", r';maximum #Delta#phi (w matched segments); Events', 20, 0 , 1.5) 
    self.histograms["tag_muon_dEta_closest_segment"] = \
      r.TH1D("tag_muon_dEta_closest_segment", r';maximum #Delta#eta (w matched segments); Events', 20, 0 , 7)
      
  def run(self):
    """ Main logic is implemented in this method
    ------------------------------------------------- 
    Things that are done here:
      1. Reconstruct event information
      2. Fill histograms
      3. Plot histograms
    """

    t = self.tree    
    maxevents = self.maxevents if self.maxevents > 0 and self.maxevents < t.GetEntries() else t.GetEntries()
    for iev, ev in enumerate(t):

      if iev >= maxevents: break # A programmer cries when seeing this :)

      if iev%(maxevents/10) == 0:
        color_msg("Event: %d"%iev, "yellow", indentLevel = 0)      
      
      # First, get all the needded objects
      self.analyze_topology(ev)
      self.match_segments_to_gen()

      #for wh, sc, st in itertools.product(self.wheels, self.sectors, self.stations):
      #  dt = self.dts[f"Wh{wh}_Sc{sc}_MB{st}"]       
      #  if len(dt.digis) != 0: dt.summarize()

      self.fill_histograms()
      self.save_histograms()

  
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
    self.clear_dts()
      
    # --------- Digis --------- #
    self.reconstruct_digis(ev)
   
    # --------- Offline segments --------- #
    self.reconstruct_offline_segments(ev)
    
    # --------- Showers --------- #
    self.reconstruct_showers(ev)
    
    # --------- Generator level muons --------- #
    self.genmuons = []
    for m in range(ev.gen_nGenParts):
      if abs(ev.gen_pdgId[m]) == 13:
        self.genmuons.append( gen_muon(m, ev.gen_pt[m], ev.gen_eta[m], ev.gen_phi[m], ev.gen_charge[m]) )
      
  
  def reconstruct_digis(self, ev):
    """ Method to retrieve digi information in DT objects """
    dtlabel = "Wh{}_Sc{}"
    ndigis = ev.digi_nDigis
    for idigi in range(ndigis):
      # Locate the hit in the system
      wh = ev.digi_wheel[idigi]
      sc = ev.digi_sector[idigi]
      st = ev.digi_station[idigi]
      sl = int(ev.digi_superLayer[idigi])

      # Extract useful information
      w = int(ev.digi_wire[idigi])
      l = int(ev.digi_layer[idigi])-1 # Layers start at 1
      time = int(ev.digi_time[idigi])

      # Add the hit to the corresponding chamber
      self.dts[f"Wh{wh}_Sc{sc}_MB{st}"].add_digi((l, w, time, sl))
    
  
  def reconstruct_offline_segments(self, ev):
    """ Method to retrieve segment information in DT objects """
    nsegments = ev.seg_nSegments
    for iseg in range(nsegments):
      # Locate the segment in the system
      wh = ev.seg_wheel[iseg]
      sc = ev.seg_sector[iseg]
      st = ev.seg_station[iseg]
      
      # Create a segment object to get the useful info
      segment_obj = segment(ev, iseg)

      # Add the segment to the corresponding chamber
      self.dts[f"Wh{wh}_Sc{sc}_MB{st}"].add_segment(segment_obj)
    
  
  def reconstruct_showers(self, ev):
    """ Method to retrieve info from the shower algorithm """
    nShowerObj = len(ev.ph2Shower_station)
    for iShower in range(nShowerObj): # Algorithm found a shower
      wh = ev.ph2Shower_wheel[iShower]
      
      """ FIXME: The sector in the CMSSW was not corrected yet (need to subtract one). It has been
      corrected in the CMSSW software, but not in the ntuple I'm currently using, so I hardcode the fix. """
      sc = ev.ph2Shower_sector[iShower] - 1 
      st = ev.ph2Shower_station[iShower]

      # Create a shower object to get the useful info
      shower_obj = shower(ev, iShower)
      
      # Add the shower to the corresponding chamber
      self.dts[f"Wh{wh}_Sc{sc}_MB{st}"].add_shower(shower_obj)

  def match_segments_to_gen(self):
    """
      FIRST MATCHING: Segments with generation particles
    -----------------------------------------------------------------
    A generation particle might have one matching segments in each station
    of the detector. Procedure:
      1. Find segments that match in dPhi and dEta with the generated muon
      2. Store that information in the sector object
    """
    
    for wh, sc, st in itertools.product(self.wheels, self.sectors, self.stations):        
      dt = self.dts[f"Wh{wh}_Sc{sc}_MB{st}"]
      segments = dt.segments
      
      # Iterate over generated muons
      for igm, gm in enumerate(self.genmuons):
        for seg in segments:
          wh = seg.wh
          sc = seg.sc
          st = seg.st
          isMB4 = (st == 4)

          # Use method to match genmuons to segments
          isMatched = gm.match_to_segment_Filter(seg, isMB4)        
          if isMatched:
            dt.add_genmuon(gm)
    
  
  def fill_histograms(self):
    for dtlab, dt in self.dts.items():
      
      genmuons = dt.genmuons
      showers  = dt.showers
      segments = dt.segments
      digis    = dt.digis
      
      matchingTrueMuon = len(segments) >= 2 and len(genmuons) > 0
      if matchingTrueMuon:
        self.histograms["tag_muon_pt"].Fill( genmuons[0].pt )
        self.histograms["tag_muon_eta"].Fill( genmuons[0].eta )
        self.histograms["tag_muon_dPhi_closest_segment"].Fill( genmuons[0].get_max_dphi() )
        self.histograms["tag_muon_dEta_closest_segment"].Fill( genmuons[0].get_max_deta() )

  
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
    return 
  
  def clear_dts(self):
    """ Clean metadata stored in the object """
    for label, station in self.dts.items():
      station.clear()
  
  def save_histograms(self):
    """ Method to store histograms in a rootfile """
    outname = os.path.join(self.outfolder, "histograms%s.root"%self.postfix)
    f = r.TFile.Open(outname, "RECREATE")
    for hname, histo in self.histograms.items():
      histo.Write(hname.replace("-", "m"))
    f.Close()
    return
