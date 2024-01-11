""" Histograms to be stored in the output rootfiles """
import ROOT as r
import utils.functions as fcns 

dummyVal = -9999
# -- These are computed using the baseline selection
histos = {}

# Efficiencies per station
histos.update({
    "seg_eff_MB1" :  {  
      "type" : "eff",
      "histoDen" : r.TH1D(f"Eff_MB1_total", r';Wheel; Events', 5, -2.5 , 2.5),
      "histoNum" : r.TH1D(f"Eff_MB1_AM_matched", r';Wheel; Events', 5, -2.5 , 2.5),
      "func"     : lambda reader: [seg.wh for seg in fcns.get_best_matches( reader, station = 1 )], # These are the values to fill with
      # These are the conditions on whether to fill numerator also. Imitating Jaime's code:
      # https://github.com/jaimeleonh/DTNtuples/blob/unifiedPerf/test/DTNtupleTPGSimAnalyzer_Efficiency.C#L443
      # Basically, for a best matching segment, if there's a bestTP candidate, fill the numerator.
      "numdef"   : lambda reader: [len(seg.matches) > 0 for seg in fcns.get_best_matches( reader, station = 1 ) ] 
  },
  "seg_eff_MB2" :  {  
      "type" : "eff",
      "histoDen" : r.TH1D(f"Eff_MB2_total", r';Wheel; Events', 5, -2.5 , 2.5),
      "histoNum" : r.TH1D(f"Eff_MB2_AM_matched", r';Wheel; Events', 5, -2.5 , 2.5),
      "func"     : lambda reader: [seg.wh for seg in fcns.get_best_matches( reader, station = 2 )], # These are the values to fill with
      # These are the conditions on whether to fill numerator also. Imitating Jaime's code:
      # https://github.com/jaimeleonh/DTNtuples/blob/unifiedPerf/test/DTNtupleTPGSimAnalyzer_Efficiency.C#L443
      # Basically, for a best matching segment, if there's a bestTP candidate, fill the numerator.
      "numdef"   : lambda reader: [len(seg.matches) > 0 for seg in fcns.get_best_matches( reader, station = 2 ) ] 
  },
  "seg_eff_MB3" :  {  
      "type" : "eff",
      "histoDen" : r.TH1D(f"Eff_MB3_total", r';Wheel; Events', 5, -2.5 , 2.5),
      "histoNum" : r.TH1D(f"Eff_MB3_AM_matched", r';Wheel; Events', 5, -2.5 , 2.5),
      "func"     : lambda reader: [seg.wh for seg in fcns.get_best_matches( reader, station = 3 )], # These are the values to fill with
      # These are the conditions on whether to fill numerator also. Imitating Jaime's code:
      # https://github.com/jaimeleonh/DTNtuples/blob/unifiedPerf/test/DTNtupleTPGSimAnalyzer_Efficiency.C#L443
      # Basically, for a best matching segment, if there's a bestTP candidate, fill the numerator.
      "numdef"   : lambda reader: [len(seg.matches) > 0 for seg in fcns.get_best_matches( reader, station = 3 ) ] 
  },
  "seg_eff_MB4" :  {  
      "type" : "eff",
      "histoDen" : r.TH1D(f"Eff_MB4_total", r';Wheel; Events', 5, -2.5 , 2.5),
      "histoNum" : r.TH1D(f"Eff_MB4_AM_matched", r';Wheel; Events', 5, -2.5 , 2.5),
      "func"     : lambda reader: [seg.wh for seg in fcns.get_best_matches( reader, station = 4 )], # These are the values to fill with
      # These are the conditions on whether to fill numerator also. Imitating Jaime's code:
      # https://github.com/jaimeleonh/DTNtuples/blob/unifiedPerf/test/DTNtupleTPGSimAnalyzer_Efficiency.C#L443
      # Basically, for a best matching segment, if there's a bestTP candidate, fill the numerator.
      "numdef"   : lambda reader: [len(seg.matches) > 0 for seg in fcns.get_best_matches( reader, station = 4 ) ] 
  },
})
  
histos.update({
  "shower_eff_muon_pt" :  {  
      "type" : "eff",
      "histoDen" : r.TH1D("Shower_eff_muon_pt_total", r';Wheel; Events', 20, 0 , 1000),
      "histoNum" : r.TH1D("Shower_eff_muon_pt_matched", r';Wheel; Events', 20, 0 , 1000),
      "func"     : lambda reader: [gm.pt for gm in reader.genmuons if gm.did_shower()], 
      "numdef"   : lambda reader: [ len(reader.showers) > 0 ] 
  },
  "shower_eff_muon_eta" :  {  
      "type" : "eff",
      "histoDen" : r.TH1D("Shower_eff_muon_eta_total", r';Wheel; Events', 20, 0 , 1000),
      "histoNum" : r.TH1D("Shower_eff_muon_eta_matched", r';Wheel; Events', 20, 0 , 1000),
      "func"     : lambda reader: [gm.eta for gm in reader.genmuons if gm.did_shower()], 
      "numdef"   : lambda reader: [ len(reader.showers) > 0 ] 
  }
})

histos.update({
  # --- Leading muon properties
  "LeadingMuon_pt" : {
    "type" : "distribution",
    "histo" : r.TH1D("LeadingMuon_pt", r';Leading muon p_T; Events', 20, 0 , 1000),
    "func" : lambda reader: reader.genmuons[0].pt,
  },
  "LeadingMuon_eta" : {
    "type" : "distribution",
    "histo" : r.TH1D("LeadingMuon_eta", r';Leading muon #eta; Events', 10, -3 , 3),
    "func" : lambda reader: reader.genmuons[0].eta,
  },
  "LeadingMuon_maxDPhi" : {
    "type" : "distribution",
    "histo" : r.TH1D("LeadingMuon_maxDPhi", r';Leading muon maximum #Delta#phi (with matched segments); Events', 20, 0 , 0.6),
    "func" : lambda reader: reader.genmuons[0].get_max_dphi()
  },
  "LeadingMuon_maxDEta" : {
    "type" : "distribution",
    "histo" : r.TH1D("LeadingMuon_Max_dEta", r';Leading muon maximum #Delta#eta (with matched segments); Events', 20, 0 , 1),
    "func" : lambda reader: reader.genmuons[0].get_max_deta()
  },
  
  # --- Subleading muon properties
  "SubLeadingMuon_pt" : {
    "type" : "distribution",
    "histo" : r.TH1D("SubLeadingMuon_pt", r';Subleading muon p_T; Events', 20, 0 , 1000),
    "func" : lambda reader: reader.genmuons[1].pt if len(reader.genmuons) > 1 else dummyVal,
  },
  "SubLeadingMuon_eta" : {
    "type" : "distribution",
    "histo" : r.TH1D("SubLeadingMuon_eta", r';Subleading muon #eta; Events', 10, -3 , 3),
    "func" : lambda reader: reader.genmuons[1].eta if len(reader.genmuons) > 1 else dummyVal,
  },
  "SubLeadingMuon_maxDPhi" : {
    "type" : "distribution",
    "histo" : r.TH1D("SubLeadingMuon_maxDPhi", r';Subleading muon maximum #Delta#phi (with matched segments); Events', 20, 0 , 0.6),
    "func" : lambda reader: reader.genmuons[1].get_max_dphi() if len(reader.genmuons) > 1 else dummyVal
  },
  "SubLeadingMuon_maxDEta" : {
    "type" : "distribution",
    "histo" : r.TH1D("SubLeadingMuon_Max_dEta", r';Subleading muon maximum #Delta#eta (with matched segments); Events', 20, 0 , 1),
    "func" : lambda reader: reader.genmuons[1].get_max_deta() if len(reader.genmuons) > 1 else dummyVal
  },
  
  # --- Muon relations
  "muon_DR" : {
    "type" : "distribution",
    "histo" : r.TH1D("muon_DR", r';#DeltaR both muons; Events', 20, 1 , 6),
    "func" : lambda reader: fcns.deltaR( reader.genmuons[0], reader.genmuons[1] ) if len(reader.genmuons) > 1 else dummyVal,
  },
  "nGenMuons" : {
    "type" : "distribution",
    "histo" : r.TH1D("nGenMuons", r';Number of generator muons; Events', 20, -3 , 3),
    "func" : lambda reader: len(reader.genmuons),
  },
  
  # --- Leading muon properties
  "nTrueShowers" : {
    "type" : "distribution",
    "histo" : r.TH1D("nTrueShowers", r'; nTrueShowers; Events', 2, 0 , 2),
    "func" : lambda reader: [gm.did_shower() for gm in reader.genmuons],
  },
})
