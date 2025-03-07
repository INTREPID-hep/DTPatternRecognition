import ROOT as r
from dtpr.utils.functions import deltaR
# Iterator for when we want to iterate over different subdetectors
from src.utils.functions import wheels, stations, sectors, superlayers

dummyVal = -9999

# Histograms defined here...
# - shower_eff_muon_pt
# - shower_eff_muon_eta
# - LeadingMuon_pt
# - LeadingMuon_eta
# - LeadingMuon_maxDPhi
# - LeadingMuon_maxDEta
# - SubLeadingMuon_pt
# - SubLeadingMuon_eta
# - SubLeadingMuon_maxDPhi
# - SubLeadingMuon_maxDEta
# - muon_DR
# - nGenMuons
# - dphimax_seg_showering_muon
# - dphimax_seg_non_showering_muon
# - dphimax_tp_showering_muon
# - dphimax_tp_non_showering_muon
# - dphi_seg_showering_muon
# - dphi_seg_non_showering_muon
# - dphi_tp_showering_muon
# - dphi_tp_non_showering_muon


# -- These are computed using the baseline selection
histos = {}

histos.update({
  "shower_eff_muon_pt" :  {  
      "type" : "eff",
      "histoDen" : r.TH1D("Shower_eff_muon_pt_total", r';Wheel; Events', 20, 0 , 1000),
      "histoNum" : r.TH1D("Shower_eff_muon_pt_num", r';Wheel; Events', 20, 0 , 1000),
      "func"     : lambda reader: [gm.pt for gm in reader.genmuons if gm.did_shower()], 
      "numdef"   : lambda reader: [ len(reader.showers) > 0 ] 
  },
  "shower_eff_muon_eta" :  {  
      "type" : "eff",
      "histoDen" : r.TH1D("Shower_eff_muon_eta_total", r';Wheel; Events', 20, -1 , 1),
      "histoNum" : r.TH1D("Shower_eff_muon_eta_num", r';Wheel; Events', 20, -1 , 1),
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
    "func" : lambda reader: deltaR( reader.genmuons[0], reader.genmuons[1] ) if len(reader.genmuons) > 1 else dummyVal,
  },
  "nGenMuons" : {
    "type" : "distribution",
    "histo" : r.TH1D("nGenMuons", r';Number of generator muons; Events', 20, -3 , 3),
    "func" : lambda reader: len(reader.genmuons),
  },
})

# --- Showering muon properties
histos.update({
    "dphimax_seg_showering_muon" : {
      "type" : "distribution",
      "histo" : r.TH1D("dphimax_showering_muon", r';Max #Delta#phi Seg showering muon; Events', 60, 0 , 0.3),
      "func" : lambda reader: [gm.get_dphimax_segments() for gm in reader.genmuons if gm.did_shower()],
    },
    "dphimax_seg_non_showering_muon" : {
      "type" : "distribution",
      "histo" : r.TH1D("dphimax_non_showering_muon", r';Max #Delta#phi Seg non-showering muon; Events', 60, 0 , 0.3),
      "func" : lambda reader: [gm.get_dphimax_segments() for gm in reader.genmuons if not gm.did_shower()],
    },
    "dphimax_tp_showering_muon" : {
      "type" : "distribution",
      "histo" : r.TH1D("dphimax_tp_showering_muon", r';Max #Delta#phi TP showering muon; Events', 60, 0 , 0.3),
      "func" : lambda reader: [gm.get_dphimax_tp() for gm in reader.genmuons if gm.did_shower()],
    },
    "dphimax_tp_non_showering_muon" : {
      "type" : "distribution",
      "histo" : r.TH1D("dphimax_tp_non_showering_muon", r';Max #Delta#phi TP non-showering muon; Events', 60, 0 , 0.3),
      "func" : lambda reader: [gm.get_dphimax_tp() for gm in reader.genmuons if not gm.did_shower()],
    },
    "dphi_seg_showering_muon" : {
      "type" : "distribution",
      "histo" : r.TH1D("dphi_showering_muon", r';#Delta#phi Seg showering muon; Events', 60, 0 , 0.3),
      "func" : lambda reader: [gm.get_dphi_segments() for gm in reader.genmuons if gm.did_shower()],
    },
    "dphi_seg_non_showering_muon" : {
      "type" : "distribution",
      "histo" : r.TH1D("dphi_non_showering_muon", r';#Delta#phi Seg non-showering muon; Events', 60, 0 , 0.3),
      "func" : lambda reader: [gm.get_dphi_segments() for gm in reader.genmuons if not gm.did_shower()],
    },
    "dphi_tp_showering_muon" : {
      "type" : "distribution",
      "histo" : r.TH1D("dphi_tp_showering_muon", r';#Delta#phi TP showering muon; Events', 60, 0 , 0.3),
      "func" : lambda reader: [gm.get_dphi_tp() for gm in reader.genmuons if gm.did_shower()],
    },
    "dphi_tp_non_showering_muon" : {
      "type" : "distribution",
      "histo" : r.TH1D("dphi_tp_non_showering_muon", r';#Delta#phi TP non-showering muon; Events', 60, 0 , 0.3),
      "func" : lambda reader: [gm.get_dphi_tp() for gm in reader.genmuons if not gm.did_shower()],
    },
})

