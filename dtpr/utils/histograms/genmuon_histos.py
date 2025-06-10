import ROOT as r
from dtpr.utils.functions import deltaR
from dtpr.utils.genmuon_functions import *

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
    "func" : lambda reader: get_dphimax_matched_segments(reader.genmuons[0])
  },
  "LeadingMuon_maxDEta" : {
    "type" : "distribution",
    "histo" : r.TH1D("LeadingMuon_Max_dEta", r';Leading muon maximum #Delta#eta (with matched segments); Events', 20, 0 , 1),
    "func" : lambda reader: get_max_deta(reader.genmuons[0])
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
    "func" : lambda reader: get_dphimax_matched_segments(reader.genmuons[1]) if len(reader.genmuons) > 1 else dummyVal
  },
  "SubLeadingMuon_maxDEta" : {
    "type" : "distribution",
    "histo" : r.TH1D("SubLeadingMuon_Max_dEta", r';Subleading muon maximum #Delta#eta (with matched segments); Events', 20, 0 , 1),
    "func" : lambda reader: get_max_deta(reader.genmuons[1]) if len(reader.genmuons) > 1 else dummyVal
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
      "func" : lambda reader: [get_dphimax_b2_matched_segments(gm) for gm in reader.genmuons if gm.showered],
    },
    "dphimax_seg_non_showering_muon" : {
      "type" : "distribution",
      "histo" : r.TH1D("dphimax_non_showering_muon", r';Max #Delta#phi Seg non-showering muon; Events', 60, 0 , 0.3),
      "func" : lambda reader: [get_dphimax_b2_matched_segments(gm) for gm in reader.genmuons if not gm.showered],
    },
    "dphimax_tp_showering_muon" : {
      "type" : "distribution",
      "histo" : r.TH1D("dphimax_tp_showering_muon", r';Max #Delta#phi TP showering muon; Events', 60, 0 , 0.3),
      "func" : lambda reader: [get_dphimax_b2_matched_tp(gm) for gm in reader.genmuons if gm.showered],
    },
    "dphimax_tp_non_showering_muon" : {
      "type" : "distribution",
      "histo" : r.TH1D("dphimax_tp_non_showering_muon", r';Max #Delta#phi TP non-showering muon; Events', 60, 0 , 0.3),
      "func" : lambda reader: [get_dphimax_b2_matched_tp(gm) for gm in reader.genmuons if not gm.showered],
    },
    "dphi_seg_showering_muon" : {
      "type" : "distribution",
      "histo" : r.TH1D("dphi_showering_muon", r';#Delta#phi Seg showering muon; Events', 60, 0 , 0.3),
      "func" : lambda reader: [get_dphi_b2_matched_segments(gm) for gm in reader.genmuons if gm.showered],
    },
    "dphi_seg_non_showering_muon" : {
      "type" : "distribution",
      "histo" : r.TH1D("dphi_non_showering_muon", r';#Delta#phi Seg non-showering muon; Events', 60, 0 , 0.3),
      "func" : lambda reader: [get_dphi_b2_matched_segments(gm) for gm in reader.genmuons if not gm.showered],
    },
    "dphi_tp_showering_muon" : {
      "type" : "distribution",
      "histo" : r.TH1D("dphi_tp_showering_muon", r';#Delta#phi TP showering muon; Events', 60, 0 , 0.3),
      "func" : lambda reader: [get_dphi_b2_matched_tp(gm) for gm in reader.genmuons if gm.showered],
    },
    "dphi_tp_non_showering_muon" : {
      "type" : "distribution",
      "histo" : r.TH1D("dphi_tp_non_showering_muon", r';#Delta#phi TP non-showering muon; Events', 60, 0 , 0.3),
      "func" : lambda reader: [get_dphi_b2_matched_tp(gm) for gm in reader.genmuons if not gm.showered],
    },
})

