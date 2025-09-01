import ROOT as r

# Histograms defined here...
# - LeadingMuon_pt
# - LeadingMuon_eta
# - SubLeadingMuon_pt
# - SubLeadingMuon_eta
# - muon_DR

histos = {}

histos.update(
    {
        # --- Leading muon properties
        "LeadingMuon_pt": {
            "type": "distribution",
            "histo": r.TH1D("LeadingMuon_pt", r";Leading muon p_T; Events", 20, 0, 1000),
            "func": lambda reader: reader.genmuons[0].pt,
        },
        "LeadingMuon_eta": {
            "type": "distribution",
            "histo": r.TH1D("LeadingMuon_eta", r";Leading muon #eta; Events", 10, -3, 3),
            "func": lambda reader: reader.genmuons[0].eta,
        },
        # --- Subleading muon properties
        "SubLeadingMuon_pt": {
            "type": "distribution",
            "histo": r.TH1D("SubLeadingMuon_pt", r";Subleading muon p_T; Events", 20, 0, 1000),
            "func": lambda reader: reader.genmuons[1].pt,
        },
        "SubLeadingMuon_eta": {
            "type": "distribution",
            "histo": r.TH1D("SubLeadingMuon_eta", r";Subleading muon #eta; Events", 10, -3, 3),
            "func": lambda reader: reader.genmuons[1].eta,
        },
        # --- Muon dR
        "muon_DR": {
            "type": "distribution",
            "histo": r.TH1D("muon_DR", r";#DeltaR both muons; Events", 20, 1, 6),
            "func": lambda reader: reader.dR,
        },
    }
)
