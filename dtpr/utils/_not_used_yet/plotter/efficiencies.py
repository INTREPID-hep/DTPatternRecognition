efficiencies = {
    "seg_eff_MB1" :  {
        "type" : "efficiency",
        "histo" : {
            "name":"Eff_MB1", 
            "title": ';Wheel; Efficiency'}, 
        "denom" : "Eff_MB1_total",
        "num": "Eff_MB1_AM_matched",
    },
    "seg_eff_MB2" :  {
        "type" : "efficiency",
        "histo" : {"name":"Eff_MB2", "title": ';Wheel; Efficiency'},
        "denom" : "Eff_MB2_total",
        "num": "Eff_MB2_AM_matched",
    },
    "seg_eff_MB3" :  {
        "type" : "efficiency",
        "histo" : {"name":"Eff_MB3", "title": ';Wheel; Efficiency'},
        "denom" : "Eff_MB3_total",
        "num": "Eff_MB3_AM_matched",
    },
    "seg_eff_MB4" :  {
        "type" : "efficiency",
        "histo" :  {"name":"Eff_MB4", "title": ';Wheel; Efficiency'},
        "denom" : "Eff_MB4_total",
        "num": "Eff_MB4_AM_matched",
    },
    "shower_eff_muon_pt" :  {
        "type" : "efficiency",
        "histo" : {"name":"Shower_eff_muon_pt", "title": ';p_{T} [GeV]; Efficiency'},
        "denom" : "Shower_eff_muon_pt_total",
        "num": "Shower_eff_muon_pt_matched",
    },
    "shower_eff_muon_eta" :  {
        "type" : "efficiency",
        "histo" : {"name":"Shower_eff_muon_eta", "title": ';#eta; Efficiency'},
        "denom" : "Shower_eff_muon_eta_total",
        "num": "Shower_eff_muon_eta_matched",
    },
    "ratio_dphimax_tp" : {
        "type": "ratio",
        "histo" : {"name":"ratio_dphimax_tp", "title": ';#Delta#phi_{max} TP; Ratio (shower / no shower)'},
        "denom" : "dphimax_tp_non_showering_muon",
        "num": "dphimax_tp_showering_muon",
    },
    "ratio_dphimax_seg" : {
        "type": "ratio",
        "histo" : {"name":"ratio_dphimax_seg", "title": ';#Delta#phi_{max} Seg; Ratio (shower / no shower)'},
        "denom" : "dphimax_non_showering_muon",
        "num": "dphimax_showering_muon",
    },
    "ratio_dphi_tp" : {
        "type": "ratio",
        "histo" : {"name":"ratio_dphi_tp", "title": ';#Delta#phi TP; Ratio (shower / no shower)'},
        "denom" : "dphi_tp_non_showering_muon",
        "num": "dphi_tp_showering_muon",
    },
    "ratio_dphi_seg" : {
        "type": "ratio",
        "histo" : {"name":"ratio_dphi_seg", "title": ';#Delta#phi Seg; Ratio (shower / no shower)'},
        "denom" : "dphi_non_showering_muon",
        "num": "dphi_showering_muon",
    },    
}
