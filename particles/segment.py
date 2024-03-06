import math

class segment(object):
    def __init__(self, ev, iseg):
        self.index = iseg
        self.wh = ev.seg_wheel[iseg]
        self.sc = ev.seg_sector[iseg]
        self.st = ev.seg_station[iseg]
        self.phi = ev.seg_posGlb_phi[iseg]
        self.eta = ev.seg_posGlb_eta[iseg]
        self.nHits_phi = ev.seg_phi_nHits[iseg]
        self.nHits_z = ev.seg_z_nHits[iseg]
        self.t0_phi = ev.seg_phi_t0[iseg]
        
        self.matches = []

    
    def add_match(self, tp):
        if tp not in self.matches: self.matches.append(tp) 
    
    def match_offline_to_AM(self, tp, max_dPhi = 0.1):
        # Fix issue with sector numbering
        tp_wh = tp.wh
        tp_sc = tp.sc
        tp_st = tp.st
        
        seg_wh = self.wh
        seg_sc = self.sc
        if   self.sc == 13: seg_sc = 4
        elif self.sc == 14: seg_sc = 10
        seg_st = self.st
        
        # Match only if TP and segment are in the same chamber
        if (tp_wh == seg_wh and tp_sc == seg_sc and tp_st == seg_st): 
            # In this case, they are in the same chamber: match dPhi
            # -- Use a conversion factor to express phi in radians
            trigGlbPhi = tp.phi/tp.phires_conv + math.pi / 6 * (tp_sc - 1)
            dphi = abs(math.acos(math.cos(self.phi - trigGlbPhi)))
            matches = (dphi < max_dPhi and tp.BX == 0)
            if matches:
                self.add_match(tp)