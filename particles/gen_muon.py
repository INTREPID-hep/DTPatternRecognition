import math

class gen_muon(object):
    def __init__(self, idm, pt, eta, phi, charge):
        self.idm = idm
        self.pt = pt
        self.eta = eta
        self.phi = phi
        self.charge = charge    

        self.matches = []
        return
    
    def match_to_segment_AM(self, seg, isMB4):
        """ This method maps the generated muon to a segment. """
        dphi = abs(math.acos( math.cos(self.phi - seg.phi) ))
        deta = abs(self.eta - seg.eta)
        if (
            dphi < 0.1 and 
            deta < 0.3 and 
            seg.nHits_phi >= 4 and
            (seg.nHits_z >= 4 or isMB4)
            ):
            st = seg.st
            self.matches.append(seg)
            return True
        return False
    
    def match_to_segment_Filter(self, seg, isMB4):
        """ This method maps the generated muon to a segment. """
        maxDPhi = 1.2 # From cuenta la vieja (0.5 * atan( [(8*1.3 + 28.6) / (avg_nDriftCells * 4.2)] ))
        maxDEta = 0.3 # Approximated from : https://cds.cern.ch/record/2705998/plots
        dphi = abs(math.acos( math.cos(self.phi - seg.phi) ))
        deta = abs(self.eta - seg.eta)
        
        matches = (dphi < maxDPhi) and (deta < maxDEta) and seg.nHits_phi >= 4 and (seg.nHits_z >= 4 or isMB4)
        if (matches):
            self.matches.append(seg)
        return matches
    
    def get_max_dphi(self):
        """ Computes the maximum dPhi of the segments that match to the generator muon"""
        return max( [abs(math.acos( math.cos(self.phi - seg.phi))) for seg in self.matches] )
    
    def get_max_deta(self):
        """ Computes the maximum dPhi of the segments that match to the generator muon"""
        return max( [abs(self.eta - seg.eta) for seg in self.matches] )