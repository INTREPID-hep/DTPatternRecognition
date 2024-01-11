import math
import re
from utils.functions import color_msg

class gen_muon(object):
    def __init__(self, idm, pt, eta, phi, charge):
        self.index = idm
        self.pt = pt
        self.eta = eta
        self.phi = phi
        self.charge = charge 
        
        # Attributes
        self.matches = []
        self.stations = []
        self.sectors = []
        self.wheels = []
        self.matched_segments_stations = []
        self.showered = False
        return

    def add_match(self, seg):
        #if seg.st not in self.stations: self.stations.append( seg.st )
        #if seg.sc not in self.sectors: self.sectors.append( seg.sc )
        #if seg.wh not in self.wheels: self.wheels.append( seg.wh )
        if seg not in self.matches: self.matches.append( seg )
        location = (seg.st, seg.sc, seg.wh) 
        if location in self.matched_segments_stations:
            # There's at least two matching segments in the same station for this muon
            self.showered = True
        self.matched_segments_stations.append( location )
        
    def match_segment(self, seg, max_dPhi, max_dEta):
        """ Check which matching criteria does the muon satisfy with a given segment """
        st = seg.st
        isMB4 = (st == 4)            
        dphi = abs(math.acos( math.cos(self.phi - seg.phi) ))
        deta = abs(self.eta - seg.eta)            
        matches = (dphi < max_dPhi) and (deta < max_dEta) and seg.nHits_phi >= 4 and (seg.nHits_z >= 4 or isMB4)
        if matches: 
            self.add_match(seg)
    
    def get_max_dphi(self):
        """ Computes the maximum dPhi of the segments that match to the generator muon."""
        if self.matches == []: 
            return -99
        return max( [abs(math.acos( math.cos(self.phi - seg.phi))) for seg in self.matches] )
    
    def get_max_deta(self):
        """ Computes the maximum dPhi of the segments that match to the generator muon."""
        if self.matches == []: 
            return -99
        return max( [abs(self.eta - seg.eta) for seg in self.matches] )
    
    def did_shower(self):
        return self.showered
        
    def summarize(self, indentLevel):
        """ Method to summarize the properties of a genmuon object """
        color_msg( f"GenPart Idx: {self.index}", indentLevel = indentLevel)
        color_msg( f"pT: {self.pt:.2f} GeV", indentLevel = indentLevel)
        color_msg( f"Eta: {self.eta:.2f}", indentLevel = indentLevel)
        color_msg( f"Phi: {self.phi:.2f}", indentLevel = indentLevel)
        color_msg( f"Matched segments indices: {[seg.index for seg in self.matches]}", indentLevel = indentLevel)
        color_msg( f"Matched segments location: {[(seg.st, seg.sc, seg.wh) for seg in self.matches]}", indentLevel = indentLevel)
        color_msg( f"Stations traversed: {self.matched_segments_stations}", indentLevel = indentLevel)   