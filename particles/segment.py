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
        return
