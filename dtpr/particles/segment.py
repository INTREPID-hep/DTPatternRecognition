import math
from dtpr.particles import Particle

class Segment(Particle):
    """
    A class representing a DT segment.

    Attributes
    ----------
    wh : int
        The wheel number of the segment.
    sc : int
        The sector number of the segment.
    st : int
        The station number of the segment.
    phi : float
        The phi position of the segment.
    eta : float
        The eta position of the segment.
    nHits_phi : int
        The number of hits in the phi direction.
    nHits_z : int
        The number of hits in the z direction.
    t0_phi : float
        The time of the segment in the phi direction.
    pos_locx_sl1 : float
        The local x position of the segment in superlayer 1.
    pos_locx_sl3 : float
        The local x position of the segment in superlayer 3.
    matches : list
        The list of trigger primitives matched to the segment.
    """

    def __init__(self, index, ev=None, **kwargs):
        """
        Initialize a Segment instance.

        A Segment can be initialized either by providing the root event entry (``ev``) or by passing each parameter individually.

        :param index: The index of the segment.
        :type index: int
        :param ev: The TTree event entry containing event data.
        :param kwargs: Additional attributes to set explicitly.
        """
        self.wh = kwargs.pop("wh", None)
        self.sc = kwargs.pop("sc", None)
        self.st = kwargs.pop("st", None)
        self.phi = kwargs.pop("phi", None)
        self.eta = kwargs.pop("eta", None)
        self.nHits_phi = kwargs.pop("nHits_phi", None)
        self.nHits_z = kwargs.pop("nHits_z", None)
        self.t0_phi = kwargs.pop("t0_phi", None)
        self.pos_locx_sl1 = kwargs.pop("pos_locx_sl1", None)
        self.pos_locx_sl3 = kwargs.pop("pos_locx_sl3", None)
        self.matches = []

        super().__init__(index, ev, **kwargs)

    def _init_from_ev(self, ev):
        """
        Properties taken from TBranches: {seg_wheel, seg_sector, seg_station, seg_posGlb_phi, 
        seg_posGlb_eta, seg_phi_nHits, seg_z_nHits, seg_phi_t0, seg_posLoc_x_SL1, seg_posLoc_x_SL3}
        """
        self.wh = ev.seg_wheel[self.index]
        self.sc = ev.seg_sector[self.index]
        self.st = ev.seg_station[self.index]
        self.phi = ev.seg_posGlb_phi[self.index]
        self.eta = ev.seg_posGlb_eta[self.index]
        self.nHits_phi = ev.seg_phi_nHits[self.index]
        self.nHits_z = ev.seg_z_nHits[self.index]
        self.t0_phi = ev.seg_phi_t0[self.index]
        self.pos_locx_sl1 = ev.seg_posLoc_x_SL1[self.index]
        self.pos_locx_sl3 = ev.seg_posLoc_x_SL3[self.index]

    def _add_match(self, tp):
        """
        Add a match to the segment.

        :param tp: The trigger primitive to match.
        :type tp: Ph2TP
        """
        if tp not in self.matches:
            self.matches.append(tp)

    def match_offline_to_AM(self, tp, max_dPhi=0.1):
        """
        Match the segment to a trigger primitive based on dPhi.

        :param tp: The trigger primitive to match.
        :type tp: Ph2TP
        :param max_dPhi: The maximum dPhi for matching.
        :type max_dPhi: float
        """
        # Fix issue with sector numbering
        seg_sc = self.sc
        if seg_sc == 13:
            seg_sc = 4
        elif seg_sc == 14:
            seg_sc = 10

        # Match only if TP and segment are in the same chamber
        if tp.wh == self.wh and tp.sc == seg_sc and tp.st == self.st:
            # In this case, they are in the same chamber: match dPhi
            # -- Use a conversion factor to express phi in radians
            trigGlbPhi = tp.phi / tp.phires_conv + math.pi / 6 * (tp.sc - 1)
            dphi = abs(math.acos(math.cos(self.phi - trigGlbPhi)))
            matches = dphi < max_dPhi and tp.BX == 0
            if matches:
                self._add_match(tp)


if __name__ == "__main__":
    seg = Segment(
        0, wh=1, sc=1, st=1, phi=0.1, eta=0.2, nHits_phi=3, nHits_z=2, t0_phi=0.3
    )
    print(seg)
