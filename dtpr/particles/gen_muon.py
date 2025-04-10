import math
from dtpr.utils.functions import  phiConv
from dtpr.base import Particle

class GenMuon(Particle):
    """
    A class representing a Generator Level Muon.

    Attributes
    ----------
    pt : float
        The transverse momentum of the generator muon.
    eta : float
        The pseudorapidity of the generator muon.
    phi : float
        The azimuthal angle of the generator muon.
    charge : int
        The charge of the generator muon.
    matches : list
        The list of segments that match to the generator muon.
    matched_segments_stations : list
        The list of stations of the segments that match to the generator muon.
    showered : bool
        True if the generator muon showered, False otherwise.
    """
    def __init__(self, index, ev=None, branches=None, **kwargs):
        """
        Initialize a Generator Level Muon instance.

        A Generator Level Muon can be initialized either by providing the root event entry (``ev``) or by passing each parameter individually.

        :param index: The index of the generator muon.
        :type index: int
        :param ev: The TTree event entry containing event data.
        :param kwargs: Additional attributes to set explicitly.
        """
        # set explicit attributes to guarantee that they are set
        self.pt = kwargs.pop("pt", None)
        self.eta = kwargs.pop("eta", None)
        self.phi = kwargs.pop("phi", None)
        self.charge = kwargs.pop("charge", None)
        self.matches = []
        self.matched_segments_stations = []
        self.showered = False

        super().__init__(index, ev, branches, **kwargs)

    def add_match(self, seg):
        """
        Add a matched segment to the muon.

        :param seg: The segment to add.
        :type seg: Segment
        """
        if seg not in self.matches:
            self.matches.append(seg)
        location = (seg.st, seg.sc, seg.wh)
        self.matched_segments_stations.append(location)

    def match_segment(self, seg, max_dPhi, max_dEta):
        """
        Check which matching criteria the muon satisfies with a given segment.

        :param seg: The segment to match.
        :type seg: Segment
        :param max_dPhi: The maximum dPhi for matching.
        :type max_dPhi: float
        :param max_dEta: The maximum dEta for matching.
        :type max_dEta: float
        """
        st = seg.st
        isMB4 = st == 4
        dphi = abs(math.acos(math.cos(self.phi - seg.phi)))
        deta = abs(self.eta - seg.eta)
        matches = (
            (dphi < max_dPhi)
            and (deta < max_dEta)
            and seg.nHits_phi >= 4
            and (seg.nHits_z >= 4 or isMB4)
        )
        if matches:
            self.add_match(seg)

    def get_max_dphi(self):
        """
        Compute the maximum dPhi of the segments that match to the generator muon.

        :return: The maximum dPhi.
        :rtype: float
        """
        if self.matches == []:
            return -99
        return max(
            [abs(math.acos(math.cos(self.phi - seg.phi))) for seg in self.matches]
        )

    def get_dphimax_segments(self):
        """
        Compute the maximum dPhi of the segments of two adjacent stations that match to the generator muon.

        :return: The maximum dPhi.
        :rtype: float
        """
        if self.matches == []:
            return -99

        dphi = []
        for seg1 in self.matches:
            for seg2 in self.matches:
                # ignore the same segment or any segment on the same chamber
                if seg1.st == seg2.st:
                    continue
                dphi.append(abs(math.acos(math.cos(seg1.phi - seg2.phi))))
        if dphi == []:
            return -99
        else:
            return max(dphi)

    def get_dphimax_tp(self):
        """
        Compute the maximum dPhi of the TPs of two adjacent stations that match to the generator muon.

        :return: The maximum dPhi.
        :rtype: float
        """
        if self.matches == []:
            return -99

        dphi = []
        for seg1 in self.matches:
            phi_tp1 = [tp.phi for tp in seg1.matches]
            for seg2 in self.matches:
                # ignore the same segment or any segment on the same chamber
                if seg1.st == seg2.st:
                    continue
                if seg1.sc != seg2.sc:
                    continue
                phi_tp2 = [tp.phi for tp in seg2.matches]
                if phi_tp1 == [] or phi_tp2 == []:
                    continue
                dphi.append(
                    max(
                        [
                            abs(math.acos(math.cos(phiConv(phi1) - phiConv(phi2))))
                            for phi1 in phi_tp1
                            for phi2 in phi_tp2
                        ]
                    )
                )

        if dphi == []:
            return -99
        else:
            return dphi

    def get_dphi_segments(self):
        """
        Compute the dPhi of the segments of two adjacent stations that match to the generator muon.

        :return: A list of dPhi values.
        :rtype: list
        """
        if self.matches == []:
            return -99

        dphi = []
        for seg1 in self.matches:
            for seg2 in self.matches:
                # ignore the same segment or any segment on the same chamber
                if seg1.st == seg2.st:
                    continue
                dphi.append(abs(math.acos(math.cos(seg1.phi - seg2.phi))))

        if dphi == []:
            return -99
        else:
            return dphi

    def get_dphi_tp(self):
        """
        Compute the dPhi of the TPs of two adjacent stations that match to the generator muon.

        :return: A list of dPhi values.
        :rtype: list
        """
        if self.matches == []:
            return -99

        dphi = []
        for seg1 in self.matches:
            phi_tp1 = [tp.phi for tp in seg1.matches]
            for seg2 in self.matches:
                # ignore the same segment or any segment on the same chamber
                if seg1.st == seg2.st:
                    continue
                if seg1.sc != seg2.sc:
                    continue
                phi_tp2 = [tp.phi for tp in seg2.matches]
                if phi_tp1 == [] or phi_tp2 == []:
                    continue
                dphi.append(
                    [
                        abs(math.acos(math.cos(phiConv(phi1) - phiConv(phi2))))
                        for phi1 in phi_tp1
                        for phi2 in phi_tp2
                    ]
                )

        if dphi == []:
            return -99
        else:
            return dphi

    def get_max_deta(self):
        """
        Compute the maximum dEta of the segments that match to the generator muon.

        :return: The maximum dEta.
        :rtype: float
        """
        if self.matches == []:
            return -99
        return max([abs(self.eta - seg.eta) for seg in self.matches])

    def did_shower(self):
        """
        Check if the muon showered.

        :return: True if the muon showered, False otherwise.
        :rtype: bool
        """
        return self.showered
