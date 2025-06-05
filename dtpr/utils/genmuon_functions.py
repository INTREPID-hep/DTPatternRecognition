# functions to analyze generator level muons

from dtpr.base import Event, Particle
from dtpr.utils.segment_functions import match_offline_AMtp
from dtpr.utils.functions import append_to_matched_list, get_unique_locs, phiConv
import math
from itertools import combinations

def match_genmuon_offline_segment(gm: Particle, seg: Particle, max_dPhi: float, max_dEta: float):
    """
    Matches a generator muon to an offline segment based on dPhi and dEta criteria.

    :param gm: The generator muon to match.
    :type gm: Particle
    :param seg: The segment to match.
    :type seg: Particle
    :param max_dPhi: The maximum dPhi for matching.
    :type max_dPhi: float
    :param max_dEta: The maximum dEta for matching.
    :type max_dEta: float
    """
    if not hasattr(gm, 'phi') or not hasattr(gm, 'eta'):
        raise AttributeError("Generator muon must have 'phi' and 'eta' attributes.")
    if any([not hasattr(seg, key) for key in ['phi', 'eta', 'st', 'nHits_phi', 'nHits_z']]):
        raise AttributeError("Segment must have 'phi', 'eta', 'st', 'nHits_phi', and 'nHits_z' attributes.")
    st = seg.st
    isMB4 = st == 4
    dphi = abs(math.acos(math.cos(gm.phi - seg.phi)))
    deta = abs(gm.eta - seg.eta)
    matches = (
        (dphi < max_dPhi)
        and (deta < max_dEta)
        and seg.nHits_phi >= 4
        and (seg.nHits_z >= 4 or isMB4)
    )
    if matches:
        append_to_matched_list(gm, 'matched_segments', seg)
        append_to_matched_list(seg, 'matched_genmuons', gm)

def analyze_genmuon_matches(ev: Event):
    """
    Match generator muons to segments and TPs in a broad dPhi/dEta window. After executing this function,
    each genmuon, segment, and TP will have their respective matched lists populated.
    """
    if not hasattr(ev, "segments"):
        raise ValueError(
            "Event does not have 'segments' they are required to analyze matches."
        )
    if not hasattr(ev, "genmuons"):
        raise ValueError(
            "Event does not have 'genmuons' they are required to analyze matches."
        )
    if not hasattr(ev, "tps"):
        raise ValueError(
            "Event does not have 'tps' they are required to analyze matches."
        )

    for gm in ev.genmuons:
        # Match segments to generator muons
        for seg in ev.segments:
            # gm.match_segment(seg, math.pi / 6., 0.8)\
            match_genmuon_offline_segment(gm, seg, 0.1, 0.3)
        # Now re-match with TPs
        for _seg in getattr(gm, 'matched_segments', []):
            for tp in ev.tps:
                match_offline_AMtp(_seg, tp, max_dPhi=0.1)

def analyze_genmuon_showers(ev: Event, method=1):
    """
    Analyze if the generator muon showered based on matched segments.
    """
    if not hasattr(ev, "genmuons"):
        raise ValueError("Event does not have 'genmuons' they are required")

    if method == 1:
        for gm in ev.genmuons:
            matchs= getattr(gm, 'matched_segments', [])
            if len(matchs) != len(get_unique_locs(matchs, ("wh", "sc", "st"))):
                # There's at least two matching segments in the same station for this muon
                gm.showered = True

    if method == 2:
        for gm in ev.genmuons:
            locs = get_unique_locs(getattr(gm, 'matched_segments', []), ("wh", "sc", "st"))
            for wh, sc, st in locs:
                simhits = ev.filter_particles("simhits", wh=wh, sc=sc, st=st)
                if len([simhit for simhit in simhits if abs(simhit.particle_type) == 11]) >= 8:
                    gm.showered = True
                    break

def get_dphi_matched_segments(gm: Particle):
    """
    Compute the dPhi between the generator muon and its matched offline segments.

    :return: The dPhi.
    :rtype: float
    """
    if not getattr(gm, 'matched_segments', []):
        return []
    return [abs(math.acos(math.cos(gm.phi - seg.phi))) for seg in getattr(gm, 'matched_segments', [])]

def get_dphimax_matched_segments(gm: Particle): # it was genmuon.get_max_dphi
    """
    Compute the maximum dPhi between the generator muon and its matched offline segments.

    :return: The maximum dPhi.
    :rtype: float
    """
    return max( get_dphi_matched_segments(gm), default=-99 ) if getattr(gm, 'matched_segments', []) else -99

def get_dphi_b2_matched_segments(gm: Particle):
    """
    Compute the dPhi between pair of the segments that match to the generator muon.

    :return: The dPhi.
    :rtype: list
    """
    if not getattr(gm, 'matched_segments', []):
        return []

    return [
        abs(math.acos(math.cos(seg1.phi - seg2.phi)))
        for seg1, seg2 in combinations(getattr(gm, 'matched_segments', []), 2)
    ]

def get_dphimax_b2_matched_segments(gm: Particle):
    """
    Compute the maximum dPhi of the pair of segments that match to the generator muon.

    :return: The maximum dPhi.
    :rtype: float
    """
    return max(get_dphi_b2_matched_segments(gm), default=-99) if getattr(gm, 'matched_segments', []) else -99

def get_dphi_b2_matched_tp(gm: Particle):
    """
    Compute the dPhi between pair of the TPs that match to the generator muon.
    
    :param gm: The generator muon.
    :type gm: Particle
    :return: The dPhi.
    :rtype: list
    """
    if not gm.matched_tps:
        return []

    return [
        abs(math.acos(math.cos(phiConv(tp1.phi) - phiConv(tp2.phi))))
        for tp1, tp2 in combinations(gm.matched_tps, 2)
    ]

def get_dphimax_b2_matched_tp(gm: Particle):
    """
    Compute the maximum dPhi of the pair of TPs that match to the generator muon.

    :param gm: The generator muon.
    :type gm: Particle
    :return: The maximum dPhi.
    :rtype: float
    """
    return max(get_dphi_b2_matched_tp(gm), default=-99) if gm.matched_tps else -99

def get_max_deta(gm: Particle):
    """
    Compute the maximum dEta of the segments that match to the generator muon.

    :param gm: The generator muon.
    :type gm: Particle
    :return: The maximum dEta.
    :rtype: float
    """
    if not getattr(gm, 'matched_segments', []):
        return -99
    return max([abs(gm.eta - seg.eta) for seg in getattr(gm, 'matched_segments', [])], default=-99)