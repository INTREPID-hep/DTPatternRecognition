""" Functions to filter events """

def baseline(reader):
    """
    Baseline filter: There must be a generator muon that matches with an offline segment.

    Args:
        reader (object): The reader object containing generator muons.

    Returns:
        bool: True if the filter condition is met, False otherwise.
    """
    genMuons = getattr(reader, "genmuons", [])
    AtLeastOneMuon = len(genMuons) > 0
    
    matches_segment = False
    for gm in genMuons:
        matches_segment = matches_segment or len(getattr(gm, 'matched_segments', [])) > 0
    return AtLeastOneMuon and matches_segment

def removeShower(reader):
    """
    Remove shower filter: Removes TPs that match to segments of a muon that showered.

    Args:
        reader (object): The reader object containing generator muons.

    Returns:
        bool: Always returns True.
    """
    genMuons = getattr(reader, "genmuons", [])
    # Remove TPs that match to segments of a muon that showered
    for igm, gm in enumerate(genMuons):
        matched_segments = getattr(gm, 'matched_segments', [])
        muon_shower = gm.showered
        
        for seg in matched_segments:
            if muon_shower:
                seg.matched_tps = []
    return True

def baseline_plus_hitsc(reader):
    """
    Baseline plus hitsc filter: Removes TPs that match to segments of a muon that showered,
    unless the segment location matches with any shower.

    Args:
        reader (object): The reader object containing generator muons and emushowers.

    Returns:
        bool: Always returns True.
    """
    genMuons = getattr(reader, "genmuons")
    emushowers = getattr(reader, "emushowers")

    shower_locs = [(shower.wh, shower.sc, shower.st) for shower in emushowers]
    # Remove TPs that match to segments of a muon that showered
    for igm, gm in enumerate(genMuons):
        matched_segments = getattr(gm, 'matched_segments', [])
        muon_shower = gm.showered
        
        for seg in matched_segments:
            loc = (seg.wh, seg.sc, seg.st)
            if loc in shower_locs:
                continue
            elif muon_shower:
                seg.matched_tps = []
    return True