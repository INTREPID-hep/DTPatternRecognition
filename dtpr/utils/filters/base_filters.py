""" Functions to filter events """

def no_filter(reader):
    """
    No filter: No filter is applied.

    Args:
        reader (object): The reader object containing generator muons.

    Returns:
        bool: Always returns True.
    """
    return True

def baseline(reader):
    """
    Baseline filter: There must be a generator muon that matches with an offline segment.

    Args:
        reader (object): The reader object containing generator muons.

    Returns:
        bool: True if the filter condition is met, False otherwise.
    """
    genMuons = getattr(reader, "genmuons")
    AtLeastOneMuon = len(genMuons) > 0
    
    matches_segment = False
    for gm in genMuons:
        matches_segment = matches_segment or len(gm.matches) > 0
    return AtLeastOneMuon and matches_segment

def removeShower(reader):
    """
    Remove shower filter: Removes TPs that match to segments of a muon that showered.

    Args:
        reader (object): The reader object containing generator muons.

    Returns:
        bool: Always returns True.
    """
    genMuons = getattr(reader, "genmuons")
    # Remove TPs that match to segments of a muon that showered
    for igm, gm in enumerate(genMuons):
        matched_segments = gm.matches
        muon_shower = gm.did_shower()
        
        for seg in matched_segments:
            if muon_shower:
                seg.matches = []
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
        matched_segments = gm.matches
        muon_shower = gm.did_shower()
        
        for seg in matched_segments:
            loc = (seg.wh, seg.sc, seg.st)
            if loc in shower_locs:
                continue
            elif muon_shower:
                seg.matches = []
    return True