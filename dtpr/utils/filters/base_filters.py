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
