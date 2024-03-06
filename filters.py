""" Functions to filter events """
from utils.functions import color_msg
def baseline( reader ):
    """ There must be a generator muon that matches with an offline segment """
    genMuons = getattr(reader, "genmuons")
    AtLeastOneMuon = len(genMuons) > 0
    
    matches_segment = False
    for gm in genMuons:
        matches_segment = matches_segment or len(gm.matches) > 0
    return AtLeastOneMuon and matches_segment

def removeShower( reader ):
    genMuons = getattr(reader, "genmuons")
    # Remove TPs that match to segments of a muon that showered
    for igm, gm in enumerate(genMuons):
        #color_msg(f"[REMOVESHOWER::DEBUG] Muon {igm}", "red", indentLevel = 0)
        matched_segments = gm.matches
        muon_shower = gm.did_shower()
        
        #color_msg(f"[REMOVESHOWER::DEBUG] Before checking shower", "blue", indentLevel = 1)
        #gm.summarize( indentLevel = 2 )
        for seg in matched_segments:
            # If the muon has showered remove all TPs that match to the segment
        #    print(igm, seg.matches)
            if muon_shower:
                seg.matches = []
        
        #color_msg(f"[REMOVESHOWER::DEBUG] After checking shower", "blue", indentLevel = 1)
        #gm.summarize( indentLevel = 2 )
        #for seg in matched_segments:
        #    print(igm, seg.matches)
    return True