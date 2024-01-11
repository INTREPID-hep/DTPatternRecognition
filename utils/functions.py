""" Miscelaneous """
import math
from particles.segment import segment 

def color_msg(msg, color = "none", indentLevel = 0):
    """ Prints a message with ANSI coding so it can be printout with colors """
    codes = {
        "none" : "0m",
        "green" : "1;32m",
        "red" : "1;31m",
        "blue" : "1;34m",
        "yellow" : "1;35m"
    }

    indentStr = ""
    if indentLevel == 0: indentStr = ">>"
    if indentLevel == 1: indentStr = "+"
    if indentLevel == 2: indentStr = "*"
    if indentLevel == 3: indentStr = "-->"

    
    print("\033[%s%s %s \033[0m"%(codes[color], "  "*indentLevel + indentStr, msg))
    return

def get_best_matches( reader, station = 1 ):
    """ Return the bin for the best matching segments of each generator muon """
    # Fill with dummy segments   
    genmuons = reader.genmuons
    bestMatches = [ None for igm in range(len(genmuons)) ]

    # This is what's done in Jaime's code: https://github.com/jaimeleonh/DTNtuples/blob/unifiedPerf/test/DTNtupleTPGSimAnalyzer_Efficiency.C#L181-L208
    # Basically: get the best matching segment to a generator muon per MB chamber

    #color_msg(f"[FUNCTIONS::GET_BEST_MATCHES] Debugging with station {station}", color = "red", indentLevel = 0)
    for igm, gm in enumerate(genmuons):
        #color_msg(f"[FUNCTIONS::GET_BEST_MATCHES] igm {igm}", indentLevel = 1)
        #gm.summarize(indentLevel = 2)
        for bestMatch in gm.matches:
            if bestMatch.st == station:
                bestMatches[ igm ] =  bestMatch
            
    # Remove those that are None which are simply dummy values
    bestMatches = filter( lambda key: key is not None, bestMatches )
    return bestMatches

def deltaPhi(phi1, phi2):
    res = phi1 - phi2
    while res > math.pi: res -= 2*math.pi
    while res <= -math.pi: res += 2*math.pi
    return res

def deltaR(p1, p2):
    dEta = abs(p1.eta-p2.eta)
    dPhi = deltaPhi(p1.phi, p2.phi)
    return math.sqrt(dEta*dEta + dPhi*dPhi)

