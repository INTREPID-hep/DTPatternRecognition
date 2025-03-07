from dtpr.utils.functions import init_ntuple_from_config, color_msg
from dtpr.utils.config import RUN_CONFIG
import os
from dtpr.utils.histograms.shower_histos import compute_tpfptnfn


def match_shower_muon(shower, muon):
    """
    Check if the shower and muon match based on their properties.
    """
    shower_loc = (shower.st, shower.sc, shower.wh)
    if shower_loc in muon.matched_segments_stations:
        return True
    return False

def match_shower_shower(shower1, shower2):
    """
    Check if the two showers match based on their locations.ç
    """
    shower1_loc = (shower1.wh, shower1.sc, shower1.st)
    shower2_loc = (shower2.wh, shower2.sc, shower2.st)
    if shower1_loc == shower2_loc:
        return True
    return False

def has_tp(ev):
    if any([typ := compute_tpfptnfn(ev, station=st)[1] == 0 or typ == 3 for st in range(1, 5)]):
        return True
    
    return False


def main():
    inpath = os.path.abspath("/mnt/c/Users/estradadaniel/cernbox/ZprimeToMuMu_M-6000_TuneCP5_14TeV-pythia8/ZprimeToMuMu_M-6000_PU200/250312_131641/0000/")
    # Start of the analysis 
    
    RUN_CONFIG.change_config_file( os.path.abspath("./run_config.yaml"))
    # Create the Ntuple object
    ntuple = init_ntuple_from_config(
        inputFolder=inpath,
        config=RUN_CONFIG,
    )
    evn=100
    for ev in ntuple.events:
        if not ev:
            continue
        if ev.index > evn:
            break

        # if any([(shower.st, shower.sc, shower.wh) in muon.matched_segments_stations for shower in ev.realshowers for muon in ev.genmuons]):
        for shower in ev.fwshowers:
            matched = False
            for realshower in ev.realshowers:
                for muon in ev.genmuons:
                    if match_shower_muon(shower, muon) and match_shower_shower(shower, realshower):
                        matched = True
            
            if matched:
                print(ev.index)
                for st in range(1, 5):
                    print(compute_tpfptnfn(ev, station=st))
                break

        if any([len(muon.matches)>= 4 for muon in ev.genmuons]):
            print("-->", ev.index)
                

    color_msg(f"Done!", color="green")


if __name__ == "__main__":
    main()

# 1
# 8
# 10
# 45
# 46
# 47
# 50
# 58
# 63
# 82
# 84
# 86
# 89
# 91
# 94
# 95

# 23 -- wh0