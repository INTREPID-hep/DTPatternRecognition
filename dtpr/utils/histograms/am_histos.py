import ROOT as r
from dtpr.utils.functions import stations, get_best_matches
from functools import partial

# Histograms defined here...
# - seg_eff_MB1
# - seg_eff_MB2
# - seg_eff_MB3
# - seg_eff_MB4
# - AM_rate_goodBX_MB1
# - AM_rate_goodBX_MB2
# - AM_rate_goodBX_MB3
# - AM_rate_goodBX_MB4 
# - AM_rate_allBX_MB1
# - AM_rate_allBX_MB2
# - AM_rate_allBX_MB3
# - AM_rate_allBX_MB4


# Define the histograms container
histos = {}

# --------------------------- AM efficiency ---------------------------

def am_eff_func(reader, station):
    return [seg.wh for seg in get_best_matches( reader, station = station )]

def am_eff_numdef(reader, station):
    return [len(seg.matches) > 0 for seg in get_best_matches( reader, station = station )]

# AM Efficiencies per station
for st in stations:
    histos.update(
        {
            f"seg_eff_MB{st}" :  {  
                "type" : "eff",
                "histoDen" : r.TH1D(f"Eff_MB{st}_AM_total", r';Wheel; Events', 5, -2.5 , 2.5),
                "histoNum" : r.TH1D(f"Eff_MB{st}_AM_num", r';Wheel; Events', 5, -2.5 , 2.5),
                "func"     : partial(am_eff_func, station=st), # These are the values to fill with
                # These are the conditions on whether to fill numerator also. Imitating Jaime's code:
                # https://github.com/jaimeleonh/DTNtuples/blob/unifiedPerf/test/DTNtupleTPGSimAnalyzer_Efficiency.C#L443
                # Basically, for a best matching segment, if there's a bestTP candidate, fill the numerator.
                "numdef"   : partial(am_eff_numdef, station=st), 
            }
        }
    )

# ------------------------------- AM rates -------------------------------

def get_tps_rate(reader, station, goodbx=True):
    return [
        tp.wh
        for tp in reader.filter_particles("tps", st=station)
        if (tp.BX == 0 if goodbx else 1)
    ]
for st in stations:
    histos.update(
        {
            f"AM_rate_goodBX_MB{st}": { # ----- good BX -----
                "type": "distribution",
                "histo": r.TH1D(f"Rate_goodBX_MB{st}_AM", r';Wheel; Events', 5, -2.5, 2.5),
                "func": partial(get_tps_rate, station=st, goodbx=True),
            },
            f"AM_rate_allBX_MB{st}": { # ----- all BX -----
                "type": "distribution",
                "histo": r.TH1D(f"Rate_allBX_MB{st}_AM", r';Wheel; Events', 5, -2.5, 2.5),
                "func": partial(get_tps_rate, station=st, goodbx=False),
            },
        }
    )
