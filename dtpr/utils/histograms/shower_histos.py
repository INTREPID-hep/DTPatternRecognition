import ROOT as r
from functools import partial
import warnings
from dtpr.utils.functions import stations, get_unique_locs, get_best_matches

# Histograms defined here...
# ----- for DtNtuple -----
# - shower_tpfptnfn_MB1
# - shower_tpfptnfn_MB2
# - shower_tpfptnfn_MB3
# - shower_tpfptnfn_MB4
# - fwshower_eff_MB1
# - fwshower_eff_MB2
# - fwshower_eff_MB3
# - fwshower_eff_MB4
# - fwshower_rate_goodBX_MB1
# - fwshower_rate_goodBX_MB2
# - fwshower_rate_goodBX_MB3
# - fwshower_rate_goodBX_MB4
# - fwshower_rate_allBX_MB1
# - fwshower_rate_allBX_MB2
# - fwshower_rate_allBX_MB3
# - fwshower_rate_allBX_MB4
# ---- for G4DtNtuple ----
# - shower_tpfptnfn_g4
# ----- for real showers -----
# - realshower_type_dist_MB1
# - realshower_type_dist_MB2
# - realshower_type_dist_MB3
# - realshower_type_dist_MB4


# Define the histograms container
histos = {}


def get_locs_to_check(reader, station=1, opt=1, by_sl=False):
    loc_ids = ["wh", "sc", "st", "sl"] if by_sl else ["wh", "sc", "st"]
    if opt == 3:
        indexs = get_unique_locs(particles=reader.filter_particles("digis", st=station), loc_ids=loc_ids)
        return indexs

    fwshowers_locs = get_unique_locs(particles=reader.filter_particles("fwshowers", st=station), loc_ids=loc_ids)
    realshowers_locs = get_unique_locs(particles=reader.filter_particles("realshowers", st=station), loc_ids=loc_ids)

    if opt == 1: #every chamber with showers, and traversed chambers
        _gm_seg_locs = get_unique_locs(particles=[seg for seg in get_best_matches(reader, station=station) if len(seg.matches) > 0], loc_ids=["wh", "sc", "st"])
        if by_sl:
            gm_seg_locs = set()
            for wh, sc, st in _gm_seg_locs:
                gm_seg_locs.add((wh, sc, st, 1)) # sl 1
                gm_seg_locs.add((wh, sc, st, 3)) # sl 3
        else:
            gm_seg_locs = _gm_seg_locs
        indexs = fwshowers_locs.union(realshowers_locs).union(gm_seg_locs)

    if opt == 2: #every chamber which any shower
        indexs = fwshowers_locs.union(realshowers_locs)

    return indexs

def compute_tpfptnfn(reader, station=1, opt=1, by_sl=False):
    """
    Classifies true positives, false positives, true negatives, and false negatives based on fwshowers and realshowers.

    Args:
        reader (object): The reader object containing fwshowers and realshowers.

    Returns:
        tuple: A tuple containing the wheel number and a classification code:
            0 - True Positive (TP)
            1 - False Positive (FP)
            2 - True Negative (TN)
            3 - False Negative (FN)
    """
    output = []

    indexs = get_locs_to_check(reader, station=station, opt=opt, by_sl=by_sl)

    # with open("output_tpfptnfn.txt", "a") as f:
    for index in indexs:
        if by_sl:
            wh, sc, st, sl = index
            kargs = {"wh": wh, "sc": sc, "st": st, "sl": sl}
        else:
            wh, sc, st = index
            kargs = {"wh": wh, "sc": sc, "st": st}

        real_showers = reader.filter_particles("realshowers", **kargs)
        fwshowers = reader.filter_particles("fwshowers", **kargs)

        if real_showers:
            if fwshowers:
                # f.write(f"{reader.iev} {" ".join([str(val) for val in kargs.values()])} tp\n")
                output.append((wh, 0)) # true positive
            else:
                # f.write(f"{reader.iev} {" ".join([str(val) for val in kargs.values()])} fn\n")
                output.append((wh, 3)) # false negative
        else:
            if fwshowers:
                # f.write(f"{reader.iev} {" ".join([str(val) for val in kargs.values()])} fp\n")
                output.append((wh, 1)) # false positive
            else:
                # f.write(f"{reader.iev} {" ".join([str(val) for val in kargs.values()])} tn\n")
                output.append((wh, 2)) # true negative

    return output

def tpfptnfn_func(reader, station=1, opt=1, by_sl=False):
    return [bin for bin in compute_tpfptnfn(reader, station=station, opt=opt, by_sl=by_sl)]

def shower_eff_func(reader, station=1, opt=1, by_sl=False):
    return [wh for wh, *_a in get_locs_to_check(reader, station=station, opt=opt, by_sl=by_sl)]

def shower_eff_numdef(reader, station=1, opt=1, by_sl=False):
    return [(cls == 0 or cls == 2) for _, cls in compute_tpfptnfn(reader, station=station, opt=opt, by_sl=by_sl)]

for st in stations:
    histos.update({ # conf maps
        "shower_tpfptnfn_MB" + str(st): {
        "type": "distribution2d",
        "histo": r.TH2D(f"shower_tpfptnfn_MB{st}", r';Wheel; [TP, FP, TN, FN]', 5, -2.5, 2.5, 4, 0, 4),
        "func": partial(tpfptnfn_func, station=st),
        }, # ----- efficiency
        "fwshower_eff_MB" + str(st):{ 
            "type": "eff",
            "histoDen" : r.TH1D(f"Fwshower_eff_MB{st}_total", r';Wheel; Events', 5, -2.5 , 2.5),
            "histoNum" : r.TH1D(f"Fwshower_eff_MB{st}_num", r';Wheel; Events', 5, -2.5 , 2.5),
            "func"     : partial(shower_eff_func, station=st),
            "numdef"   : partial(shower_eff_numdef, station=st),
        },
    })

histos.update({ # conf map for G4DtNtuple
    "shower_tpfptnfn_g4": {
        "type": "distribution2d",
        "histo": r.TH2D("shower_tpfptnfn_g4", r';Wheel; [TP, FP, TN, FN]', 1, -2.5, 1.5, 4, 0, 4),
        "func": partial(tpfptnfn_func, station=1, opt=3),
    },
})

# ------------------------------ Shower rates -------------------------------

def get_showers_rate(reader, station, goodbx=True):
    return [
        shower
        for shower in reader.filter_particles("fwshowers", st=station)
        if (shower.BX == 20 if goodbx else 1)
    ]

for st in stations:
    histos.update({
        f"fwshower_rate_goodBX_MB{st}": { # ----- good BX -----
            "type": "distribution",
            "histo": r.TH1D(f"Rate_goodBX_MB{st}_FwShower", r';Wheel; Events', 5, -2.5, 2.5),
            "func": lambda reader, st=st: [
                shower.wh for shower in get_showers_rate(reader, station=st, goodbx=True)
            ],
        },
        f"fwshower_rate_goodBX_MB{st}": { # ----- all BX -----
            "type": "distribution",
            "histo": r.TH1D(f"Rate_allBX_MB{st}_FwShower", r';Wheel; Events', 5, -2.5, 2.5),
            "func": lambda reader, st=st: [
                shower.wh for shower in get_showers_rate(reader, station=st, goodbx=False)
            ],
        },
    })


# ---------------- real showers distributions ----------------
for st in stations:
    histos.update({
        f"realshower_type_dist_MB{st}": {
            "type": "distribution2d",
            "histo": r.TH2D(f"realshower_type_dist_MB{st}", r';Wheel; Type', 5, -2.5, 2.5, 4, 1, 5),
            "func": lambda reader, st=st: [
                (shower.wh, shower.shower_type) for shower in reader.filter_particles("realshowers", st=st)
            ],
        },
    })