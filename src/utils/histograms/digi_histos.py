import ROOT as r
from functools import partial
from numpy import mean
from src.utils.functions import stations, wheels, sectors

# Histograms defined here...
# - digi_w_gmts_MB1
# - digi_w_gmts_MB2
# - digi_w_gmts_MB3
# - digi_w_gmts_MB4
# - digi_w_gmtns_MB1
# - digi_w_gmtns_MB2
# - digi_w_gmtns_MB3
# - digi_w_gmtns_MB4
# - digi_wd_gmts_MB1
# - digi_wd_gmts_MB2
# - digi_wd_gmts_MB3
# - digi_wd_gmts_MB4
# - digi_wd_gmtns_MB1
# - digi_wd_gmtns_MB2
# - digi_wd_gmtns_MB3
# - digi_wd_gmtns_MB4
# - digi_wl_gmts_MB1
# - digi_wl_gmts_MB2
# - digi_wl_gmts_MB3
# - digi_wl_gmts_MB4
# - digi_wl3d_gmts_MB1
# - digi_wl3d_gmts_MB2
# - digi_wl3d_gmts_MB3
# - digi_wl3d_gmts_MB4
# - digi_wl_gmtns_MB1
# - digi_wl_gmtns_MB2
# - digi_wl_gmtns_MB3
# - digi_wl_gmtns_MB4
# - digi_wl3d_gmtns_MB1
# - digi_wl3d_gmtns_MB2
# - digi_wl3d_gmtns_MB3
# - digi_wl3d_gmtns_MB4

# Define the histograms container 
histos = {}

def get_digis_distribution(
    reader, station=1, _4showereds=False, distribution_type="mean", group_by_sl=False
):
    """
    Calculates the distribution of digis for a given station and showered status.

    Args:
        reader (object): The reader object containing digis.
        station (int): The station number. Default is 1.
        _4showereds (bool): The showered status. Default is False.
        distribution_type (str): The type of distribution to calculate ("mean" or "length"). Default is "mean".
        group_by_sl (bool): If True, groups digis by superlayer. Default is False.

    Returns:
        list: The calculated distribution data.
    """
    hasShowereds = any(genmuon.showered for genmuon in reader.genmuons)
    data = []

    if hasShowereds == _4showereds:
        # Iterate over all wheels
        for wh in wheels:
            # Iterate over all sectors
            for sc in sectors:
                # Iterate over all superlayers if needed
                sl_range = [1, 2, 3] if group_by_sl else [None]

                for sl in sl_range:
                    # Filter digis for the current wheel, sector, and station (and superlayer if it's the case)
                    if sl:
                        digis = reader.filter_particles(
                            "digis", wh=wh, sc=sc, st=station, sl=sl
                        )
                    else:
                        digis = reader.filter_particles(
                            "digis", wh=wh, sc=sc, st=station
                        )

                    if not digis:
                        continue
                    wires = [digi.w for digi in digis]
                    if distribution_type == "mean":
                        w_mean = mean(wires)
                        data += (
                            [(wh, sl, w - w_mean) for w in wires]
                            if group_by_sl
                            else [(wh, w - w_mean) for w in wires]
                        )
                    elif distribution_type == "length":
                        w_min, w_max = min(wires), max(wires)
                        data.append(
                            (wh, sl, w_max - w_min)
                            if group_by_sl
                            else (wh, w_max - w_min)
                        )

    return data

def digi_w_ocupancy(reader, station, _4showereds=False):
    hasShowereds = any(genmuon.showered for genmuon in reader.genmuons)
    return [(digi.wh, digi.w) for digi in reader.filter_particles("digis", st=station) if hasShowereds == _4showereds]

# wire ocupancy 
for st in stations:
    histos.update({\
        # ----------- digis wire ocupancy -----------
        f"digi_w_gmts_MB{st}":{ # for Genmuons That Showered -> (gmts)
            "type": "distribution2d",
            "histo" : r.TH2I(f"digi_w_gmts_MB{st}", r';Wheel; Events', 5, -2.5, 2.5, 200, 0, 200),
            "func" : partial(digi_w_ocupancy, station=st, _4showereds=True),
        },

        f"digi_w_gmtns_MB{st}":{ # for Genmuons That Not Showered -> (gmtns)
            "type": "distribution2d",
            "histo" : r.TH2I(f"digi_w_gmtns_MB{st}", r';Wheel; Events', 5, -2.5, 2.5, 200, 0, 200),
            "func" : partial(digi_w_ocupancy, station=st, _4showereds=False),
        },

        # --------- distribution of mean (w - avg_w) ------------
        
        f"digi_wd_gmts_MB{st}":{ # for Genmuons That Showered -> (gmts)
            "type": "distribution2d",
            "histo" : r.TH2D(f"digi_wd_gmts_MB{st}", r';Wheel; Events', 5, -2.5, 2.5, 200, -100, 100),
            "func" : partial(get_digis_distribution, station=st, _4showereds=True, distribution_type="mean"),
        },

        f"digi_wd_gmtns_MB{st}":{ # for Genmuons That Not Showered -> (gmtns)
            "type": "distribution2d",
            "histo" : r.TH2D(f"digi_wd_gmtns_MB{st}", r';Wheel; Events', 5, -2.5, 2.5, 200, -100, 100),
            "func" : partial(get_digis_distribution, station=st, _4showereds=False, distribution_type="mean"),
        },

        # --------- distribution of length (max_w - min_w) ------------
        f"digi_wl_gmts_MB{st}":{ # for Genmuons That Showered -> (gmts)
            "type": "distribution2d",
            "histo" : r.TH2D(f"digi_wl_gmts_MB{st}", r';Wheel; Events', 5, -2.5, 2.5, 200, 0, 200),
            "func" : partial(get_digis_distribution, station=st, _4showereds=True, distribution_type="length"),
        },

        f"digi_wl3d_gmts_MB{st}":{ # same as above but 3D (wheel, sl, length)
            "type": "distribution3d",
            "histo" : r.TH3D(f"digi_wl3d_gmts_MB{st}", r';Wheel; Events', 5, -2.5, 2.5, 200, 0, 200, 200, 0, 200),
            "func" : partial(get_digis_distribution, station=st, _4showereds=True, distribution_type="length", group_by_sl=True),
        },

        f"digi_wl_gmtns_MB{st}":{ # for Genmuons That Not Showered -> (gmtns)
            "type": "distribution2d",
            "histo" : r.TH2D(f"digi_wl_gmtns_MB{st}", r';Wheel; Events', 5, -2.5, 2.5, 200, 0, 200),
            "func" : partial(get_digis_distribution, station=st, _4showereds=False, distribution_type="length"),
        },

        f"digi_wl3d_gmtns_MB{st}":{ # same as above but 3D (wheel, sl, length)
            "type": "distribution3d",
            "histo" : r.TH3D(f"digi_wl3d_gmtns_MB{st}", r';Wheel; Events', 5, -2.5, 2.5, 200, 0, 200, 200, 0, 200),
            "func" : partial(get_digis_distribution, station=st, _4showereds=False, distribution_type="length", group_by_sl=True),
        },
    })