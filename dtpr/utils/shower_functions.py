from dtpr.utils.functions import color_msg
from dtpr.utils.functions import create_outfolder, get_unique_locs
from dtpr.base import Event, Particle
import warnings
from pandas import DataFrame
import matplotlib.pyplot as plt
from numpy import ceil, array
import gc
from collections import deque


def build_fwshowers(ev: Event, threshold=None, debug=False, debug_step=4, debug_path="./results"):
    """
    Emulate the behavior of shower reconstruction in FPGA firmware.

    Args:
        thr (int, optional): The threshold for shower building. Defaults to None.
        debug (bool, optional): Whether to enable debugging. Defaults to False.
        debug_step (int, optional): The step interval for debugging. Defaults to 4.
        debug_path (str, optional): The path to save debug plots. Defaults to "./results".
    """
    if not hasattr(ev, "digis"):
        warnings.warn("'digis' is not included in _PARTICLE_TYPES. Please check the config YAML file. Skipping firmware shower building.")
        return
    #prepare the event to store the showers
    setattr(ev, "fwshowers", [])
    if not threshold:
        threshold = 6

    digis_df = DataFrame([digi.__dict__ for digi in ev.digis])

    if digis_df.empty:  # if there are no digis, pass
        return
    
    ev_BXs = digis_df.BX.unique()  # Get all unique BXs in the event
    shower_info = {}

    digis_groups = digis_df.groupby(["wh", "sc", "st", "sl"])

    if debug and ev.index % debug_step == 0:
        ncols, nrows = 4, int(ceil(digis_groups.ngroups / 4))
        fig, axs = plt.subplots(nrows, ncols, figsize=(10 * ncols, 8 * nrows))
        axs = axs.flat

    for idx, ((wh, sc, st, sl), digis_group) in enumerate(digis_groups):  # apply by SL
        if sl == 2: # SL 2 is not used in the firmware
            continue
        spread = digis_group.drop_duplicates(["l", "w"])["w"].std()**2 > 1
        if not spread:
            continue
        showered, nHits, sBX, hits_history = _process_superlayer(ev_BXs, digis_group, threshold)
        if showered:
            if debug and ev.index % debug_step == 0:
                color_msg(
                    f"Shower detected in (wh, sc, st, sl): ({wh}, {sc}, {st}, {sl}), with {nHits} hits in bx {sBX}",
                    "purple",
                    indentLevel=2,
                )

            key = (wh, sc, st, sl)  # since is by SL, it could generate multiple showers per chamber
            if key not in shower_info:
                shower_info[key] = (nHits, sBX)

        if debug and ev.index % debug_step == 0:
            axs[idx].plot(hits_history[:, 0], hits_history[:, 1])
            axs[idx].hlines(threshold, min(ev_BXs), max(ev_BXs), "r", "--")
            if showered:
                axs[idx].text(x=25, y=5, s="FwSHOWER", color="darkgreen", alpha=0.8)
            axs[idx].set_title(f"wh = {wh}, sc = {sc}, st = {st}, sl = {sl}")
            axs[idx].set_xlabel("BX")
            axs[idx].set_ylabel("Hits")

    if debug and ev.index % debug_step == 0:
        plt.tight_layout()
        create_outfolder(f"{debug_path}/fwshower_plots/")
        fig.savefig(f"{debug_path}/fwshower_plots/hits_evolution_ev{ev.index}.pdf")
        plt.close(fig)
        gc.collect()

    for ish, ((wh, sc, st, sl), (nHits, bx)) in enumerate(shower_info.items()):
        _shower = Particle(index=ish, wh=wh, sc=sc, st=st, nDigis=nHits, BX=bx, name="Shower")
        _shower.sl = sl
        ev.fwshowers.append(_shower)

def _process_superlayer(ev_BXs, digis_df, threshold):
    """
    Detect a shower in a superlayer by counting hits. That is done by going through the following rules:
    - Only 8 hits can be counted per BX.
    - Hits not read within 4 BXs will be removed from the buffer.
    - The DT relaxing time is longer than 1 BX, so if a hit was counted in a previous BX, it will be ignored.
    - The shower is detected if the number of hits in the last 16 BXs is greater than or equal to the threshold.

    Args:
        ev_BXs (set): The set of BXs in the event.
        digis_df (DataFrame): The dataframe containing digi information.
        threshold (int): The threshold for shower detection.

    Returns:
        tuple: A tuple containing a boolean indicating if a shower was detected,
                the maximum number of hits, and the history of hit counts.
    """
    wires_buff = deque()  # Use deque such as FIFO
    num_hits_last_16Bxs = deque(maxlen=16)  # Use deque to count hits, in BXs larger than 16 it will start to delete elements
    showered = False
    num_hits_history = []

    min_bx, max_bx = min(ev_BXs), max(ev_BXs)
    hot_w = set()  # hot wires is reset each two BXs

    for bx in range(min_bx, max_bx + 1):
        if (bx - min_bx) % 2 == 0:
            hot_w.clear()  # reset hot wires every two BXs

        wires_buff.extend((w, bx) for w in digis_df.loc[digis_df["BX"] == bx, "w"])  # hits of this bx

        # Remove hits older than 4 BXs
        while wires_buff and bx - wires_buff[0][1] > 4:
            wires_buff.popleft()

        hits_counter = 0
        for _ in range(8):  # Just count until reaching 8 wires.
            if not wires_buff:
                break
            w, _ = wires_buff.popleft()
            if w in hot_w:  # if the wire is already hot from previous BX, ignore it
                continue
            hot_w.add(w)
            hits_counter += 1  # count hits

        num_hits_last_16Bxs.append(hits_counter)
        num_hits_history.append([bx, sum(num_hits_last_16Bxs)])  # this is just to debug producing plots
        if sum(num_hits_last_16Bxs) >= threshold:
            showered = True

    num_hits_history = array(num_hits_history)
    nHits = num_hits_history[:, 1].max()
    sBX = num_hits_history[num_hits_history[:, 1] == nHits][0, 0]
    return showered, nHits, sBX, num_hits_history


def build_real_showers(ev: Event, threshold=None, debug=False):
    """
    Build real showers based on simhit information.

    Args:
        threshold (int, optional): The threshold for shower building. Defaults to None.
        debug (bool, optional): Whether to enable debugging. Defaults to False.
    """

    if not hasattr(ev, "simhits"):
        warnings.warn("'simhits' is not included in _PARTICLE_TYPES. Please check the config YAML file. Skipping real shower building.")
        return

    ev.realshowers = []
    thr = 8 if threshold is None else threshold

    simhits_locs = get_unique_locs(particles=ev.simhits, loc_ids=["wh", "sc", "st", "sl"])
    digis_locs = get_unique_locs(particles=ev.digis, loc_ids=["wh", "sc", "st", "sl"])
    indexs = simhits_locs.union(digis_locs)

    for wh, sc, st, sl in indexs:
        if sl == 2:  # SL 2 is not used
            continue

        simhits_sdf = DataFrame([simhit.__dict__ for simhit in ev.filter_particles("simhits", wh=wh, sc=sc, st=st, sl=sl)])
        digis_sdf = DataFrame([digi.__dict__ for digi in ev.filter_particles("digis", wh=wh, sc=sc, st=st, sl=sl)])

        _build_shower = False

        if not simhits_sdf.empty:
            simhits_sdf = simhits_sdf[["l", "w", "particle_type"]].drop_duplicates()
            # conditions...
            # pass the threshold of hits
            pass_thr = len(simhits_sdf.drop_duplicates(["l", "w"])) >= thr
            # at least 3 muon hits
            are_muons_hits = len(simhits_sdf.loc[simhits_sdf["particle_type"].abs() == 13]) >= 3
            # at least 1 electron hit
            are_electron_hits = len(simhits_sdf.loc[simhits_sdf["particle_type"].abs() == 11]) > 0
            # hits are spread out in the chamber
            spread = simhits_sdf["w"].std()**2 > 1
            # are duplicated mathced segments
            try:
                segments_locs = [(_wh, _sc, _st) for gm in ev.genmuons for _st, _sc, _wh in gm.matched_segments_stations if _st == st and _sc == sc and _wh == wh]
                are_duplicated_segments = len(segments_locs) > len(set(segments_locs))
            except:
                are_duplicated_segments = False # -- for G4 DTNtuples there are no segments
            if pass_thr:
                if debug: color_msg(f'spread: {spread} --> {simhits_sdf["w"].std()**2}', "purple", indentLevel=2)
                if are_muons_hits and are_electron_hits and spread:
                    shower_type = 1
                elif are_electron_hits and spread:
                    shower_type = 2
                elif are_duplicated_segments:
                    shower_type = 3
                else:
                    continue
                _build_shower = True

        # elif not digis_sdf.empty:
        #     digis_sdf = digis_sdf[["l", "w"]].drop_duplicates()
        #     # conditions...
        #     pass_thr = len(digis_sdf) >= thr
        #     # hits are spread out in the chamber
        #     std_ = digis_sdf["w"].std()**2
        #     spread = std_ > 1 and std_ < 5
        #     if debug: color_msg(f'spread: {spread} --> {std_}', "purple", indentLevel=2)
        #     if debug: color_msg(f'iqr: {digis_sdf["w"].quantile(0.25)}, {digis_sdf["w"].quantile(0.75)}', "purple", indentLevel=2)
        #     if len(digis_sdf) >= thr and spread:
        #         shower_type = 4
        #         _build_shower = True
        
        if _build_shower:
            _index = ev.realshowers[-1].index + 1 if ev.realshowers else 0
            _shower = Particle(index=_index, wh=wh, sc=sc, st=st, name="Shower") 
            _shower.shower_type = shower_type
            _shower.sl = sl
            ev.realshowers.append(_shower)
            if debug:
                color_msg(
                    f'Realshower detected in (wh, sc, st, sl): ({wh}, {sc}, {st}, {sl}) - type: {shower_type}',
                    "green",
                    indentLevel=2,
                )



def analyze_fwshowers(ev: Event):
    """
    Determine if the firmware showers are real by comparing them with the real showers.

    Args:
        ev (Event): The event to analyze.
    """
    if not hasattr(ev, "fwshowers") or not hasattr(ev, "realshowers"):
        warnings.warn("Either 'fwshowers' or 'realshowers' are not included in _PARTICLE_TYPES. Please check the config YAML file. Skipping shower analysis.")
        return

    #prepare the event to store the showers
    for shower in ev.fwshowers:   
        wh, sc, st , sl = shower.wh, shower.sc, shower.st, shower.sl
        if ev.filter_particles("realshowers", wh=wh, sc=sc, st=st, sl=sl):
            shower.is_true_shower = True
        else:
            shower.is_true_shower = False
