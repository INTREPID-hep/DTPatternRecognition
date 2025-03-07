# Generic analysis template generated on Mon Mar 24 10:04:30 2025
# 
# Author:
#     [Your Name]
# Version:
#     0.1
#
# This function is a template for a generic analysis based on NTuples information.

from dtpr.base import Event
from dtpr.utils.functions import color_msg, init_ntuple_from_config
from dtpr.utils.config import RUN_CONFIG
from tqdm import tqdm

TP = 0
FP = 0

def _process_event(ev1: Event, ev2: Event):
    global TP, FP    
    fw_showers1 = ev1.fwshowers
    fw_showers2 = ev2.fwshowers

    for fs1 in fw_showers1:
        if fs1 not in fw_showers2:
            real_shower = ev1.filter_particles("realshowers", wh=fs1.wh, sc=fs1.sc, st=fs1.st, sl=fs1.sl)
            if real_shower:
                tqdm.write(f"{ev1.index} {fs1.wh} {fs1.sc} {fs1.st} {fs1.sl} TP")
                TP += 1
            else:
                tqdm.write(f"{ev1.index} {fs1.wh} {fs1.sc} {fs1.st} {fs1.sl} FP")
                FP += 1


def inspect_showThrChanges():
    # Start of the analysis 
    color_msg(f"Running program...", "green")

    file_thr6 = "/lustrefs/hdd_pool_dir/L1T/Filter/ThresholdScan_Zprime_DY/last/ZprimeToMuMu_M-6000_PU200/250312_131631/0000/"
    file_thr14 = "/lustrefs/hdd_pool_dir/L1T/Filter/ThresholdScan_Zprime_DY/last/ZprimeToMuMu_M-6000_PU200/250312_131715/0000/"

    RUN_CONFIG.change_config_file("./run_config.yaml")

    # Create the Ntuple object
    ntuple_6 = init_ntuple_from_config(
        inputFolder=file_thr6,
        maxfiles=1,
        config=RUN_CONFIG,
    )

    ntuple_14 = init_ntuple_from_config(
        inputFolder=file_thr14,
        maxfiles=1,
        config=RUN_CONFIG,
    )

    total = len(ntuple_6.events)

    with tqdm(total=total, desc=color_msg("Processing : ", color="purple"), unit=" events", ascii=True) as pbar:
        for i in range(total):
            ev_6 = ntuple_6.events[i]
            ev_14 = ntuple_14.events[i]
            if not ev_6 or not ev_14: continue

            _process_event(ev_6, ev_14)
            if i % (total // 100) == 0:
                pbar.update((total // 100))

    color_msg(f"TP: {TP} FP: {FP}", color="red")
    color_msg(f"Done!", color="green")


if __name__ == "__main__":
    inspect_showThrChanges()