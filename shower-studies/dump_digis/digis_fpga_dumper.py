import os
from dtpr.base import Event, NTuple
from dtpr.utils.functions import color_msg, create_outfolder, get_unique_locs
from dtpr.utils.config import RUN_CONFIG
from tqdm import tqdm
from collections import deque

HOTS_PERSISTANCE = 2
OBDT_PERSISTANCE = 4

def get_last_bx_and_id_send(file):
    try:
        with open(file, "r") as f:
            lines = f.readlines()
            last_line = lines[-1]
            return (
                int(last_line.split()[0]) + 50, # delay between events
                int(last_line.split()[-2]) + 1 # the last id
            )
    except Exception as e:
        return 0, 0

def process_digis(event, wh, sc, st, file, bx_send, last_id=0):
    global OBDT_PERSISTANCE, HOTS_PERSISTANCE
    digis = event.filter_particles("digis", wh=wh, sc=sc, st=st)
    min_bx, max_bx = digis[0].BX, digis[-1].BX
    obdt_buffer = deque()
    hot_w = []  # hot wires is reset each two BXs
    id = last_id
    for bx in range(min_bx, max_bx + 17):
        digis_ = [digi for digi in digis if digi.BX == bx and digi.sl != 2] # sl=2 is not used
        for digi in digis_:
            if hot_w and (digi.sl, digi.l, digi.w) in [(sl, l, w) for _, (sl, l, w) in hot_w]:
                print(f"Hot wire: {digi.sl} {digi.l} {digi.w} {bx} {digi.time}")
                continue
            obdt_buffer.extend([(id, digi)])
            hot_w.append([bx, (digi.sl, digi.l, digi.w)])
            id += 1
        for i in range(8):
            if not obdt_buffer:
                break
            idd, digi = obdt_buffer.popleft()
            # file.write(f"{bx_send} {digi.sl} {digi.BX} {digi.time} {digi.l} {digi.w} {idd}\n")
            file.write(f"{bx_send} {digi.sl} {digi.BX} {int(digi.time % 25 * 32 / 25)} {digi.l} {digi.w} {idd} {event.index}\n") # tdc ???
        bx_send += 1
        while obdt_buffer and bx - obdt_buffer[0][1].BX > OBDT_PERSISTANCE:
            obdt_buffer.popleft()
        while hot_w and bx - hot_w[0][0] > HOTS_PERSISTANCE:
            hot_w = hot_w[1:]


def _dump_digis(event: Event, outpath: str, tag: str=""):
    locs = get_unique_locs(event.digis, loc_ids=["wh", "sc", "st"])
    for wh, sc, st in locs:
        file_path = f"{outpath}/digis_wh{wh}_sc{sc}_st{st}{tag}.txt"
        with open(file_path, "a") as f:
            bx_send, last_id = get_last_bx_and_id_send(f.name)
            process_digis(event, wh, sc, st, f, bx_send, last_id)
        
def _dump_showers(event: Event, outpath: str, tag: str=""):
    locs = get_unique_locs(event.emushowers, loc_ids=["wh", "sc", "st"])
    for wh, sc, st in locs:
        showers_file_path = f"{outpath}/showers_wh{wh}_sc{sc}_st{st}{tag}.txt"
        with open(showers_file_path, "a") as showers_f:
            showers = event.filter_particles("emushowers", wh=wh, sc=sc, st=st)
            for shower in showers:
                showers_f.write(
                    f"# Event {event.index}\n"
                    f"sl: {shower.sl}\n"
                    f"nDigis: {shower.nDigis}\n"
                    f"BX: {shower.BX}\n"
                    f"minW: {shower.min_wire}\n"
                    f"maxW: {shower.max_wire}\n"
                    f"avgPos: {shower.avg_pos}\n"
                    f"avgTime: {shower.avg_time}\n"
                    f"wires_profile: {shower.wires_profile}\n"
                )
        
def dump_digis(
        inpath: str,
        outfolder: str,
        tag: str="",
        maxfiles: int=-1,
        event_number: int=-1,
    ):
    """
    DOCSTRING DESCRIPTION...

    Parameters:
    inpath (str): Path to the input folder containing the ntuples.
    outfolder (str): Path to the output folder where results will be saved.
    tag (str): Tag to append to output filenames.
    maxfiles (int): Maximum number of input files to process.
    maxevents (int): Maximum number of events to process.
    """
    create_outfolder(os.path.join(outfolder, "digis_fpga_dumper_results"))
    create_outfolder(os.path.join(outfolder, "showers_dumper_results"))

    # Start of the analysis 
    color_msg(f"Running program...", "green")

    # Create the Ntuple object
    ntuple = NTuple(
        inputFolder=inpath,
        maxfiles=maxfiles,
    )

    if isinstance(event_number, str):
        event_indices = eval(f"slice({event_number.replace(':', ',')})")
        events = ntuple.events[event_indices]
        beg, *end = event_number.split(":")
        try:
            total = (int(end[0]) - int(beg)) // int(end[1])
        except IndexError:
            total = int(end[0]) - int(beg)
        print(f"Total events: {total}")
    else:
        if event_number == -1:
            events = ntuple.events
            total = len(ntuple.events)
        else:
            events = [ntuple.events[event_number]]
            total = 1
    
    with tqdm(
        total=total,
        desc=color_msg(
            "Running:", color="purple", indentLevel=1, return_str=True
        ),
        ncols=100,
        ascii=True,
        unit=" event",
    ) as pbar:
        for ev in events:
            if not ev: 
                color_msg(f"Event {ev.index} not pass, skipping...", color="red")
                continue
            if total < 10:
                pbar.update(1)
            elif ev.index % (total // 10) == 0:
                pbar.update(total // 10)
            if ev.index >= total:
                break

            _dump_digis(ev, outpath=os.path.join(outfolder, "digis_fpga_dumper_results"), tag=tag)
            _dump_showers(ev, outpath=os.path.join(outfolder, "showers_dumper_results"), tag=tag)

    color_msg(f"Done!", color="green")

if __name__ == "__main__":
    inpath = os.path.join("/mnt/c/Users/estradadaniel/cernbox/ZprimeToMuMu_M-6000_TuneCP5_14TeV-pythia8/ZprimeToMuMu_M-6000_PU200/250312_131631/0000/")
    outfolder = "."
    RUN_CONFIG.change_config_file(config_path="./run_config.yaml")
    dump_digis(inpath=inpath, outfolder=outfolder)