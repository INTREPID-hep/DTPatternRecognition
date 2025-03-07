import os
from dtpr.base import Event
from dtpr.utils.functions import color_msg, create_outfolder, get_unique_locs, init_ntuple_from_config
from dtpr.utils.config import RUN_CONFIG
from tqdm import tqdm
from collections import deque

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
    HOTS_PERSISTANCE = RUN_CONFIG.digis_fpga_dumper["HOTS_PERSISTANCE"]
    OBDT_PERSISTANCE = RUN_CONFIG.digis_fpga_dumper["OBDT_PERSISTANCE"]

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
        while obdt_buffer and bx - obdt_buffer[0][1].BX >= OBDT_PERSISTANCE:
            obdt_buffer.popleft()
        while hot_w and bx - hot_w[0][0] >= HOTS_PERSISTANCE:
            hot_w = hot_w[1:]


def _dump_digis(event: Event, outpath: str, tag: str=""):
    locs = get_unique_locs(event.digis, loc_ids=["wh", "sc", "st"])
    for wh, sc, st in locs:
        file_path = f"{outpath}/digis_wh{wh}_sc{sc}_st{st}{tag}.txt"
        try:
            with open(file_path, "a") as f:
                bx_send, last_id = get_last_bx_and_id_send(f.name)
                process_digis(event, wh, sc, st, f, bx_send, last_id)
        except Exception as e:
            color_msg(f"Error processing event {event.index} for loc (wh={wh}, sc={sc}, st={st}): {e}")

def _dump_showers(event: Event, outpath: str, tag: str=""):
    locs = get_unique_locs(event.fwshowers, loc_ids=["wh", "sc", "st"])
    for wh, sc, st in locs:
        showers_file_path = f"{outpath}/showers_wh{wh}_sc{sc}_st{st}{tag}.txt"
        try:
            with open(showers_file_path, "a") as showers_f:
                showers = event.filter_particles("fwshowers", wh=wh, sc=sc, st=st)
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
        except Exception as e:
            color_msg(f"Error processing event {event.index} for loc (wh={wh}, sc={sc}, st={st}): {e}")

def dump_digis(
        inpath: str,
        outfolder: str,
        tag: str,
        maxfiles: int,
        maxevents: int,
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
    ntuple = init_ntuple_from_config(
        inputFolder=inpath,
        maxfiles=maxfiles,
    )

    _maxevents = maxevents if maxevents > 0 and maxevents < len(ntuple.events) else len(ntuple.events)


    for ev in tqdm(
        ntuple.events[:_maxevents],
        total=_maxevents,
        desc=color_msg(f"Running:", color="purple", indentLevel=1, return_str=True),
        ncols=100,
        ascii=True,
        unit="event"
    ):
        if not ev: 
            continue

        _dump_digis(ev, outpath=os.path.join(outfolder, "digis_fpga_dumper_results"), tag=tag)
        _dump_showers(ev, outpath=os.path.join(outfolder, "showers_dumper_results"), tag=tag)

    color_msg(f"Done!", color="green")

if __name__ == "__main__":
    inpath = os.path.join("/mnt/c/Users/estradadaniel/cernbox/ZprimeToMuMu_M-6000_TuneCP5_14TeV-pythia8/ZprimeToMuMu_M-6000_PU200/250312_131631/0000/")
    outfolder = "../../test/dump_digis/"
    RUN_CONFIG.change_config_file(os.path.join(outfolder, "run_config.yaml"))
    create_outfolder(os.path.join(outfolder, "digis_dumper_test"))
    ntuple = init_ntuple_from_config(
        inputFolder=inpath,
        maxfiles=-1,
    )

    event = ntuple.events[68]
    _dump_digis(event, outpath=os.path.join(outfolder, "digis_dumper_test"), tag="ev69_test")