import os
import importlib
from dtpr.base import Event
from dtpr.utils.functions import color_msg
from src.utils.functions import create_outfolder
from src.utils.config import RUN_CONFIG
from tqdm import tqdm
from src.utils.functions import get_unique_locs
from collections import deque

def get_last_bx_and_id_send(file):
    try:
        with open(file, "r") as f:
            lines = f.readlines()
            last_line = lines[-1]
            return (
                int(last_line.split()[0]) + 50, # delay between events
                int(last_line.split()[-1]) + 1 # the last id
            )
    except Exception as e:
        return 0, 0

def process_digis(event, wh, sc, st, file, bx_send, last_id=0):
    HOTS_PERSISTANCE = RUN_CONFIG.digis_fgpa_dumper["HOTS_PERSISTANCE"]
    OBDT_PERSISTANCE = RUN_CONFIG.digis_fgpa_dumper["OBDT_PERSISTANCE"]

    digis = event.filter_particles("digis", wh=wh, sc=sc, st=st)
    min_bx, max_bx = digis[0].BX, digis[-1].BX
    obdt_buffer = deque()
    hot_w = []  # hot wires is reset each two BXs
    id = last_id
    for bx in range(min_bx, max_bx + 17):
        digis_ = [digi for digi in digis if digi.BX == bx and digi.sl != 2] # sl=2 is not used
        for digi in digis_:
            if hot_w and (digi.sl, digi.l, digi.w) in [(sl, l, w) for _, (sl, l, w) in hot_w]:
                continue
            obdt_buffer.extend([(id, digi)])
            hot_w.append([bx, (digi.sl, digi.l, digi.w)])
            id += 1
        for i in range(8):
            if not obdt_buffer:
                break
            idd, digi = obdt_buffer.popleft()
            # file.write(f"{bx_send} {digi.sl} {digi.BX} {digi.time} {digi.l} {digi.w} {idd}\n")
            file.write(f"{bx_send} {digi.sl} {digi.BX} {int(digi.time % 25 * 32 / 25)} {digi.l} {digi.w} {idd}\n") # tdc ???
        bx_send += 1
        while obdt_buffer and bx - obdt_buffer[0][1].BX >= OBDT_PERSISTANCE:
            obdt_buffer.popleft()
        while hot_w and bx - hot_w[0][0] >= HOTS_PERSISTANCE:
            hot_w = hot_w[1:]


def _process_event(event: Event, outpath: str, tag: str=""):
    locs = get_unique_locs(event.digis, loc_ids=["wh", "sc", "st"])
    for wh, sc, st in locs:
        file_path = f"{outpath}/digis_wh{wh}_sc{sc}_st{st}{tag}.txt"
        try:
            with open(file_path, "a") as f:
                bx_send, last_id = get_last_bx_and_id_send(f.name)
                process_digis(event, wh, sc, st, f, bx_send, last_id)
        except Exception as e:
            color_msg(f"Error processing event {event.iev} for loc (wh={wh}, sc={sc}, st={st}): {e}")

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

    # Start of the analysis 
    color_msg(f"Running program...", "green")

    # Create the Ntuple object
    _ntuple_module, _ntuple_class = RUN_CONFIG.ntuple_source.rsplit(".", 1)
    _ntuple_module = importlib.import_module(_ntuple_module)
    _NTUPLE =  getattr(_ntuple_module, _ntuple_class)

    ntuple = _NTUPLE(
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

        _process_event(ev, outpath=os.path.join(outfolder, "digis_fpga_dumper_results"), tag=tag)

    color_msg(f"Done!", color="green")
