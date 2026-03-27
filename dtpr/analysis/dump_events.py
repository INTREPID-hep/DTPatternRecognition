from ..base.config import RUN_CONFIG
from ..base import NTuple
from ..utils.dumper import dump_events, color_msg

def dump(
    inpath: str, 
    outfolder: str, 
    tag: str, 
    maxfiles: int,
    maxevents: int,
    format: str = "ttree",
    dump_yaml: bool = True,
) -> None:
    fRNTuple = False if format == "ttree" else True  # Set to True if you want to force the use of RNTuple format for dumping events

    color_msg("Running program to dump events...", "green")

    # Create the Ntuple object and set maxevents
    ntuple = NTuple(inputFolder=inpath, maxfiles=maxfiles)
    _maxevents = min(maxevents if maxevents > 0 else len(ntuple.events), len(ntuple.events))

    events = ntuple.events[:_maxevents] if _maxevents > 0 else ntuple.events
    dump_events(events, outpath=outfolder, tag=tag, fRNTuple=fRNTuple, dump_yaml_schema=dump_yaml)

    color_msg("Done!", "green")
