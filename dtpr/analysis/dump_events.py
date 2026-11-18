import os
from functools import partial

from ..base import NTuple
from ..core.engine import job_range, run_chunks, split_entries
from ..utils.dumper import (
    color_msg,
    dumped_branch_names,
    remove_nbranches_from_file,
    resolve_output_file,
    write_events_file,
    write_yaml_schema,
)


def write_dump_chunk(
    chunk,
    outpath: str,
    inpath: str,
    maxfiles: int = -1,
    fRNTuple: bool = False,
) -> None:
    """
    Dump the events of one entry range into one ROOT file.

    :param chunk: Pair ``(begin, end)`` of entries to process
    :type chunk: tuple
    :param outpath: Path of the ROOT file to write for this chunk
    :type outpath: str
    :param inpath: Path to the input folder containing NTuples
    :type inpath: str
    :param maxfiles: Maximum number of files to process
    :type maxfiles: int
    :param fRNTuple: Write an RNTuple instead of a TTree
    :type fRNTuple: bool
    :return: None
    :rtype: None
    """
    begin, end = chunk
    ntuple = NTuple(inputFolder=inpath, maxfiles=maxfiles, entry_range=(begin, end))
    events = [ev for ev in ntuple.events if ev is not None]
    if not events:
        color_msg(f"Chunk {begin}-{end} has no event to dump, skipping it.", "yellow")
        return
    write_events_file(events, outpath, fRNTuple=fRNTuple)


def dump(
    inpath: str,
    outfolder: str,
    tag: str,
    maxfiles: int,
    maxevents: int,
    format: str = "ttree",
    dump_yaml: bool = True,
    force_overwrite_yaml: bool = False,
    remove_nbranches: bool = False,
    ncores: int = -1,
    merge: bool = True,
    job_id: int = 0,
    job_count: int = 1,
) -> None:
    """
    Dump events from DT NTuple files into a ROOT file, one chunk per core.

    Every chunk writes its own ROOT file and the files are merged with ROOT: a
    tree is concatenated by the same call that adds histograms.

    :param inpath: Path to the input folder containing NTuples
    :type inpath: str
    :param outfolder: Path to the output folder
    :type outfolder: str
    :param tag: Tag to identify the output file
    :type tag: str
    :param maxfiles: Maximum number of files to process
    :type maxfiles: int
    :param maxevents: Maximum number of events to process (0 = all)
    :type maxevents: int
    :param format: Output format, 'ttree' or 'rntuple'
    :type format: str
    :param dump_yaml: Dump the YAML schema next to the ROOT file
    :type dump_yaml: bool
    :param force_overwrite_yaml: Overwrite an existing YAML schema
    :type force_overwrite_yaml: bool
    :param remove_nbranches: Drop the branches whose name starts with 'n'
    :type remove_nbranches: bool
    :param ncores: Number of worker processes; <= 1 runs serially, 0 uses every core
    :type ncores: int
    :param merge: Merge the chunk files into the final file
    :type merge: bool
    :param job_id: Index of this job when the run is split across machines
    :type job_id: int
    :param job_count: Number of jobs the entries are split across
    :type job_count: int
    :return: None
    :rtype: None
    """
    fRNTuple = False if format == "ttree" else True

    color_msg("Running program to dump events...", "green")

    # The chain gives the number of entries to split; no event is built here
    ntuple = NTuple(inputFolder=inpath, maxfiles=maxfiles)
    total = min(maxevents if maxevents > 0 else len(ntuple.events), len(ntuple.events))

    job_begin, job_end = job_range(total, job_id, job_count)
    ncores = os.cpu_count() if ncores == 0 else ncores
    chunks = [
        (job_begin + begin, job_begin + end)
        for begin, end in split_entries(job_end - job_begin, max(1, ncores))
    ]

    suffix = f"_p{job_id}" if job_count > 1 else ""
    base = outfolder if ".root" in outfolder else os.path.join(outfolder, "dtpr_events_dumped.root")
    outpath = resolve_output_file(base, tag + suffix)

    if ncores > 1:
        color_msg(
            f"Dumping {len(chunks)} chunks on {min(ncores, len(chunks))} cores...",
            color="cyan",
            indentLevel=1,
        )

    writer = partial(
        write_dump_chunk,
        inpath=inpath,
        maxfiles=maxfiles,
        fRNTuple=fRNTuple,
    )
    written = run_chunks(writer, chunks, ncores, outpath, merge=merge, desc="events")

    if not merge and ncores > 1:
        color_msg(
            "Chunk files kept: the branch cleanup and the YAML schema are written for the merged file only.",
            "yellow",
        )
        return

    if not os.path.exists(outpath):
        color_msg("No events to dump.", "yellow")
        return

    if remove_nbranches:
        remove_nbranches_from_file(outpath)

    if dump_yaml:
        write_yaml_schema(
            dumped_branch_names(outpath),
            outpath,
            tag=tag + suffix,
            force_overwrite_yaml=force_overwrite_yaml,
        )

    color_msg("Done!", "green")
