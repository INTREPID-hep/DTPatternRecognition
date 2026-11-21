"""Run a command over chunks of one NTuple, one ROOT file per chunk.

Every command that reads events can be expressed as a writer:

    write_chunk(chunk, outpath) -> None

where ``chunk`` is a ``(begin, end)`` entry range of the chain. The engine runs
that writer for every chunk - serially, or one process per core - and then
merges the per-chunk ROOT files into the final one.

Nothing is returned or pickled beyond the chunk and its path: ROOT merges
histograms and trees with the same call, so the merge step is ROOT's own
``TFileMerger`` and a run that is interrupted leaves the finished chunks on disk.
"""

import os
from multiprocessing import get_context

from tqdm import tqdm

from ..utils.functions import color_msg, create_outfolder


def split_entries(nentries, nchunks):
    """
    Split ``[0, nentries)`` into at most ``nchunks`` contiguous, near-equal ranges.

    :param nentries: Number of entries to split.
    :type nentries: int
    :param nchunks: Number of chunks wanted.
    :type nchunks: int
    :returns: List of ``(begin, end)`` ranges covering every entry exactly once.
    :rtype: list of tuple
    """
    nentries = max(0, int(nentries))
    if nentries == 0:
        return [(0, 0)]
    nchunks = max(1, min(int(nchunks), nentries))
    size, extra = divmod(nentries, nchunks)

    chunks = []
    begin = 0
    for index in range(nchunks):
        end = begin + size + (1 if index < extra else 0)
        chunks.append((begin, end))
        begin = end
    return chunks


def job_range(nentries, job_id, job_count):
    """
    The slice of ``[0, nentries)`` that job ``job_id`` of ``job_count`` handles.

    A job axis is a different axis from the core count: it splits a run over
    machines, and every job then splits its own slice over its cores.

    :param nentries: Number of entries to split.
    :type nentries: int
    :param job_id: Index of this job, in ``[0, job_count)``.
    :type job_id: int
    :param job_count: Number of jobs.
    :type job_count: int
    :returns: Pair ``(begin, end)`` of the entries of this job.
    :rtype: tuple
    :raises ValueError: If ``job_id`` is outside ``[0, job_count)``.
    """
    job_count = max(1, int(job_count))
    job_id = int(job_id)
    if job_id < 0 or job_id >= job_count:
        raise ValueError(f"job_id must be in [0, {job_count}), got {job_id}")
    ranges = split_entries(nentries, job_count)
    if job_id >= len(ranges):
        return (nentries, nentries)
    return ranges[job_id]


def part_paths(outpath, count):
    """
    Names of the per-chunk files that live next to ``outpath``.

    :param outpath: Path of the merged output.
    :type outpath: str
    :param count: Number of chunks.
    :type count: int
    :returns: List of paths.
    :rtype: list of str
    """
    stem, extension = os.path.splitext(outpath)
    return ["%s_p%d%s" % (stem, index, extension) for index in range(count)]


def merge_parts(parts, outpath):
    """
    Merge the chunk files into ``outpath`` with ROOT's ``TFileMerger``.

    Histograms are added bin by bin and trees are concatenated - the same call
    handles both, which is why a chunk writer only has to write a ROOT file.

    :param parts: The chunk files, in order.
    :type parts: list of str
    :param outpath: Path of the merged output.
    :type outpath: str
    :returns: The path of the merged file.
    :rtype: str
    """
    import ROOT as r

    merger = r.TFileMerger(False)
    for part in parts:
        if not merger.AddFile(part):
            raise RuntimeError(f"Cannot add {part} to the merge into {outpath}")
    if not merger.OutputFile(outpath):
        raise RuntimeError(f"Cannot open the merged output {outpath}")
    # OutputFile only names the output: the merge itself happens in Merge()
    if not merger.Merge():
        raise RuntimeError(f"Cannot merge the parts into {outpath}")
    return outpath


def _run_writer(task):
    """Run one chunk in a worker process (picklable top-level function)."""
    write_chunk, chunk, path = task
    write_chunk(chunk, path)
    return path


def run_chunks(write_chunk, chunks, ncores, outpath, merge=True, desc="chunks"):
    """
    Write every chunk and merge the result.

    :param write_chunk: Callable ``(chunk, outpath) -> None`` writing one chunk.
    :param chunks: List of ``(begin, end)`` entry ranges.
    :type chunks: list of tuple
    :param ncores: Number of worker processes; ``<= 1`` runs serially.
    :type ncores: int
    :param outpath: Path of the final ROOT file.
    :type outpath: str
    :param merge: Merge the chunk files into ``outpath`` and delete them; when
        False the chunk files are kept for a manual merge.
    :type merge: bool
    :param desc: Label of the progress bar.
    :type desc: str
    :returns: The files that were written: ``[outpath]``, or the chunk files.
    :rtype: list of str
    """
    outpath = os.path.abspath(outpath)
    create_outfolder(os.path.dirname(outpath))

    # One chunk, one file: no part file and no merge to pay for
    if len(chunks) == 1:
        write_chunk(chunks[0], outpath)
        return [outpath]

    parts = part_paths(outpath, len(chunks))
    tasks = list(zip([write_chunk] * len(chunks), chunks, parts))
    if ncores > 1:
        with get_context("fork").Pool(processes=ncores) as pool:
            for _ in tqdm(
                pool.imap_unordered(_run_writer, tasks),
                total=len(tasks),
                desc=desc,
                ncols=100,
                ascii=True,
                unit=" chunk",
            ):
                pass
    else:
        for task in tasks:
            _run_writer(task)

    # A chunk may legitimately write nothing (no event passed the selectors)
    written = [part for part in parts if os.path.exists(part)]
    if not written:
        color_msg("No chunk wrote a file, nothing to merge.", "yellow")
        return []

    if not merge:
        color_msg(
            f"Kept {len(written)} chunk files; merge them with: "
            f"hadd -f {outpath} {' '.join(written)}",
            "yellow",
        )
        return written

    merge_parts(written, outpath)
    for part in written:
        try:
            os.remove(part)
        except OSError:
            pass
    return [outpath]
