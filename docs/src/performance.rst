Performance
===========

``fill-histos`` and ``dump-events`` read events through the same layer a Python script uses,
so the behaviour described here applies to both.

Reading only what is needed
---------------------------

An ``NTuple`` reads the branches its configuration touches. When a branch-backed attribute is used
in an event, the entry is read once and every following particle of that event reuses the
converted value. ROOT's ``TTreeCache`` prefetches the baskets of the range being processed, which
matters most on remote storage.

Building particles
------------------

For each collection the framework prepares, once per event, how to read every configured
attribute, and then builds all particles of that collection from those readers. An ``expr``
attribute sees the attributes declared before it as local variables, and a ``[flat, counts]``
pair is resolved from cumulative counts, so a list-valued attribute does not rescan the counts of
the particles before it.

Running several chunks at once
------------------------------

``fill-histos`` and ``dump-events`` accept:

- ``--cores N``: split the entries into chunks processed at the same time (``-1``, the default,
  runs serially; ``0`` uses every core).
- ``--no-merge``: keep the ROOT file of every chunk instead of merging them, so an interrupted run
  keeps its finished work; the command prints the ``hadd`` line that merges the parts.
- ``--job-id`` and ``--job-count``: split the entries across machines, one file per job.

The progress bar refreshes about ten times over a run instead of once per event, and a slow run
still refreshes at least every five seconds.

Processing part of a file from Python
-------------------------------------

``NTuple(..., entry_range=(begin, end))`` exposes one contiguous window of the chain, which is how
a script processes a slice of a file exactly as the chunked commands do.

Measuring the loop
------------------

``bench/bench_loop.py`` builds events from a test NTuple and prints milliseconds per event together
with the particle counts, so a change in speed and a change in content can be checked in the same
run.
