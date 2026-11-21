Event Dumping
=============

The event dumping tool exports processed events into a new ROOT file using the
same event-by-event abstraction used across the framework. This is useful when
you want to persist filtered or transformed event content for downstream tools.

.. rubric:: Usage

Use the ``dump-events`` command from the CLI:

.. code-block:: bash

    dtpr dump-events -i [INPATH] -o [OUTPATH] -cf [CONFIG_FILE] [other options]

Common examples:

.. code-block:: bash

    # Default output format (TTree) with YAML schema generation
    dtpr dump-events -i ./input_ntuple.root -o ./results

    # Use RNTuple format and add a tag to output files
    dtpr dump-events -i ./input_ntuple.root -o ./results -f rntuple -t _v2

    # Limit processing and disable YAML schema dump
    dtpr dump-events -i ./input_ntuple.root -o ./results --maxevents 1000 --no-yaml

.. rubric:: Main Options

- ``-i``, ``--inputpath``: input ROOT file or folder containing ROOT files.
- ``-o``, ``--outputpath``: output folder (or output ROOT path).
- ``-t``, ``--tag``: optional tag appended to output file names.
- ``--maxfiles``: maximum number of input files to read.
- ``--maxevents``: maximum number of events to dump.
- ``-f``, ``--format``: output format, either ``ttree`` (default) or ``rntuple``.
- ``--no-yaml``: skip YAML schema generation.
- ``--force-overwrite-yaml``: overwrite existing YAML schema files instead of skipping them.

.. rubric:: Parallel Runs and Job Splitting

``dump-events`` splits the entries the same way ``fill-histos`` does. ``--cores N`` processes ``N``
chunks at the same time; every chunk dumps its own ROOT file and the files are merged with ROOT at
the end, so a chunked dump holds exactly the same events as a serial one.

``--no-merge`` keeps the per-chunk files and prints the ``hadd`` command that reassembles them. The
YAML schema and the branch cleanup (``--remove-nbranches``) describe the merged file, so they are
written only for a merged run.

To split one run across machines, ``--job-id`` and ``--job-count`` give each job its own slice of the
entries. A job writes ``dtpr_events_dumped_p[JOB_ID].root``, and the job files are merged once all
jobs have finished:

.. code-block:: bash

    dtpr dump-events -i ./input_ntuple.root -o ./results --job-id 0 --job-count 4
    # ... one run per job, then:
    hadd -f results/dtpr_events_dumped.root results/dtpr_events_dumped_p*.root

.. rubric:: Output Files

When ``-o/--outputpath`` points to a directory, the command creates:

- ``dtpr_events_dumped{tag}.root``
- ``dumps_events_config{tag}.yaml`` (unless ``--no-yaml`` is used)

If ``-o/--outputpath`` is a ROOT file path, that path is used as the base output name.

Inside the ROOT output, dumped data is written under:

- ``dtprDumper/Events``

.. rubric:: Flattened Branch Structure

Dumped events are serialized into columnar branches. For nested collections, the dumper
uses a deterministic flattening convention so relationships can be reconstructed later.

Flattening rules:

- Event-level scalar attributes are written with an ``event_event`` prefix.
- Particle scalar attributes are written as ``particleType_attribute``.
- Nested list-of-list attributes are split into two companion branches:

  - ``<name>_flat``: concatenated values.
  - ``<name>_counts``: per-particle counts used to reconstruct each sub-list.

Example (from dumped schema):

.. code-block:: yaml

    particle_types:
      tps:
        attributes:
          matched_segments:
            target: "segments"
            identifier: "idx"
            branch:
              - tps_matched_segments_flat
              - tps_matched_segments_counts
          matched_genmuons:
            target: "genmuons"
            identifier: "idx"
            branch:
              - tps_matched_genmuons_flat
              - tps_matched_genmuons_counts

This representation allows storing arbitrarily nested matching relations in ROOT,
while keeping enough information to recover the per-particle lists.

For particle index ``i``, reconstruction follows:

.. math::

    \mathrm{start} = \sum_{k=0}^{i-1} \mathrm{flat\_counts}[k], \qquad
    \mathrm{end} = \mathrm{start} + \mathrm{flat\_counts}[i]

and the recovered list is ``flat[start:end]``.

.. note::
    For flattened list-of-list attributes, the generated YAML schema includes a ``target`` field 
    that should be set to the name of the target collection (e.g., "segments", "genmuons"). 
    This field is used by the framework to resolve cross-references when loading data back into Event objects.
    By default, the YAML schema is generated only if it does not already exist at the target location.
    Use ``--force-overwrite-yaml`` to overwrite existing schemas. The exact value used in ``amount`` 
    depends on the generated branch map. Branches with unknown/empty-only Awkward type are skipped during dumping.
