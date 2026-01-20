Dumping Events to ROOT Ntuples
==============================

The ``dump-events`` CLI command allows you to snapshot/dump processed events back into a flat ROOT ntuple 
after processing. This is useful when you want to save events after applying preprocessors and selectors, 
store intermediate analysis results, share processed data in a standard ROOT format, or read back events 
later without reprocessing.

.. rubric:: Overview

After processing your events with preprocessors and selectors, you may want to save the modified events 
to disk for later analysis. The ``dump-events`` tool writes your processed events to a new ROOT file in 
a flat ntuple structure, preserving:

- Event-level attributes
- All particle collections (e.g., genmuons, segments, digis)
- Particle-to-particle relationships (e.g., matched_segments, matched_tps)

The dumped files are standard ROOT files that can be read using the provided ``DumpedEventReader`` class 
or directly with ROOT tools.

.. rubric:: How It Works

Storage Strategy for Particle References
-----------------------------------------

When particles contain references to other particles (e.g., ``matched_segments``, ``matched_tps``), 
the dumper stores them as indices with type information for later reconstruction:

**For list references:**

- ``{ptype}_{attr}_indices``: Vector of indices of referenced particles
- ``{ptype}_{attr}_type``: Particle type name (for reconstruction)

**For single references:**

- ``{ptype}_{attr}_index``: Index of referenced particle
- ``{ptype}_{attr}_type``: Particle type name

For example, if a ``genmuon`` has ``matched_segments``, it will be stored as:

- ``genmuons_matched_segments_indices``: ``vector<vector<int>>`` containing segment indices
- ``genmuons_matched_segments_type``: ``vector<string>`` containing ``"Segment"``

This approach allows full reconstruction of particle relationships when reading the dumped ntuple.

.. rubric:: Usage

Command Line Interface
----------------------

Basic usage to dump all events:

.. code-block:: bash

    dtpr dump-events -i /path/to/input.root -o ./results

Dump a specific number of events:

.. code-block:: bash

    dtpr dump-events -i /path/to/input.root -o ./results --maxevents 1000

Dump with custom tree name and tag:

.. code-block:: bash

    dtpr dump-events -i /path/to/input.root -o ./results --tree-name MyEvents -t _processed

Dump only specific particle types:

.. code-block:: bash

    dtpr dump-events -i /path/to/input.root -o ./results --particle-types genmuons segments

Use custom configuration:

.. code-block:: bash

    dtpr dump-events -i /path/to/input.root -o ./results -cf my_config.yaml

Python API
----------

You can also use the dump function directly in your Python scripts:

.. code-block:: python

    from dtpr.analysis.dump_events import dump_events

    # Dump all particle types
    dump_events(
        inpath="/path/to/input.root",
        outfolder="./results",
        tag="_processed",
        maxfiles=1,
        maxevents=-1,  # -1 means all events
        tree_name="EVENTS",
        particle_types=None,  # None means all types
    )

    # Dump specific particle types only
    dump_events(
        inpath="/path/to/input.root",
        outfolder="./results",
        tag="_genmuons_only",
        maxfiles=-1,
        maxevents=1000,
        tree_name="EVENTS",
        particle_types=["genmuons", "segments"],
    )

.. rubric:: Reading Dumped Events

Using DumpedEventReader
-----------------------

The ``DumpedEventReader`` class reconstructs the Event and Particle objects from dumped ntuples, 
including particle-to-particle relationships:

.. code-block:: python

    from dtpr.utils.dumped_ntuple_reader import DumpedEventReader

    # Method 1: Using context manager (recommended)
    with DumpedEventReader("dumped_events.root", tree_name="EVENTS") as reader:
        # Get number of events
        print(f"Total events: {len(reader)}")
        
        # Access by index
        event = reader[0]
        print(event)
        
        # Slice events
        events_10_to_20 = reader[10:20]
        
        # Iterate over events
        for event in reader:
            print(event)
            # Access particles
            for genmuon in event.genmuons:
                print(f"GenMuon pt: {genmuon.pt}")
                # Access matched particles (relationships are reconstructed!)
                if hasattr(genmuon, 'matched_segments'):
                    print(f"  Matched segments: {len(genmuon.matched_segments)}")
                    for seg in genmuon.matched_segments:
                        print(f"    Segment at wh={seg.wh}, st={seg.st}, sc={seg.sc}")

    # Method 2: Manual management
    reader = DumpedEventReader("dumped_events.root")
    event = reader.read_event(5)  # Read specific event
    reader.close()

Using ROOT Directly
-------------------

You can also read the dumped files with standard ROOT tools:

.. code-block:: python

    import ROOT as r

    f = r.TFile("dumped_events.root")
    tree = f.Get("EVENTS")

    for i, entry in enumerate(tree):
        if i >= 10:
            break
        
        print(f"Event {i}:")
        print(f"  Event number: {entry.number}")
        print(f"  Number of genmuons: {entry.n_genmuons}")
        
        # Access particle attributes
        for j in range(entry.n_genmuons):
            print(f"  GenMuon {j}: pt={entry.genmuons_pt[j]}, eta={entry.genmuons_eta[j]}")
            
            # Access matched particle indices
            if hasattr(entry, 'genmuons_matched_segments_indices'):
                matched_indices = list(entry.genmuons_matched_segments_indices[j])
                print(f"    Matched segment indices: {matched_indices}")

.. rubric:: Example Workflow

Step 1: Process and Dump Events
--------------------------------

.. code-block:: python

    from dtpr.base.config import RUN_CONFIG
    from dtpr.analysis.dump_events import dump_events

    # Configure your analysis
    RUN_CONFIG.change_config_file("my_config.yaml")

    # This configuration includes preprocessors that add matched_segments
    dump_events(
        inpath="/data/dt_ntuples/",
        outfolder="./processed_events",
        tag="_with_matching",
        maxfiles=-1,
        maxevents=-1,
    )

Step 2: Read Back and Analyze
------------------------------

.. code-block:: python

    from dtpr.utils.dumped_ntuple_reader import DumpedEventReader
    from dtpr.utils.functions import color_msg

    with DumpedEventReader("processed_events/dumped_events_with_matching.root") as reader:
        matched_count = 0
        unmatched_count = 0
        
        for event in reader:
            for genmuon in event.genmuons:
                if hasattr(genmuon, 'matched_segments') and len(genmuon.matched_segments) > 0:
                    matched_count += 1
                else:
                    unmatched_count += 1
        
        color_msg(f"Matched genmuons: {matched_count}", "green")
        color_msg(f"Unmatched genmuons: {unmatched_count}", "yellow")

.. rubric:: Branch Structure in Dumped Files

Event-level branches
--------------------

- ``index``: Event index
- ``number``: Event number
- Other event attributes from your Event class

Particle-level branches
------------------------

For each particle type (e.g., "genmuons"):

- ``n_genmuons``: Number of genmuons (int)
- ``genmuons_<attr>``: Particle attribute vectors

  - Simple types: ``vector<int>``, ``vector<double>``, ``vector<bool>``
  - Nested vectors: ``vector<vector<T>>``
  - Strings: ``vector<string>``

Particle reference branches
----------------------------

- ``{ptype}_{attr}_indices``: Vector of vectors of indices
- ``{ptype}_{attr}_type``: Vector of particle type names
- ``{ptype}_{attr}_index``: Vector of single indices (for non-list references)

Example for genmuons with matched segments:

.. code-block:: text

    n_genmuons                              : int
    genmuons_pt                            : vector<double>
    genmuons_eta                           : vector<double>
    genmuons_matched_segments_indices      : vector<vector<int>>
    genmuons_matched_segments_type         : vector<string>

.. rubric:: Tips and Best Practices

1. **Memory Management**: For large datasets, process in batches using ``--maxevents``

   .. code-block:: bash

       dtpr dump-events -i input.root -o ./batch1 --maxevents 10000

2. **Selective Dumping**: Only dump particle types you need to save space

   .. code-block:: bash

       dtpr dump-events -i input.root -o ./results --particle-types genmuons segments

3. **Verification**: Always check the first few events after dumping

   .. code-block:: python

       from dtpr.utils.dumped_ntuple_reader import DumpedEventReader
       with DumpedEventReader("dumped_events.root") as reader:
           print(reader[0])  # Print first event to verify structure

4. **Compatibility**: Dumped files are standard ROOT files and can be read by any ROOT-based tool

.. rubric:: Limitations

- String attributes are stored but may have encoding limitations
- Complex nested structures (particles containing particles containing particles) may need special handling
- Circular references are not supported

.. rubric:: Troubleshooting

**Problem**: "Cannot find tree 'EVENTS' in file"

- Check tree name with ``--tree-name`` option
- Verify file was created successfully

**Problem**: Particle references not reconstructed

- Ensure both ``_indices`` and ``_type`` branches exist
- Check that particle type names match

**Problem**: Memory issues with large files

- Use ``--maxevents`` to process in smaller batches
- Use ``--particle-types`` to dump only needed particles

.. automodule:: dtpr.analysis.dump_events
    :members: dump_events
    :member-order: bysource
