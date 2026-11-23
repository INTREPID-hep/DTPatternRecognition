NTuple Data Access
===================

The ``NTuple`` class serves as a gateway for accessing information from one or more ``.root`` NTuples located in a specified input path. It provides an interface to build ``Event`` instances directly from TTree event entries, supporting preprocessing, filtering, and dynamic event construction according to the **preprocessors** and **selectors** specified in the configuration.

A **preprocessor** is a function that takes an ``Event`` instance as input and applies any necessary modifications or additions. A **selector** is a function that returns a boolean based on the information of an ``Event``, allowing for event filtering.

The ``NTuple`` class is designed to be generic and handle many types of NTuple formats, since ``Event`` creation is controlled through a ``YAML`` **configuration file**. Internally, the ``NTuple`` class loads the ROOT TTree via a TChain (accessible via the ``tree`` attribute), and generates ``Event`` instances on demand (via the ``events`` attribute), applying preprocessors and selectors before returning each event.

The preprocessing feature allows selectors to be based on properties not present in the input NTuples, but computed via extra preprocessing steps.

The ``NTuple`` class supports the following configuration keys:

- ``ntuple_tree_name``: The name of the TTree to use (should be the same for all files in the input folder).
- ``ntuple_preprocessors``:  A map of preprocessors to use in the NTuple.
- ``ntuple_selectors``: A map of selectors to use in the NTuple.

Both preprocessors and selectors maps should contain a ``src`` key specifying the path to the function, and optionally a ``kwargs`` map for additional parameters.

.. rubric:: Example configuration

.. code-block:: yaml

    ntuple_tree_name: '/dtNtupleProducer/DTTREE'

    ntuple_preprocessors:
      test-preprocessor:
        src: 'dtpr.utils.preprocessors.test_preprocessor'
        kwargs:
          dummy_val: -999

    ntuple_selectors:
      test-selector:
        src: 'dtpr.utils.selectors.test_selector'

.. _preprocessor_selector_examples:

Example: Preprocessor and Selector Functions
--------------------------------------------

A preprocessor can be used to compute new event-level quantities before selection or analysis. For example, to compute the deltaR between the two generator muons and store it as an attribute in the event:

.. literalinclude:: ../../../dtpr/utils/preprocessors.py
    :language: python
    :lines: 1-10

A selector can be used to filter events, for example, to keep only those with at least two generator muons:

.. literalinclude:: ../../../dtpr/utils/selectors.py
    :language: python
    :lines: 1-7

How Data is Accessed
--------------------

The ``NTuple`` provides efficient, memory-friendly access to event data. The ``NTuple`` acts as a "Data Hub"
and an auxiliary class, the ``EventList`` (see below :ref:`event_list`), as an "Event Factory", ensuring that
only the requested events are loaded and processed.

.. mermaid::

    sequenceDiagram
        participant User
        participant NTuple
        participant ROOT_TChain
        participant EventList
        participant Event

        User->>NTuple: Create NTuple(inputFolder="path/to/data")
        NTuple->>NTuple: Find .root files in inputFolder
        NTuple->>ROOT_TChain: Create TChain (from ROOT library)
        NTuple->>ROOT_TChain: Add each .root file to TChain
        NTuple-->>User: NTuple object is ready (contains EventList)

        User->>EventList: Request an Event (e.g., ntuple.events[5])
        EventList->>ROOT_TChain: Ask for raw data entry #5
        ROOT_TChain-->>EventList: Provides raw data (ev_entry)
        EventList->>Event: Create Event(ev_entry, index=5, use_config=True)
        Note over Event: Event dynamically builds Particles based on config
        EventList->>NTuple: Call NTuple's 'event_processor(Event)'
        Note over NTuple: Applies configured Preprocessors & Selectors
        NTuple-->>EventList: Returns processed Event (or None if filtered)
        EventList-->>User: Return the fully processed Event

**Flow Explanation:**

1. **NTuple Initialization:**  
   When an ``NTuple`` object is created, it scans the input folder for all ``.root`` files and constructs a ROOT ``TChain`` to combine them. It then creates an ``EventList`` instance, passing it the configured ``TChain`` and its own event processing method.

2. **Requesting an Event:**  
   When a specific event is requested (e.g., ``ntuple.events[5]``), the ``EventList`` asks the ``TChain`` to load only the raw data for that event. It then creates an ``Event`` object, which dynamically builds its ``Particle`` collections according to the configuration. Before returning the event, any configured preprocessors and selectors are applied.

3. **Efficient Access:**  
   This design ensures that only the requested events are loaded and processed, minimizing memory usage and allowing efficient access to large datasets.

.. rubric:: Example usage

The following example shows how to use the ``NTuple`` class to read DT Ntuples and access events:

.. literalinclude:: ../../../dtpr/base/ntuple.py
    :language: python
    :dedent:
    :start-after: [start-example-1]

.. rubric:: Output

.. code-block:: text

    + Opening input file /root/DTPatternRecognition/test/ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root
    >> ------ Event 39954 info ------
    + Index: 9
    + Digis
        * Number of digis: 167
    + Segments
        * Number of segments: 21
        * AM-Seg matches:
        --> Segment 1 info -->
            - Wh: -2, Sc: 5, St: 1, Phi: 2.2211668491363525, Eta: -0.9381515979766846
        --> Segment 2 info -->
            - Wh: -2, Sc: 5, St: 1, Phi: 2.2211666107177734, Eta: -0.9440188407897949
        --> Segment 8 info -->
            - Wh: -2, Sc: 5, St: 2, Phi: 2.221505880355835, Eta: -0.9430407881736755
        --> Segment 9 info -->
            - Wh: -2, Sc: 5, St: 2, Phi: 2.2215068340301514, Eta: -0.9163724184036255
        --> Segment 10 info -->
            - Wh: -2, Sc: 12, St: 2, Phi: -0.6958962082862854, Eta: -0.8441917300224304
    + Tps
        * Number of tps: 32
    + Genmuons
        * Number of genmuons: 2
        * GenMuon 1 info -->
        --> Pt: 548.1250610351562, Eta: -0.8365859985351562, Phi: -0.6915670037269592, Charge: 1, Matched_segments_stations: [...], Showered: True
        * GenMuon 0 info -->
        --> Pt: 511.7483215332031, Eta: -0.9357022643089294, Phi: 2.2167818546295166, Charge: -1, Matched_segments_stations: [...], Showered: True
    + Emushowers
        * Number of emushowers: 1
    + Simhits
        * Number of simhits: 188
    + Realshowers
        * Number of realshowers: 3
    <generator object EventList.__getitem__.<locals>.<genexpr> at 0x...>
    >> ------ Event 39956 info ------
    + Index: 0
    + Digis
        * Number of digis: 81
    + Segments
        * Number of segments: 8
        * AM-Seg matches:
        --> Segment 5 info -->
            - Wh: 0, Sc: 8, St: 1, Phi: -2.5716519355773926, Eta: -0.2304154634475708
        --> Segment 7 info -->
            - Wh: -1, Sc: 8, St: 4, Phi: -2.5743513107299805, Eta: -0.36347508430480957
    + Tps
        * Number of tps: 21
    + Genmuons
        * Number of genmuons: 2
        * GenMuon 0 info -->
        --> Pt: 258.0812683105469, Eta: -1.9664770364761353, Phi: 0.5708979964256287, Charge: -1, Matched_segments_stations: [...], Showered: True
        * GenMuon 1 info -->
        --> Pt: 190.72511291503906, Eta: -0.2504693865776062, Phi: -2.558511257171631, Charge: 1, Matched_segments_stations: [...], Showered: True
    + Emushowers
        * Number of emushowers: 0
    + Simhits
        * Number of simhits: 50
    + Realshowers
        * Number of realshowers: 0
    Event orbit number: -1

.. important::

    The ``NTuple.events`` attribute is not a simple list, but an instance of the :ref:`event_list` class.
    This design prevents loading all events into memory simultaneously. Instead, it allows iteration and access by index and slice,
    while internally iterating over the root tree entries to create the required event on the fly.

.. _event_list:

EventList
---------

Since a ROOT TTree can contain many events (entries), it is useful to have a class that manages a
list of ``Event`` instances. The ``EventList`` class allows users to access specific events by index 
or slice, and is designed to handle ``Event`` instances created directly from a ROOT TTree.

An ``EventList`` instance cannot be created by directly passing ``Event`` instances. This restriction
exists to minimize memory usage. Instead of storing all events in memory, the class generates event
instances on demand when the user requests a specific event or a subset of events using the TTree.

To create an ``EventList`` instance, only the ROOT tree and an optional event preprocessing function
are required. The preprocessing function is applied to each generated ``Event`` instance.

.. warning::
    This class is not intended to be instantiated directly by the user. It is used internally by the 
    ``NTuple`` class to manage events.

.. autoclass:: dtpr.base.EventList
    :members: