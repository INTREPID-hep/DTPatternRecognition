NTuple
========

The ``NTuple`` class serves as a simple gateway to access information from single or multiple `.root` 
Ntuples (flat) located in the specified input path. It represents an interface to build ``Event`` instances
directly from the TTree event entries. This class is capable of preprocessing, filtering and building ``Event`` 
instances according to the **preprocessors**, and **selectors** passed.

A preprocessor refers to a function that takes an ``Event`` instance as input and returns the same 
``Event`` instance after applying any necessary modifications or additions. A selector refers to a 
function that returns a boolean based on the information of an ``Event``.

The ``NTuple`` class is designed to be generic and handle many types of NTuple since ``Event`` creation can be
controlled through **particle classes** definitions and **configuration files**. The ``NTuple`` class only loads
the ROOT TTrees in a TChain (accessible through the attribute ``tree``), and generates ``Event`` instances
on the fly (accessible through the attribute ``events``), applying before returning, the ``NTuple._event_preprocessor``
method to perform any necessary processing and selecting steps.

This was implemented in that way to allow creating selectors based on properties that by default
do not come directly from the input NTuples, but can be computed with extra preprocess functions
steps before applying the selection.

The following example shows how to use the ``NTuple`` class to read DT Ntuples and access to events.

.. code-block:: python

    # ...
    from dtpr.utils.config import RUN_CONFIG

    # First, you need to update the configuration file path since Events creation
    # requires it, If not, it will work with the default one dtpr/utils/yamls/run_template.yaml.
    RUN_CONFIG.change_config(config_path="path/to/config.yaml")

    # path to the DT Ntuple, it could be a folder with several files
    input_folder = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "../../test/ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root",
            )
        )

    ntuple = NTuple(input_folder, tree_name="/dtNtupleProducer/DTTREE")
    # you can access events by index or slice such as a list
    print(ntuple.events[10])
    print(ntuple.events[3:5]) # slicing return a generator

    # or loop over the events
    for iev, ev in enumerate(ntuple.events):
        print(ev)
        break

    # or simply use the TTree
    for i, ev in enumerate(ntuple.tree):
        print(ev.event_orbitNumber)
        break

.. rubric:: Output

.. code-block:: text

    + Opening input file /root/DTPatternRecognition/test/ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root
    >> ------ Event 39963 info ------
    + Index: 10
    + Digis
        * Number of digis: 105
    + Segments
        * Number of segments: 14
    + Tps
        * Number of tps: 20
    + Genmuons
        * Number of genmuons: 2
        * GenMuon 1 info -->
        --> Pt: 28.614336013793945, Eta: 4.255909442901611, Phi: -1.395720362663269, Charge: 1, Matched_segments_stations: [], Showered: False
        * GenMuon 0 info -->
        --> Pt: 23.001548767089844, Eta: 2.4365286827087402, Phi: 1.7807667255401611, Charge: -1, Matched_segments_stations: [], Showered: False
    + Emushowers
        * Number of emushowers: 1
    + Simhits
        * Number of simhits: 0
    <generator object EventList.__getitem__.<locals>.<genexpr> at 0x7f00c424b440>
    >> ------ Event 39956 info ------
    + Index: 0
    + Digis
        * Number of digis: 81
    + Segments
        * Number of segments: 8
    + Tps
        * Number of tps: 21
    + Genmuons
        * Number of genmuons: 2
        * GenMuon 0 info -->
        --> Pt: 258.0812683105469, Eta: -1.9664770364761353, Phi: 0.5708979964256287, Charge: -1, Matched_segments_stations: [], Showered: False
        * GenMuon 1 info -->
        --> Pt: 190.72511291503906, Eta: -0.2504693865776062, Phi: -2.558511257171631, Charge: 1, Matched_segments_stations: [], Showered: False
    + Emushowers
        * Number of emushowers: 0
    + Simhits
        * Number of simhits: 50
    Event orbit number: -1

.. important::
    
    The ``NTuple.events`` attribute is not a simple list, but an instance of the :doc:`event_list` class.
    This design prevents loading all events into memory simultaneously. Instead, it allows iteration and access by index and slice,
    while internally iterating over the root tree entries to create the required event on the fly.

Previous example does not use any preprocessor or selector, but you can define them and pass as 
arguments to the ``NTuple`` class. But, since Events creation requires from a config file, an already 
defined function (``init_ntuple_from_config``) that instantiates the NTuple class and passes 
preprocessors and selectors taken from the ``YAML`` configuration file is available in ``dtpr.utils.functions``.
This only requires specifying the following in the configuration file under ``ntuple_preprocessors``
and ``ntuple_selectors`` sections:

.. rubric:: config.yaml

.. code-block:: yaml

    ntuple_selectors:
    - "dtpr.utils.filters.base_filters.baseline"
    # Add other as needed ....

    ntuple_preprocessors:
        genmuon_matcher:
            src: "dtpr.utils.genmuon_functions.analyze_genmuon_matches"
        genmuon_showerer:
            src: "dtpr.utils.genmuon_functions.analyze_genmuon_showers"
            kwargs: # if the function requires arguments...
                method: 2
        # ...
    # ...


and then, insted of creating the NTuple instance directly, you can use the function after updating the configuration file path.

.. code-block:: python

    from dtpr.utils.functions import init_ntuple_from_config

    RUN_CONFIG.change_config(config_path="path/to/config.yaml")
    # ntuple = NTuple(input_folder, tree_name="/dtNtupleProducer/DTTREE")
    ntuple = init_ntuple_from_config(input_folder, tree_name="/dtNtupleProducer/DTTREE")

    # ...

.. autoclass:: dtpr.base.NTuple