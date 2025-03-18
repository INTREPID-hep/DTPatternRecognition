Event
=====

The ``Event`` class is designed to represent an event entry from a ROOT TTree, facilitating access
to information by abstracting info into instances of Python objects ( :doc:`../particles/main` ). 
There is a set of already defined particles in the ``dtpr.particles`` module. Each instance of the 
``Event`` class allows to comfortably access those objects such as offline segments, generation-level muons,
simulation digis, showers, and more.

The ``Event`` class can dynamically build particles based on the TTree event's entry information if 
specified through a **configuration file**. This file should contain information about the types
of particles and how to build them, allowing for flexible and customizable event processing.

To illustrate the dynamic particle building/ feature, consider the ``GenMuon`` class from the 
``dtpr.particles`` module. Suppose we want to create instances of ``GenMuon`` based
on the TTree event's entry when an event is instantiated. This requires specifying the following in 
a `YAML` configuration file under the ``particle_types`` section:

.. rubric:: config.yaml

.. code-block:: yaml

    particle_types:
        genmuons:
            class: 'dtpr.particles.gen_muon.GenMuon'
            n_branch_name: 'gen_nGenParts'
            conditioner: # optional
                property: 'gen_pdgId'
                condition: "==13"
            sorter: # optional
                by: 'pt'
                reverse: True

The ``Event`` class will create **n** (determined for the value of the ``gen_nGenParts`` branch) instances 
of the ``GenMuon`` class. The ``conditioner`` and ``sorter`` are optional. The former filters the TTree
event's entry based on specified conditions, and the ``sorter`` orders the created particles. Each 
particle class have a specific way to extract the information from the TTree event's entry (coded in
`_init_from_ev` class method). So, ensure that the information provided has the required branches to
build the particles. 

Then, the way to create an event instance having a TTree over which iterate, should be like this:

.. code-block:: python

    from dtpr.base import Event
    from dtpr.utils.config import RUN_CONFIG

    # First, you need to update the configuration file path. If not, 
    # it will work with the default one dtpr/utils/yamls/run_template.yaml.
    # You can also use the latter to define your own configuration file.
    RUN_CONFIG.change_config(config_path="path/to/config.yaml")

    for iev, ev in enumerate(tree):
        event = Event(ev, index=iev)
        # Print the event summary
        print(event)
        if iev == 1: break # Just to show the first two events

.. rubric:: Output

.. code-block:: text

    >> ------ Event 39956 info ------ 
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

The ``Event`` class is not limited to using TTree information. It can be used just like a container
by manually adding any type of attribute that you consider necessary. A simple example of this is, 
for instance, adding customized particles to the event. For practice, let us take
the class ``Shower`` from the ``dtpr.particles`` module just to illustrate how to add
objects to the event:

.. code-block:: python

    from dtpr.base import Event
    from dtpr.particles import Shower

    event = Event(index=0)
    showers = [Shower(index=i) for i in range(5)]
    event.showers = showers

    print(event)
    print(event.showers[-1])

.. rubric:: Output

.. code-block:: text

    >> ------ Event 0 info ------ 
    + Showers 
        * Number of showers: 5 
    >> Shower 4 info --> 
    + Wh: 1, Sc: 1, St: 1, Sl: None, Ndigis: None, Bx: None, Min_wire: None, Max_wire: None, Avg_pos: None, Avg_time: None, Wires_profile: []  

.. autoclass:: dtpr.base.Event
    :members:
    :private-members: _init_from_ev, _build_particles
    :special-members: __str__, __getter__,__setter__