Event Abstraction
=================

The ``Event`` class is designed to encapsulate the information of an event entry from a ROOT TTree, facilitating access
to it by abstracting into collections of :doc:`Particles <particle_abstraction>` instances and data members representing event metadata
(such as an index, event number, etc.).

Thanks to the design of the ``Particle`` class, the ``Event`` class can dynamically build particles
based on specifications from a **configuration file** as described in the :doc:`particle_abstraction`. 
This file should contain information about the types of particles and how to build them, allowing for flexible and customizable event processing.

The dynamic particle-building feature searches in the config file for the map with key ``particle_types``.
This should list each type of particle to be created with its respective specifications (see :doc:`particle_abstraction` for a specific example).

.. rubric:: Example ``particle_types`` structure in ``run_config.yaml``

.. code-block:: yaml

    particle_types:
      # ...
      particle_name:
        class: 'path.to.particle_class'  # Optional: custom class, defaults to 'dtpr.particles.Particle'
        amount: (int, TBranch<int> or TBranch<vector>)  # How many particles to build per event
        attributes:
          attr_name:
            branch: 'branch_name'        # Directly from a TTree branch
            expr: 'expression'           # Python expression using other attributes
            src: 'path.to.function'      # Callable, receives the particle as input
        filter: 'expression'             # Optional: filter particles using 'p' (particle) and 'ev' (event)
        sorter:
          by: 'expression'               # Sorting key, can use 'p' and 'ev'
          reverse: True/False            # Optional: reverse sorting order

The ``Event`` class creates **n** instances of the ``Particle`` class, where **n** is determined by the ``amount`` key. 
Attributes are defined using the ``branch`` (direct mapping from TTree branches), ``expr`` (computed values using expressions), or ``src`` (callable methods). 
The callable methods must accept the particle instance as an argument, and the expression can access the particle 
instance attributes for any computation.

The ``particle_types`` map can also manage the following keys:

- ``class``: Specifies the class path to be used for the particle. If not specified, it defaults to the ``Particle`` class.
- ``filter``: Allows filtering of particles based on boolean conditions. These conditions can use the particle instance (referred to as ``p``) and the root event entry (referred to as ``ev``).
- ``sorter``: Sorts particles using Python's ``sorted()``. The ``by`` key specifies the sorting expression (which can also use ``p`` and ``ev``), and the ``reverse`` key (default: ``False``) can reverse the order.

Internally, all particle collections are stored in a dictionary (``self._particles``) within the ``Event`` object. But, since
the ``__getattr__`` and ``__setattr__`` methods are overridden, you can access collections like ``event.digis`` directly.

.. rubric:: Example usage

To iterate over a TTree and create event instances with particles specified in the 
:download:`configuration file <../../_static/run_config.yaml>`, you can use the following code:

.. literalinclude:: ../../../dtpr/base/event.py
    :language: python
    :dedent:
    :start-after: [start-example-2]

.. rubric:: Output

.. code-block:: text

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

.. rubric:: How this Event is Built

The process of building the ``Event`` and populating it with ``Particle`` objects can be visualized as follows:

.. mermaid::
    :name: event_building_process
    :align: center
    :zoom: true

    sequenceDiagram
        participant User
        participant Event
        participant ROOTEntry
        participant RUN_CONFIG
        participant Particle

        User->>Event: Create Event(index=0, ev=ROOTEntry, use_config=True)
        Event->>RUN_CONFIG: Request particle_types configuration
        Note over RUN_CONFIG: (e.g., defines "digis", "segments", "genmuons")
        loop For each configured particle type (e.g., "digis")
            Event->>ROOTEntry: Get number of instances (e.g., "digi_nDigis")
            Note over ROOTEntry: Returns N for "digis"
            loop For each instance (e.g., digi 0 to N-1)
                Event->>Particle: new Particle(index=i, ev=ROOTEntry, attributes={...})
                Note over Particle: Particle reads its attributes from ROOTEntry based on config
                Particle-->>Event: Return created Particle object
            end
            Event->>Event: Store list of built particles (e.g., self.digis = [...])
        end
        Event-->>User: Return fully populated Event

**Flow Explanation:**

1.  **User Initiates Event:** The user creates an ``Event`` object, providing it with a raw data entry from the NTuple and ``use_config=True``.
2.  **Event Consults Configuration:** The ``Event`` looks to the ``RUN_CONFIG`` to determine which particle types to build and how to build them.
3.  **Dynamic Particle Creation:** For each particle type defined in the configuration:
    - The ``Event`` determines how many particles of this type exist in the current raw data entry.
    - For each particle, the ``Event`` creates a new ``Particle`` (or custom class) instance, which reads its attributes from the raw data or computes them as specified.
    - All created particles are collected into a list and stored as an attribute of the ``Event`` (e.g., ``event.digis``).
4.  **Event Ready:** After all particle types are built, the ``Event`` object is ready for analysis.



On the other hand, the ``Event`` class is highly flexible and not restricted to TTree data. As demonstrated above, you can
define any type of attribute mapping in the configuration file or even create an empty event instance and then add attributes manually as needed.

.. rubric:: Example: manual event creation

.. literalinclude:: ../../../dtpr/base/event.py
    :language: python
    :dedent:
    :start-after: [start-example-1]
    :end-before: [end-example-1]

.. rubric:: Output

.. code-block:: text

    >> ------ Event 1 info ------
        + Index: 1
    >> ------ Event 1 info ------
        + Index: 1
        + Showers
           * Number of showers: 5
    >> Shower 4 info -->
        + Wh: 1, Sc: 1, St: 1

.. autoclass:: dtpr.base.Event
    :members:
    :member-order: bysource
    :private-members: _build_particles
    :special-members: __str__, __getter__,__setter__