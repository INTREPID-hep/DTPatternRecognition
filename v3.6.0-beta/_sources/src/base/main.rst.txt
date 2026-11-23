Base
====

The ``dtpr.base`` module provides a collection of core classes designed to simplify and enhance the manipulation
of **Event** data extracted from ROOT TTrees of **NTuples**. These classes enable efficient extraction,
transformation, and analysis of event information in a structured, object-oriented way.

The main abstractions are:

- **Particle**: Represents a single physics object (e.g., muon, electron) with its properties.
- **Event**: Aggregates particles and event-level information, providing an intuitive interface for event-by-event analysis.
- **NTuple**: Handles access to ROOT files and trees, mapping data into Event and Particle objects.
- **EventList**: Provides iterable access to collections of events.

.. important::
    The base classes handle reading and processing data from input files using a central configuration
    file, such as :download:`run_config.yaml <../../_static/run_config.yaml>`. Throughout the documentation,
    you will find details on the configurable parameters that allow support for various data formats.

.. rubric:: Visual Overview

.. mermaid::
    :name: dtpr_visual_overview
    :align: center
    :zoom: true

    flowchart TD
        A0["Particle Abstraction"]
        A1["Event Abstraction"]
        A2["NTuple Data Access"]
        A3["Configuration Management"]
        A4["Command Line Interface (CLI)"]
        A5["Event Preprocessing and Filtering"]
        A6["Histogram Filling"]
        A7["Matplotlib Plotting"]
        A8["Events Visualizer (GUI)"]
        A1 -- "Aggregates" --> A0
        A2 -- "Provides" --> A1
        A3 -- "Configures" --> A2
        A3 -- "Configures" --> A1
        A4 -- "Loads" --> A3
        A2 -- "Applies" --> A5
        A5 -- "Operates on" --> A1
        A6 -- "Processes" --> A1
        A3 -- "Defines" --> A6
        A4 -- "Invokes" --> A6
        A7 -- "Visualizes" --> A1
        A3 -- "Configures" --> A7
        A4 -- "Invokes" --> A7
        A8 -- "Inspects" --> A1
        A8 -- "Uses" --> A7
        A4 -- "Launches" --> A8

.. toctree::
    :maxdepth: 1
    :caption: Classes:

    particle_abstraction
    event_abstraction
    ntuple_data_access
    configuration_management