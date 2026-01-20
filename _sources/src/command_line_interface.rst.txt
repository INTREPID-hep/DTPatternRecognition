Command Line Interface (CLI)
============================

``dtpr`` is the primary way to interact with the DT Pattern Recognition framework.
It provides a unified entry point for running analyses, plotting, inspecting events, and
generating templates, all configured through YAML files for maximum flexibility.

An analysis-tool in this context refers to a script that processes NTuple data on an event-by-event
basis, leveraging the methods and classes provided by ``dtpr.base``. These tools are designed to be
reusable and are integrated into the central CLI entry point ``dtpr``, which passes user arguments
to the analysis-tool codes interactively.

To run an analysis, simply set up your configuration YAML file and execute one of the pre-implemented
analysis tools using:

.. code-block:: bash

    dtpr [ANALYSIS-NAME] [--OPTIONAL-ARGS]

The ``dtpr`` command invokes ``dtpr/cli.py``, making it straightforward to add new analyses for easy execution.

The positional argument ``ANALYSIS-NAME`` specifies the analysis-tool to run. Available options include: 
``fill-histos``, ``plot-dts``, ``plot-dt``, ``event-visualizer``, and more. Optional arguments can be 
explored using the ``-h/--help`` flag. Key optional arguments include: ``-i/--inpath``, ``-o/--outpath``, 
``--maxfiles``, and ``--maxevents``. Refer to their respective sections for more details.

.. tip::
    The configuration file also defines parameters such as which histograms to fill or which matplotlib styles to apply.

.. rubric:: How the CLI Works

The CLI is designed for flexibility and maintainability. Its structure and available commands are defined
in a dedicated YAML file: ``dtpr/utils/yamls/config_cli.yaml``. This approach allows you to easily add
or modify commands and arguments without changing Python code.

The workflow is as follows:

1. **You Type a Command:**  
   For example, you run ``dtpr fill-histos -cf custom.yaml -i mydata.root`` in your terminal.

2. **CLI Script is Executed:**  
   The system runs the main Python script for the CLI, ``dtpr/cli.py``.

3. **CLI Structure Loaded from YAML:**  
   The CLI uses the ``argparse`` library to define all available subcommands and their arguments. 
   These definitions are loaded from ``config_cli.yaml``, making the CLI structure easily configurable.

4. **Arguments are Parsed:**  
   ``argparse`` reads your command, matches the subcommand, and extracts all argument values.

5. **Configuration is Updated:**  
   If you specify a configuration file with ``-cf`` or ``--config-file``, the CLI updates the global
   ``RUN_CONFIG`` object to use your chosen YAML file. All subsequent operations will use your custom settings.

6. **Analysis Function is Called:**  
   The CLI knows which Python function to call for each subcommand (e.g., 
   ``dtpr.analysis.fill_histograms.fill_histos``) and executes it, passing all relevant arguments.

.. mermaid::
    :name: cli_view
    :align: center
    :zoom: true

    sequenceDiagram
        participant User
        participant dtpr CLI
        participant config_cli.yaml
        participant RUN_CONFIG
        participant Analysis Function

        User->>dtpr CLI: dtpr fill-histos -cf custom.yaml -i mydata.root
        dtpr CLI->>dtpr CLI: (Internal argparse setup)
        dtpr CLI->>config_cli.yaml: Load CLI structure (subcommands, args)
        config_cli.yaml-->>dtpr CLI: Returns CLI definitions
        dtpr CLI->>dtpr CLI: Parse user command based on definitions

        dtpr CLI->>RUN_CONFIG: Call change_config_file("custom.yaml")
        RUN_CONFIG->>RUN_CONFIG: Delete old settings
        RUN_CONFIG->>config_cli.yaml: Load custom.yaml (for RUN_CONFIG settings)
        config_cli.yaml-->>RUN_CONFIG: Provides new settings
        RUN_CONFIG-->>dtpr CLI: Configuration updated

        dtpr CLI->>Analysis Function: Call fill_histos(inpath="mydata.root", ...)
        Note over Analysis Function: This function now uses the new RUN_CONFIG
        Analysis Function-->>dtpr CLI: Task completed
        dtpr CLI-->>User: Output messages

.. rubric:: Available Analyses and Tools

The following analyses and utilities are available via the CLI:

.. toctree::
    :maxdepth: 1
    :caption: Analyses:

    histogram_filling
    events_inspection
    matplotlib_plotting
    events_visualizer_gui
    dump_events_guide

For a full list of commands and options, run:

.. code-block:: bash

    dtpr --help
    dtpr [ANALYSIS-NAME]


