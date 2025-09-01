Event Inspection
================

The event inspection tool provides a convenient way to examine 
the contents of specific events in your NTuples without writing a full analysis loop. 
This is especially useful for debugging, quick checks, or developing new analysis logic.

Instead of manually creating an ``NTuple`` and iterating over all events, the 
``inspect-events`` tool manages NTuple creation and event iteration for you. 
You can specify a single event, a slice of events, or all events to inspect. 
Additionally, you can define custom inspection functions in your configuration file, 
similar to how preprocessors and selectors are configured.

Custom inspector functions are defined in your configuration YAML 
under the ``inspector-functions`` key. Each function entry should specify 
the source (Python import path) and optional keyword arguments:

.. code-block:: yaml

    inspector-functions:
      func1:
        src: "dtpr.utils.inspector_functions.digi_inspector"
        kwargs:
          arg1: value1
      # Add more inspector functions as needed

These functions will be called for each event during inspection. If no inspector functions 
are specified, the tool will print a summary of the event by default.

.. rubric:: Usage

To inspect events, use the following command:

.. code-block:: bash

    dtpr inspect-events -i [INPATH] -cf [CONFIG] -evn [EVENT INDEX]

Where:

- ``[INPATH]`` is the path to the input folder containing the NTuples.

- ``[CONFIG]`` is the path to the configuration file.

- ``[EVENT INDEX]`` specifies the event index or slice to inspect (e.g., ``0``, ``10:20``, or ``-1`` for all events).

.. rubric:: How It Works

1. The tool loads your configuration and creates an ``NTuple`` instance for the specified input folder.
2. It retrieves the inspector functions from the configuration and prepares them for use.
3. It selects the requested event(s) by index or slice.
4. For each event:
   - If the event passes all filters and is not ``None``, each inspector function is called with the event as input.
   - If no inspector functions are defined, the event's string representation is printed.

.. rubric:: Example Inspector Function

Inspector functions receive the event as their first argument and can accept additional keyword arguments as specified in the configuration. For example:

.. code-block:: python

    # File: dtpr/utils/inspector_functions.py
    def digi_inspector(event, arg1=None, tqdm_pbar=None):
        print(f"Event {event.index}: Number of digis = {len(getattr(event, 'digis', []))}")
        # Additional inspection logic here

.. note::
    Inspector functions can also update the progress bar by accepting the ``tqdm_pbar`` argument.

.. rubric:: Example Output

.. code-block:: text

    Inspecting event 15 from NTuples
    Event 15: Number of digis = 87
    Event 15: Number of segments = 8
    ...
    Done!

This tool streamlines the process of event-by-event inspection, making it easy to apply custom 
logic or simply print event summaries for rapid debugging and validation.
