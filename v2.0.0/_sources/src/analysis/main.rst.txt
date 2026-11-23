analysis
========

This section provides an overview of the analysis tools available in the DT Pattern Recognition package.

An analysis in this context refers to a script that processes NTuple data on an event-by-event basis, leveraging 
the tools provided by ``dtpr.base``. These analyses are designed to offer reusable utilities that can be 
integrated into the central CLI tool ``dtpr``, which passes user-defined arguments to the analysis code.

To execute one of the pre-implemented analyses, use the following command:

.. code-block:: bash

    dtpr [ANALYSIS-NAME] [--OPTIONAL-ARGS]


The ``dtpr`` command invokes ``dtpr/cli.py``, making it straightforward to add new analyses for easy execution.

The positional argument ``ANALYSIS-NAME`` specifies the analysis to run. Currently, some available options include: 
``fill-histos``, ``plot-dts``, ``plot-dt``, and ``event-visualizer``. Optional arguments can 
be explored using the ``-h/--help`` flag. Key optional arguments include: ``-i/--inpath``, ``-o/--outpath``, 
``--maxfiles``, and ``--maxevents``. Refer to their respective sections for more details.

.. caution::

    As explained in the :doc:`../base/main` section, the framework is aimeded to be as generic as possible. 
    So, parameters such as the particle classes to construct in an Event or the preprocessors and selector 
    functions to apply, are defined in a ``YAML`` configuration file. Therefore, analyses also utilize this configuration 
    file to define parameters like which histograms to fill or which matplotlib styles to apply.


.. toctree::
    :maxdepth: 1
    :caption: Analyses:

    fill-histos
    plot-dts
    event-visualizer
    inspect-event


