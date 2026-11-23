base
====

The ``dtpr.base`` module provides a collection of classes designed to simplify and enhance the manipulation
of **Event** data extracted from ROOT TTrees of **NTuples**. These classes enable efficient extraction,
transformation, and analysis of event information.

.. important::
   The base classes handle reading and processing data from input files using a central configuration
   file, such as :download:`run_config.yaml <../../_static/run_config.yaml>`. Throughout the documentation,
   you will find details on the configurable parameters that allow support for various data formats.

.. toctree::
   :maxdepth: 1
   :caption: Classes:

   particle
   event
   ntuple
   event_list



