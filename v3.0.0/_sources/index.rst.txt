DT Pattern Recognition Package
==============================
This package is intended to serve as a base framework for Trigger pattern recognition processes in the CMS Drift Tubes system,
using as input data ``.root`` NTuples in a format similar to :download:`this one <./_static/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root>`.
Since the framework was developed in the context of the Drift Tube detectors, it also includes visualization tools considering
the geometrical features of the CMS DT system by using another auxiliary package `mplDTs <https://danielestrada971102.github.io/mplDTs>`_ 
and custom implementations in the ``dtpr/utils/dt_plot_functions.py`` module.

The core idea of this framework is to simplify the analysis of input data by adopting an event-by-event approach 
using Python classes. This approach makes the code more intuitive and readable. For example:

.. code-block:: python

   for muon in events.genmuons:
      if muon.pt > 5:
         print(muon.eta)

is much clearer compared to the traditional method:

.. code-block:: python

   for i in range(len(tree.gen_pdgID)):
      if tree.gen_pt[i] > 5:
         print(tree.gen_eta[i])

In addition to the event-by-event approach, the framework is designed to be highly flexible, aiming to support various 
data formats since the mapping of input data into Python classes is configurable through a ``YAML`` :download:`configuration file <./_static/run_config.yaml>`, as you will 
see along the different sections. The idea is that this allows users to adapt the framework to their specific needs.

Some useful, and as general as possible, tools using the framework have been implemented to facilitate some common tasks 
such as filling ROOT histograms, or inspecting or visualizing information from the NTuples. They are accessible through the 
``dtpr`` CLI command and are described in the :doc:`src/command_line_interface` section. Reading, manipulating, and mapping
the information from the input files into the respective classes are handled
by central classes, just go around the :doc:`src/base/main` section to know more about them.

Bear in mind that the package is under development, so feel free to contribute, report bugs, or suggest improvements.
Take a look at the `Contributors <https://github.com/DanielEstrada971102/DTPatternRecognition/blob/main/CONTRIBUTING.md>`_ 
and `Developers <https://github.com/DanielEstrada971102/DTPatternRecognition/blob/main/DEVELOPERS.md>`_ guides.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   src/installation
   src/base/main
   src/command_line_interface