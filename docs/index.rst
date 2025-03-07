DT Pattern Recognition Package
==============================
This package provides a set of base tools for implementing pattern recognition algorithms for the CMS DT system
using input data in NTuples format. It also includes visualization tools for DT Ntuples, taking into account
the geometrical features of the CMS DT system by using another auxiliary package `mplDTs <https://github.com/DanielEstrada971102/mplDTs>`_.

The main concept of this framework is to facilitate analysis on input data using an event-by-event approach
with Python classes. This allows for more intuitive and readable code, such as:

.. code-block:: python

   for muon in events.genmuons:
      if muon.pt > 5:
      print(muon.eta)

compared to:

.. code-block:: python

   for i in range(len(tree.gen_pdgID)):
   if tree.gen_pt[i] > 5:
      print(tree.gen_eta[i])


Some useful, and as general as possible, tools using the framework have been implemented to facilitate some common tasks 
such as fill ROOT histograms, or inspect or visualize information from the DT Ntuples. They are accessible through the
`dtpr` CLI command and are described in the :doc:`src/analysis/main` section. But, reading, 
manipulating, and mapping the information from the input files into the respective classes are handled
by central classes, just go  around the :doc:`src/base/main` section to know more about them.

The package is just in its early stages, so feel free to contribute, report bugs, or suggest improvements.
Take a look to the `Contributors <>` and `Developers <>` guides.


Installation
------------

First, download the source files or clone the repository:

.. code-block:: shell

   git clone https://github.com/DanielEstrada971102/DTPatternRecognition.git
   cd DTPatternRecognition

You can then install the package with pip by running:

.. code-block:: shell

   pip install .

To check if the package was installed successfully, run:

.. code-block:: shell

   pip show DTPatternRecognition

.. important::
   Be aware that the package requires PyROOT, so you should have it installed. If you are working in
   a Python virtual environment, ensure to include ROOT in it. To do this, use the command:

   .. code-block:: shell

      python -m venv --system-site-packages ROOT ENV_DIR[ENV_DIR ...]


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   src/analysis/main
   src/base/main
   src/particles/main
   src/utils/main