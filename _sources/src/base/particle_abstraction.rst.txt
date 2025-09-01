Particle Abstraction
=====================

The ``Particle`` class is a flexible base class for representing any object with properties in your
analysis—such as physics particles (muons, electrons) or detector objects (hits, segments). It allows
you to easily convert NTuple data into Python objects by initializing attributes from TTree branches,
keyword arguments, or expressions. You can also extend this class to create your own custom particle types.

.. rubric:: Example

A ``Particle`` object can be created from scratch, defining its properties directly:

.. code-block:: python

    from dtpr.base import Particle

    # Create a Particle instance named "Shower" with an index and some attributes
    my_shower = Particle(index=0, wh=1, sc=3, st=4, nDigis=10, name="Shower")

    # Print the particle's summary
    print(my_shower)

.. rubric:: Output

.. code-block:: text

    >> Shower 0 info -->
        + Wh: 1, Sc: 3, St: 4, Ndigis: 10

In this example:
- ``my_shower`` is an instance of the ``Particle`` class.
- ``index=0`` gives it a unique identifier.
- ``wh=1``, ``sc=3``, ``st=4``, ``nDigis=10`` are custom attributes representing properties such as 
the "wheel", "sector", "station" (detector locations), and "number of digis" (number of hits).
- ``name="Shower"`` assigns a descriptive name to the particle.

This ``Particle`` object encapsulates all relevant information about the detected shower.

Dynamic Attribute Initialization
--------------------------------

The core functionality of the ``Particle`` class is implemented in the ``_init_from_dict`` method, which 
allows attributes to be defined dynamically. For each attribute, its value can be obtained from several sources:

+------------+-----------------------------------------------------------------------+----------------------------------------+
| Property   | Description                                                           | Example                                |
+============+=======================================================================+========================================+
| ``branch`` | Read directly from a raw data branch (e.g., from an NTuple).          | ``'digi_wheel'``                       |
+------------+-----------------------------------------------------------------------+----------------------------------------+
| ``expr``   | Calculate using a Python expression.                                  | ``'time // 25'``                       |
+------------+-----------------------------------------------------------------------+----------------------------------------+
| ``src``    | Call a Python function, passing the ``Particle`` itself to a callable.| ``'path.to.module.get_detector_side'`` |
+------------+-----------------------------------------------------------------------+----------------------------------------+
| ``type``   | Optionally convert the value to a specific data type.                 | ``'int'``                              |
+------------+-----------------------------------------------------------------------+----------------------------------------+

Understanding this mechanism enables the definition of particle initialization through the configuration file or passing directly.
the dictionary-like structure to read from a ROOT TTree entry.

.. literalinclude:: ../../../dtpr/base/particle.py
    :language: python
    :lines: 181-205
    :dedent:

.. rubric:: Output

.. code-block:: text

    >> GenMuon 0 info -->
        + Pt: 258.0812683105469, Eta: -1.9664770364761353, Phi: 0.5708979964256287, Charge: -1

The following snippet from ``run_config.yaml`` demonstrates how "digis" (detector hits) are defined using these options:

.. code-block:: yaml

    # File: dtpr/utils/yamls/run_config.yaml
    particle_types:
      digis:
        amount: 'digi_nDigis' # How many digis to create, read from 'digi_nDigis' branch
        attributes:
          wh:
            branch: 'digi_wheel' # 'wh' attribute comes from 'digi_wheel' branch
          sc:
            branch: 'digi_sector'
          # ... other branch-based attributes ...
          time:
            branch: 'digi_time'
          BX:
            expr: 'time // 25 if time is not None else None' # 'BX' computed from 'time'

In this configuration:

- The ``digis`` particle type is configured to create as many instances as indicated by the ``digi_nDigis`` entry in the raw data.
- For each digi, its ``wh`` (wheel) attribute is read directly from the ``digi_wheel`` branch.
- The ``BX`` (Bunch Crossing) attribute is computed using a Python expression that divides the ``time`` attribute by 25, illustrating how new information can be derived from existing attributes.

Custom Particle Classes
-----------------------

A custom particle class skeleton can be generated using the CLI:

.. code-block:: bash

    dtpr create-particle -o [output_folder] --name TestParticle

This command creates a file ``testparticle.py`` with a template for your new class.

.. literalinclude:: ../../_static/testparticle.py
    :language: python

By inheriting from ``Particle``, your class can initialize attributes from NTuple data and provides 
useful methods such as a colored string representation and equality comparison.

.. autoclass:: dtpr.base.Particle
    :members:
    :member-order: bysource
    :private-members: _init_from_ev, _init_from_dict
    :special-members: __init__,