Particle
========

The ``Particle`` class serves as a base class designed to simplify the process of converting NTuple information into a Python object. 
It offers a flexible approach to initializing particle attributes directly from the branches of TTree event entries or 
from keyword arguments (kwargs) that provide values, evaluable expressions, or paths to callable methods for determining the attributes. 
If specific class methods are required, you can always create a new class that inherits from this base class and invoke its constructor.

The following example demonstrates two different ways to create a particle instance:

.. literalinclude:: ../../../dtpr/base/particle.py
    :language: python
    :lines: 171-190
    :dedent:

.. rubric:: Output

.. code-block:: text

    >> Particle 0 info -->
    + Wh: -2, Sc: 1, St: 1, Detector_side: -z
    >> GenMuon 0 info -->
    + Pt: 258.0812683105469, Eta: -1.9664770364761353, Phi: 0.5708979964256287, Charge: -1

There are some specific classes already implemented (see :doc:`particles <../particles/main>`) that define their own useful class methods, 
you can take them as reference. If you want to implement your own class, you can also use the ``dtpr`` CLI command
to generate a skeleton for a particle that inherits from the ``Particle`` class as follows:

.. code-block:: bash

    dtpr create-particle -o [output_folder] --name TestParticle

This will create a new file called ``testparticle.py`` in the specified output folder with the following
content:

.. literalinclude:: ../../_static/testparticle.py
    :language: python

By inheriting from ``Particle``, the class gains the ability to initialize its attributes seamlessly
from TTree branches or dictionary-like mappings. Additionally, it includes a custom ``__str__`` method
that provides a visually enhanced string representation by utilizing the ``color_msg`` function from
``dtpr.utils.functions`` to apply color formatting to the output. The class is also equipped with a
``__eq__`` method, enabling comparison between two instances.

.. autoclass:: dtpr.base.Particle
    :members:
    :member-order: bysource
    :private-members: _init_from_ev, _init_from_dict
    :special-members: __init__, __str__, __eq__