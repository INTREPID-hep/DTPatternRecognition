particles
=========

In the context of ``dtpr``, a particle is a class that represents any object you would like to
add to an :doc:`Event <../base/event>` instance. The purpose of defining a particle is to easily access the 
information of ``TBranches`` and to have the possibility to build them on the fly each time an Event
instance is produced. However, you can also manually add a particle to an event instance if desired by passing
the corresponding attributes.

Every particle class should inherit from the base class :ref:`Particle` and implement the method
that handles the initialization of attributes from TBranches of a supplied TTree entry, ``_init_from_ev``.
Classes that inherit from ``Particle`` ensure that the particle class has a custom ``__str__`` method that returns a string
using the ``color_msg`` function from ``dtpr.utils.functions`` to color the output.

You can generate a skeleton for a particle class using the dtpr command as follows:

.. code-block:: bash

    dtpr create-particle -o [output_folder] --name TestParticle

This will create a new file called ``testparticle.py`` in the specified output folder with the following
content:

.. code-block:: python

    from dtpr.particles import Particle

    class TestParticle(Particle):
        def __init__(self, index, ev=None, **kwargs):
        """
        Initialize a TestParticle instance.

        description here...

        :param index: The index of the TestParticle.
        :type index: int
        :param ev: The root event object containing event data.
        :param kwargs: Additional attributes to set explicitly.
        .
        . (add more parameters here if needed)
        .
        """
        # Initialize the attributes of the instance with the input arguments
        # self.attr1 = kwargs.pop("attr1", None)
        # .
        # (add more attrributes here if needed)
        # .
        # .

        super().__init__(index, ev, **kwargs)


    def _init_from_ev(self, ev):
        # constructor with root event entry info
        # Extract the need attributes from the root event entry
        # and assign them to the corresponding attributes of the instance
        try:
            pass 
        except (AttributeError, IndexError):
            raise ValueError("The provided event does not contain information.")


    if __name__ == '__main__':
        # Test the class here
        particle_instance = TestParticle(1)

        print(particle_instance)

To have your particle type generated on the fly when Event instances are created, you need to add
the necessary information into an event configuration file as described in the :doc:`../base/event` 
section. You can also generate a copy of the base event configuration file using the ``dtpr`` bash 
command as follows:

.. code-block:: bash

    dtpr create-config -o [output_folder]

This will create a copy of the base file ``dtpr/utils/yamls/run_config.yaml`` in the specified output 
folder. Then, add your particle to your configuration file and ensure that the correct file path is set
in `RUN_CONFIG` by doing:

.. code-block:: python

    from dtpr.utils.config import RUN_CONFIG

    RUN_CONFIG.change_config(config_path="path/to/event_config.yaml")
    # your code here...


The following particles are available in the package:

.. toctree::
    :maxdepth: 1
    :caption: Classes:

    gen_muon
    digi
    g4digi
    segment
    ph2TP
    shower
    simhit

Particle
--------

.. autoclass:: dtpr.particles.Particle
    :members:
    :private-members: _init_from_ev
    :special-members: __str__, __init__