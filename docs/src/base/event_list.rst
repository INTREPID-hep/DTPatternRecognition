EventList
=========

A ROOT TTree can contain many events (entries), so, it is useful to have a class that manages 
a list of ``Event`` instances. The ``EventList`` class allows users to access specific events by index or slice. 
It is designed to handle ``Event`` instances created directly from a ROOT TTree. 

An ``EventList`` instance cannot be created by directly passing ``Event`` instances. This restriction exists 
to minimize memory usage. Instead of storing all events in memory, the class generates event instances 
on demand when the user requests a specific event or a subset of events.

To create an ``EventList`` instance, only the ROOT tree and an optional event preprocessing function 
are required. The preprocessing function is applied to each generated ``Event`` instance.

.. warning::
    This class is not intended to be instantiated directly by the user. It is used internally by the 
    :doc:`ntuple` class to manage events.

.. autoclass:: dtpr.base.EventList
    :members:
