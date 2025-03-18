EventList
=========

Since a ROOT TTree can contain many events (entries), it is useful to have a class that can manage 
a list of events, allowing the user to access a specific event by index or slice. The current version
of the ``EventList`` class is designed to handle ``Event`` instances created directly from a ROOT TTree.
Therefore, an instance of ``EventList`` cannot be created by directly passing ``Event`` instances. 

This restriction exists because the class aims to minimize memory allocation for events. Instead of storing 
all events in memory, it generates event instances on the fly when the user requests a specific event or 
a subset of them. 

With this in mind, an ``EventList`` instance requires only the ROOT tree and an optional event preprocessing 
function, which is applied to each created ``Event`` instance. By default, a dummy function is used that returns 
the event as is.

.. warning::
    Based on the description above, this class is not intended to be instantiated directly by the user. Instead, 
    it is used internally by the :doc:`ntuple` class to manage events.

.. autoclass:: dtpr.base.EventList
    :members:
