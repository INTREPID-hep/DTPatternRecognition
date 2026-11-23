inspect-event
=============

Sometimes, only minimal checks or inspections over events are required. So, creating a code to use an NTuple instance 
and looping through all the events each time can be unnecessary and inefficient in such cases. This "analysis" 
module simplifies the process by managing the creation of an NTuple instance and iterating over the 
requested events (specified by index or slice). It also allows applying custom functions to the events 
by including them in the configuration file, similar to how preprocessors and selectors are defined 
(see :doc:`../base/ntuple`). The configuration format is as follows:

.. code-block:: yaml

    # ...
    inspector-functions:
    # Define the functions to be used in the event inspector in the following format:
    func1: # Name of the function (not relevant to the code)
        src: "dtpr.utils.inspector_functions.digi_inspector"
        kwargs:
            arg1: value1
        # Additional arguments can be added here
    # ...

To run the inspection, use the following command:

.. code-block:: bash

    dtpr inspect-event -i [INPATH] -cf [CONFIG] -evn [EVENT INDEX] 

Here:

- ``[INPATH]`` is the path to the input folder containing the NTuples.
- ``[CONFIG]`` is the path to the configuration file.
- ``[EVENT INDEX]`` specifies the event index or slice to inspect. Use ``-1`` to inspect all events.