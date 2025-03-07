event-visualizer
================

For debugging or performing multiple visual inspections across several events or DT chambers, 
running the previous command each time can be cumbersome. To streamline this process, a simple 
Graphical User Interface (GUI) was implemented. This GUI dynamically loads event information and 
produces the required plots, as demonstrated in the following short clip:

.. raw:: html

    <p align="center">
        <img src="../../_static/gui_visualizer.gif" width="600">
    </p>

To open the GUI, run the following command:

.. code-block:: bash

    dtpr event-visualizer -i [INPATH] -o [OUTPATH]
    # The OUTPATH argument is only required if a custom run_config.yaml will be used.