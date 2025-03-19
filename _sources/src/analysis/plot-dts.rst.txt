plot-dt / plot-dts
==================

These tools allow for easy visualization of DT chamber plots with detailed information at the drift
cell level using the Patches module from ``mpldts.patches.dt_patch``. Let's illustrate this with a
specific example.

Suppose you have access to DIGI information from your input data. By specifying its reconstruction
in the ``run_config.yaml``, you can access this information through statements like 
``event[i].digis[j].property``, where ``property`` could be ``time``, ``BX``, etc. To visualize the 
digis patterns in a DT chamber, you can configure the following in your ``run_config.yaml``:

.. code-block:: yaml

    # ------------------------------- Configuration for DT plots -------------------------------------#
    dt_plots_configs:
        .
        .
        .
        dt-cell-info:
        particle_type: 'digis'      # Particle type to use
        cmap_var: 'time'            # Variable to use for the colormap

        cmap-configs:
        cmap: 'viridis'             # Colormap to use
        cmap_under: 'none'          # Color for values under vmin
        norm:
            class: 'matplotlib.colors.Normalize'  # Normalization class
            vmin: 299                             # Minimum value for normalization
            vmax: 1000                            # Maximum value for normalization
        .
        .
        .

Then, run the following command:

.. code-block:: bash

    dtpr plot-dt -i [INPATH] -o [OUTPATH] -evn 0 --wheel -2 --sector 1 --station 1

This will produce a plot similar to the one below:

.. image:: ../../_static/dt_plot_thr6_ev0.svg
    :width: 600px
    :align: center

Alternatively, you can run:

.. code-block:: bash

    dtpr plot-dts -i [INPATH] -o [OUTPATH] -evn 0

This will generate a plot like the one below:

.. image:: ../../_static/dt_plot_thr6_ev0_global.svg
    :width: 800px
    :align: center


You can use any "particle" (``DIGI``, ``SimHit``, etc.) which can be represented in a DT cell, and 
also use any of its numerical variables, and customize the color map options. Ensure that information
about ``sl``, ``l``, and ``w`` is present (``sl``: DT SuperLayer, ``l``: DT Layer, ``w``: DT cell or wire).