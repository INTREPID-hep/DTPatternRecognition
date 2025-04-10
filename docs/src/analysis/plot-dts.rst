plot-dt / plot-dts
==================

These tools allow for easy visualization of DT chamber plots with detailed information at the drift
cell level using the Patches module from ``mpldts.patches.dt_patch``. Let's illustrate this with a
specific example.

Suppose you have access to ``digi`` information from your input data. So, by specifying its reconstruction
in the configuration file, you will be able to access this information through statements like 
``event[i].digis[j].property``, where ``property`` could be ``time``, ``BX``, etc. To visualize the 
digi patterns in a DT chamber, you can configure the following in your configuration file:

.. code-block:: yaml

    dt_plots_configs:
        mplhep-style: 'CMS'         # Style to use for the plots

        figure-configs:
            # Any valid matplotlib figure rc parameters can be used here

        dt_plots_configs:
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

            # DT bounds and cells are matplotlib.patches.Rectangle objects, so other rc parameters can be used
            bounds-kwargs:
                facecolor: ["lightgray", "lightyellow", "lightpink", "lightblue"]   # Color of the rectangle [MB, SL1, SL3, SL2]
                edgecolor: "k"                                                      # Color of the edge
                alpha: 0.3                                                          # Transparency of the rectangle
                linewidth: 0.5                                                      # Width of the edge
                # ...

            cells-kwargs:
                edgecolor: "k"              # Color of the edge
                linewidth: 0.025              # Width of the edge
                #...

And then, by running the command:

.. code-block:: bash

    dtpr plot-dt -i [INPATH] -o [OUTPATH] -cf [CONFIG_FILE] -evn 0 --wheel 2 --sector 1 --station 1

You will produce a plot similar to the one below:

.. image:: ../../_static/dt_plot_ev0.svg
    :width: 1000px
    :align: center

Alternatively, you can run:

.. code-block:: bash

    dtpr plot-dts -i [INPATH] -o [OUTPATH] -cf [CONFIG_FILE] -evn 0

And you will generate a plot like the one below:

.. image:: ../../_static/dt_plots_ev0.svg  
    :width: 800px
    :align: center


You can use any "particle" (``DIGI``, ``SimHit``, etc.) which can be represented in a DT cell, and 
also use any of its numerical variables, and customize the color map options. Ensure that information
about ``sl``, ``l``, and ``w`` is present (``sl``: DT SuperLayer, ``l``: DT Layer, ``w``: DT cell or wire).

You can always use the methods directly from the modules ``dtpr.analysis.plot_dt_chamber`` and 
``dtpr.analysis.plot_dt_chambers``.

plot_dt_chamber
^^^^^^^^^^^^^^^
.. automodule:: dtpr.analysis.plot_dt_chamber
    :members: make_dt_plot, embed_dt2axes

plot_dt_chambers
^^^^^^^^^^^^^^^^
.. automodule:: dtpr.analysis.plot_dt_chambers
    :members: make_dt_plots, embed_dtwheel2axes