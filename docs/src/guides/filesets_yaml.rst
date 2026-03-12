filesets.yaml Guide
===================

Define datasets as named groups of files so the same analysis can run over
multiple samples consistently.

.. code-block:: yaml

   filesets:
     DY:
       - /path/to/dy_1.root
       - /path/to/dy_2.root
     Zprime:
       - /path/to/zprime_1.root
