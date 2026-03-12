Dask Task Graphs
================

YDANA builds Dask task graphs from YAML-defined workflows.

.. mermaid::

   flowchart TD
      A[run config YAML] --> B[Config]
      B --> C[NTuple lazy arrays]
      C --> D[Pipeline operations]
      D --> E[Histogram filling graph]
      E --> F[Compute and write outputs]

See :class:`ydana.base.histos.Histogram` for histogram behavior and
:mod:`ydana.base.pipeline` for operation ordering.
