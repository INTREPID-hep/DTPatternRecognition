Release Notes
=============

Every release is published three times from the same text: as an annotated git
tag, as a GitHub
`Release <https://github.com/INTREPID-hep/DTPatternRecognition/releases>`_ and
as a section of the `CHANGELOG.md` file at the root of the repository.

The manual of each release is kept next to the manual of the current ``main``
branch and can be selected from the version menu at the top of the page. The
manual you are reading now describes the version selected there.

.. list-table::
   :header-rows: 1
   :widths: 16 12 72

   * - Release
     - Date
     - Highlights
   * - `v3.6.0-beta <https://github.com/INTREPID-hep/DTPatternRecognition/releases/tag/v3.6.0-beta>`_
     - 2026-11-21
     - Particle building through one reader per attribute, ``entry_range``
       access, chunked parallel runs whose parts are merged by ROOT, and a
       progress bar that refreshes ten times per run.
   * - `v3.5.0 <https://github.com/INTREPID-hep/DTPatternRecognition/releases/tag/v3.5.0>`_
     - 2026-05-11
     - ROOT ``Draw(...)`` expressions in histogram definitions, multiprocessing
       removed from ``fill-histos``, fixes to the events dumper.
   * - `v3.4.0 <https://github.com/INTREPID-hep/DTPatternRecognition/releases/tag/v3.4.0>`_
     - 2026-04-20
     - ``dtpr dump-events`` and ``dtpr.utils.dumper``, lists and glob patterns as
       input, faster ``EventList`` entry access, ``--cores`` deprecated.
   * - `v3.3.0 <https://github.com/INTREPID-hep/DTPatternRecognition/releases/tag/v3.3.0>`_
     - 2026-02-16
     - ``!include`` directive for YAML configuration files, visualizer canvas fix.
   * - `v3.2.0 <https://github.com/INTREPID-hep/DTPatternRecognition/releases/tag/v3.2.0>`_
     - 2026-02-11
     - Several input NTuples, configuration split across files, keyword arguments
       forwarded to ``src``, move to the INTREPID-hep organisation.
   * - `v3.0.0 <https://github.com/INTREPID-hep/DTPatternRecognition/releases/tag/v3.0.0>`_
     - 2025-09-11
     - Package restructure, PEP 621 packaging, tests and continuous integration,
       manual on GitHub Pages, YAML artist configuration, parallel
       ``fill-histos``, renamed ``inspect-events`` and ``events-visualizer``.
   * - `v2.1.0 <https://github.com/INTREPID-hep/DTPatternRecognition/releases/tag/v2.1.0>`_
     - 2025-06-11
     - ``method3`` classifier and clean-up.
   * - `v2.0.0 <https://github.com/INTREPID-hep/DTPatternRecognition/releases/tag/v2.0.0>`_
     - 2025-06-05
     - New YAML particle format, custom ``Particle`` subclasses removed, recursive
       NTuple discovery.
   * - `v1.1.0 <https://github.com/INTREPID-hep/DTPatternRecognition/releases/tag/v1.1.0>`_
     - 2025-06-03
     - Faster ``EventList.get_by_number``, particles no longer share mutable
       attributes, mplDTs v2.0.0.
   * - `v1.0.0 <https://github.com/INTREPID-hep/DTPatternRecognition/releases/tag/v1.0.0>`_
     - 2025-05-16
     - First packaged release: the ``dtpr`` command line and the event, particle,
       histogram and plotting layers.

No `v3.1.0` was ever tagged. `v3.3.0` and `v3.4.0` were first tagged
`v3.3.0-beta` and `v3.4.0-beta`, and the version recorded inside those trees
still carries the `-beta` suffix.
