# Changelog

All notable changes to DTPatternRecognition are documented in this file.

Each release is published three times from the same text: the section of this
file for that version is the message of the annotated git tag, the body of the
GitHub Release, and the source of the `docs/src/release_notes.rst` table.
Versions follow [Semantic Versioning](https://semver.org/), `-beta` marks a
pre-release, and dates are the tag dates.

## [Unreleased]

### Documentation

- The documentation site is built for every tag, and a version selector at the
  top of the page switches between the manual of each release and the manual of
  `main`. The site root redirects to `latest`.
- New `docs/requirements.txt` with the actual Sphinx dependencies, and the
  `release` shown by the manual is taken from `pyproject.toml` (or from
  `SPHINX_RELEASE`) instead of being typed by hand.

## [v3.6.0-beta] - 2026-11-21

### Added

- `NTuple(entry_range=(begin, end))` and `EventList(begin=..., end=...)` to
  process a contiguous window of the input.
- `--cores`, `--no-merge`, `--job-id` and `--job-count` in `fill-histos` and
  `dump-events`: the entries are split into chunks, every chunk writes its own
  ROOT file and ROOT merges the parts. `--job-id`/`--job-count` run one chunk
  externally (batch system, separate machine).
- Performance section in the manual, and an updated command-line reference.

### Changed

- Event building is driven by per-attribute readers: each branch is converted
  once per event (`EntryView`) and every `Particle` attribute is materialised
  from its own reader instead of a per-particle dictionary.
- Expressions are compiled once per attribute rather than once per particle,
  literal attributes are copied instead of deep-copied, `[flat, counts]`
  branch pairs are resolved from cumulative counts, and ROOT prefetches baskets
  through a `TTreeCache`.
- The event progress bar refreshes about ten times over a run instead of once
  per event; a block that takes longer than five seconds is split so the bar
  keeps moving.

### Fixed

- `!include` accepts a list of files, resolves paths relative to the including
  file and detects circular includes.
- `Particle` equality and hashing are symmetric and ignore private state.
- `dtpr.utils.functions` no longer imports matplotlib to type-hint a function.

## [v3.5.0] - 2026-05-11

### Added

- Histogram definitions accept ROOT `Draw(...)` syntax, so a histogram can be
  produced by an expression instead of a branch name.

### Changed

- Multiprocessing was removed from histogram filling; `fill-histos` runs
  serially until the chunked writer replaced it.

### Fixed

- Fixes to the events dumper, and the event builder resolves cross-referenced
  particles.

## [v3.4.0] - 2026-04-20

### Added

- `dtpr dump-events` and `dtpr.utils.dumper` to write processed events back to a
  ROOT file.
- `NTuple` accepts a list of files or a glob pattern.
- Segments can be plotted off the chosen base variables.

### Changed

- `--cores` is deprecated in `fill-histos` and ignored.
- `EventList` entry access no longer iterates over every entry.
- `get_callable_from_src` caches by source string.

### Fixed

- Errors and warnings raised by the analysis are no longer swallowed.

> Tagged `v3.4.0-beta` and renamed to `v3.4.0`; the version recorded inside
> the tree is still `3.4.0-beta`.

## [v3.3.0] - 2026-02-16

### Added

- `!include` directive for YAML configuration files, so a configuration can be
  split across several files.

### Fixed

- Canvas sizing in the events visualizer.

> Tagged `v3.3.0-beta` and renamed to `v3.3.0`; the version recorded inside
> the tree is still `3.3.0-beta`.

## [v3.2.0] - 2026-02-11

### Added

- Several input NTuples read as one chain.
- Configuration split across YAML files.
- Extra keyword arguments are forwarded to `src` when a `Particle` is built.
- The base variables plotted for segments are configurable.

### Changed

- The repository moved to the INTREPID-hep organisation, and absolute imports
  became relative imports.

No `v3.1.0` was ever tagged.

## [v3.0.0] - 2025-09-11

### Added

- `fill-histos --cores` to fill histograms in parallel.
- Tests, `black` and continuous integration.
- The manual is built and deployed to GitHub Pages.
- DT plots are described by a YAML artist configuration, the visualizer uses it,
  and a progress-bar manager was added.

### Changed

- The package was restructured into `base`, `analysis`, `utils` and `gui`, and
  `pyproject.toml` moved to PEP 621.

### Breaking

- `inspect-event` became `inspect-events` and `event-visualizer` became
  `events-visualizer`.

## [v2.1.0] - 2025-06-11

### Added

- `method3` for the showered generator-muon classification.

### Changed

- Clean-up and fixes, about 460 lines removed.

> `pyproject.toml` still declared `2.0.0`; install by tag.

## [v2.0.0] - 2025-06-05

### Breaking

- The YAML particle definition format changed.
- The custom `Particle` subclasses were removed; their functionality is
  replaced by free functions.

### Added

- `NTuple` loads `.root` files recursively from nested directories.
- A default analysis for the event inspector.

## [v1.1.0] - 2025-06-03

### Changed

- `EventList.get_by_number` reads the branch directly instead of building the
  event.
- `Particle` no longer shares mutable attributes between instances (deep copy).

### Fixed

- The event inspector.

### Dependencies

- mplDTs v2.0.0.

## [v1.0.0] - 2025-05-16

First packaged release. `dtpr` exposes `fill-histos`, `plot-dts`, `plot-dt`,
`inspect-event`, `event-visualizer` and the `create-*` generators; the event and
particle mapping is driven by `run_config.yaml`, and DT plots and the visualizer
are included.

> `pyproject.toml` declared `0.1.0`; install by tag.

[v3.6.0-beta]: https://github.com/INTREPID-hep/DTPatternRecognition/tree/v3.6.0-beta
[v3.5.0]: https://github.com/INTREPID-hep/DTPatternRecognition/tree/v3.5.0
[v3.4.0]: https://github.com/INTREPID-hep/DTPatternRecognition/tree/v3.4.0
[v3.3.0]: https://github.com/INTREPID-hep/DTPatternRecognition/tree/v3.3.0
[v3.2.0]: https://github.com/INTREPID-hep/DTPatternRecognition/tree/v3.2.0
[v3.0.0]: https://github.com/INTREPID-hep/DTPatternRecognition/tree/v3.0.0
[v2.1.0]: https://github.com/INTREPID-hep/DTPatternRecognition/tree/v2.1.0
[v2.0.0]: https://github.com/INTREPID-hep/DTPatternRecognition/tree/v2.0.0
[v1.1.0]: https://github.com/INTREPID-hep/DTPatternRecognition/tree/v1.1.0
[v1.0.0]: https://github.com/INTREPID-hep/DTPatternRecognition/tree/v1.0.0
[unreleased]: https://github.com/INTREPID-hep/DTPatternRecognition/compare/v3.6.0-beta...main
