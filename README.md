# YDANA (**Y**AML-Driven **D**ask-**A**wkward **N**tuple **A**nalyzer)

YDANA is a YAML configuration-driven framework for High Energy Physics (HEP) columnar data analysis. 

It combines the object-oriented event structures of Coffea's `NanoEventsFactory` with the distributed processing power of `dask-awkward`. The main goal of YDANA is to provide a declarative workflow where datasets, schema mappings, preprocessing steps, and histograms are configured directly from YAML files, reducing the need to write custom execution scripts.

## Motivation

Setting up distributed analysis pipelines can sometimes require a lot of repetitive code for managing data chunks, memory, and output merging. YDANA aims to simplify this process by offering:

- **Simplified configuration:** Define your datasets, cuts, and histograms in readable YAML files.
- **Automatic scaling:** Test your configuration locally on a single file, then run the exact same YAML on a Dask cluster.
- **Lazy execution:** By leveraging `dask-awkward`, the framework only reads the specific branches you use, which helps save memory and processing time.
- **Resume-safe exports:** When saving processed events to ROOT or Parquet, YDANA writes file-by-file (partitioning). If a job is interrupted, you don't lose your progress.

## Core Features

- **YAML-first orchestration:** Manage the analysis behavior from end-to-end using config files.
- **Schema-aware data access:** Flat ROOT branches are automatically mapped into structured Awkward records.
- **Composable pipeline:** Preprocessors and selectors are applied sequentially based on your configuration.
- **Practical CLI:** Common tasks like `fill-histos`, `dump`, and `merge-dumps` are available directly via the `ydana` command.

## Installation

You can install YDANA directly from GitHub. Using `uv` is recommended for speed:

```shell
uv venv
source .venv/bin/activate
uv pip install "git+[https://github.com/INTREPID-hep/ydana.git](https://github.com/INTREPID-hep/ydana.git)@[TAG_VERSION]"
uv pip show ydana
If you prefer plain pip, the same install works in any active virtual environment:

```shell
pip install "git+https://github.com/INTREPID-hep/ydana.git@[TAG_VERSION]"
pip show ydana
```

## Quick check

```shell
ydana --help
```

## Documentation and contribution

- Project docs: https://intrepid-hep.github.io/ydana/
- Contribution guide: [CONTRIBUTING.md](CONTRIBUTING.md)
- Development guide: [DEVELOPERS.md](DEVELOPERS.md)