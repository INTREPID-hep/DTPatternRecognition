# Developer Guide

## Overview

YDANA is a YAML-driven framework for columnar analysis workflows with `dask-awkward`.
This guide covers local setup, test execution, formatting, and docs workflow.

## Local setup

### Option 1: uv

```shell
git clone https://github.com/INTREPID-hep/ydana.git
cd ydana
uv venv
source .venv/bin/activate
uv sync --extra dev
```

### Option 2: venv + pip

```shell
git clone https://github.com/INTREPID-hep/ydana.git
cd ydana
python -m venv .venv
source .venv/bin/activate
pip install -e .
pip install .[dev]
```

## Common commands

```shell
# tests
uv run pytest

# lint + import/order checks
uv run ruff check ydana

# auto-fix lint/import issues when possible
uv run ruff check ydana --fix

# format check
uv run ruff format --check ydana

# apply formatting
uv run ruff format ydana

# CLI smoke
uv run ydana --help
```

## Documentation workflow

```shell
cd docs
make html
```

Generated HTML is available under `docs/_build/html`.

## Recommended practices

- Keep PRs small and focused.
- Add tests for behavioral changes.
- Keep docstrings and CLI help aligned with actual behavior.
- Prefer declarative configuration changes over hardcoded values where possible.