# Contributing to YDANA-HEP

Thanks for contributing.

## How to contribute

1. Open an issue for bugs, feature requests, or questions.
2. Create a branch for your work.
3. Implement and test your changes locally.
4. Submit a pull request to `main`.

## Issue guidelines

- Search existing issues before opening a new one.
- Provide a minimal reproducible example when reporting bugs.
- Include environment details (Python version, install method, relevant dependencies).
- Use clear titles and expected vs actual behavior.
- Apply the most accurate label from the approved label catalog. https://github.com/INTREPID-hep/ydana-hep/labels/analysis, https://github.com/INTREPID-hep/ydana-hep/labels/bug, https://github.com/INTREPID-hep/ydana-hep/labels/data, https://github.com/INTREPID-hep/ydana-hep/labels/enhancement, https://github.com/INTREPID-hep/ydana-hep/labels/event, https://github.com/INTREPID-hep/ydana-hep/labels/feature, https://github.com/INTREPID-hep/ydana-hep/labels/information, https://github.com/INTREPID-hep/ydana-hep/labels/refactor, https://github.com/INTREPID-hep/ydana-hep/labels/result


## Pull request guidelines

- Keep PRs focused and small when possible.
- Add or update tests for behavior changes.
- Update docs for user-facing changes.
- Prefer clean, declarative commit messages with clear intent.
- Avoid committing generated artifacts, large binaries, or local environment files.

## Local workflow

Set up your environment first (create venv, run `uv sync --extra dev`).

```shell
git checkout -b feature/my-change
uv run pytest
uv run ruff check ydana
uv run ruff check ydana --fix
uv run ruff format --check ydana
git add <files>
git commit -m "Describe change"
git push origin feature/my-change
```

Before opening a PR with documentation changes, build docs locally to check for warnings:

```shell
cd ../docs
python -m sphinx -W -b html . _build/html
```

If you prefer Make targets:

```shell
cd ../docs
make livehtml
```

## Helpful links

- Repository: [https://github.com/INTREPID-hep/ydana-hep](https://github.com/INTREPID-hep/ydana-hep)
- Issues: [https://github.com/INTREPID-hep/ydana-hep/issues](https://github.com/INTREPID-hep/ydana-hep/issues)
- New issue: [https://github.com/INTREPID-hep/ydana-hep/issues/new](https://github.com/INTREPID-hep/ydana-hep/issues/new)
