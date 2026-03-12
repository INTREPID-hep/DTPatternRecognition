# Contributing to YDANA

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

## Pull request guidelines

- Keep PRs focused and small when possible.
- Add or update tests for behavior changes.
- Update docs for user-facing changes.
- Use descriptive commit messages.
- Avoid committing generated artifacts, large binaries, or local environment files.

## Local workflow

See [DEVELOPERS.md](DEVELOPERS.md) to set up your environment first (create venv, run `uv sync --extra dev`).

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

Before opening a PR with documentation changes, build docs locally:

```shell
cd ../docs
python -m sphinx -W -b html . _build/html
```

If you prefer Make targets:

```shell
cd ../docs
make html
```

## Helpful links

- Repository: https://github.com/INTREPID-hep/ydana
- Issues: https://github.com/INTREPID-hep/ydana/issues
- New issue: https://github.com/INTREPID-hep/ydana/issues/new
- Developer setup: [DEVELOPERS.md](DEVELOPERS.md)
