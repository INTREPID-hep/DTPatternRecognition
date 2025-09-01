# Developers' guide

[[_TOC_]]

## Development setup

The recommended way of development is under a virtual environment. Bear in mind that you will need PyROOT.

### Quick start

**Option 1: Using venv and pip**

1. Clone the repository:
    ```shell
    git clone https://github.com/DanielEstrada971102/DTPatternRecognition.git && cd DTPatternRecognition
    ```
2. Create and activate a virtual environment (with access to system-wide ROOT):
    ```shell
    python -m venv --system-site-packages ENV_DIR
    source ENV_DIR/bin/activate
    ```
3. Install the package in editable mode (this will install main dependencies only):
    ```shell
    pip install -e .
    ```
    > The auxiliary package `mplDTs` is listed as a dependency in `pyproject.toml` and will be installed from GitHub automatically.
    > If you need to work with it in editable mode as well, you can clone it from [here](https://github.com/DanielEstrada971102/mplDTs) and install it in editable mode similarly.

4. If you also want to install development dependencies (for testing, linting, etc.), run:
    ```shell
    pip install .[dev]
    ```

**Option 2: Using Poetry**

1. Clone the repository:
    ```shell
    git clone https://github.com/DanielEstrada971102/DTPatternRecognition.git && cd DTPatternRecognition
    ```
2. Install dependencies and activate the environment:
    ```shell
    poetry install
    $(poetry env active)
    ```
    > If you don't need to activate the Poetry environment, you can prefix commands with `poetry run`, e.g., `poetry run pytest`.
    > The auxiliary package `mplDTs` is listed as a dependency in `pyproject.toml` and will be installed from GitHub automatically.
    > If you need to work with it in editable mode as well, you can clone it from [here](https://github.com/DanielEstrada971102/mplDTs) and ensure to replace in the `pyproject.toml` file the dependency line with `mplDTs = {path = "PATH_TO_MPLDTS_FOLDER", develop = true}`.
   > If you need PyROOT, make sure it is available in your environment. You may need to install ROOT system-wide or ensure your Poetry environment can access it. For this, the easiest way is to enable system site packages before installing with the command `poetry config virtualenvs.options.system-site-packages true`.

## Development guidelines

### Coding style

You can check and fix your code formatting through the usage of `Black`:

```shell
# If using Poetry
poetry run black --check -l 100 dtpr

# Or, if using pip/venv
black --check -l 100 dtpr
```

### Documentation Writing

We use [Sphinx](https://www.sphinx-doc.org/en/master/usage/quickstart.html) with the Read the Docs theme for Python code documentation.

To update the documentation, edit the files in the `docs/src` directory. To preview your changes, build the source files by running:

```shell
cd docs
make html
```

The generated files will be placed in the `docs/_build` directory. You can view the main `index.html` file at `docs/_build/html/index.html`.

Once your changes are ready, commit and push them. The updates will be automatically deployed to the online documentation via GitHub Actions when the main branch is updated.

## Tips

- For a smoother development experience, consider using an IDE or code editor with Python and Git integration, such as [Visual Studio Code](https://code.visualstudio.com/) or [PyCharm](https://www.jetbrains.com/pycharm/).
- Regularly pull the latest changes from the main branch to keep your local repository up to date.
- Use descriptive commit messages and commit often to avoid losing your work.