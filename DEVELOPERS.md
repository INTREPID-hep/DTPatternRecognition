# Developers' guide

[[_TOC_]]

## Development setup

The recommended way of development is under a virtual environment. Bear in mind that you will need PyROOT.

### Quick start

1. Clone the repository
    ```shell
    git clone https://github.com/DanielEstrada971102/DTPatternRecognition.git && cd DTPatternRecognition
    ```
2. Install the `DTPatternRecognition` project along with all its dependencies in a virtual environment with the commands
    ```shell
    python -m venv --system-site-package ROOT ENV_DIR[ENV_DIR ...]
    source [ENV_DIR]/bin/activate
    # being in the main directory
    pip install -e .
    ```

## Development guidelines
### Coding style

You can check and fix your code formatting through the usage of `Black`:

``` shell
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
...