# Pattern Recognition for DTs
This package is intended to serve as a base framework for Trigger pattern recognition processes in the CMS Drift Tubes system,
using as input data .root NTuples in a format similar to [this one](test-ntuple). It is basically a flat tree structure, where
each branch represents a property of a CMS event or particle(s).

Since the framework was developed in the context of the Drift Tube detectors, it also includes visualization tools considering
the geometrical features of the CMS DT system by using the [mplDTs package](https://danielestrada971102.github.io/mplDTs) and custom implementations in the [`dtpr/utils/dt_plot_functions.py`](plot-functions-folder) module.

Comprehensive documentation is available in read-the-docs format at the [GitHub Pages site](https://danielestrada971102.github.io/DTPatternRecognition/).

The package is under development, so feel free to contribute, report bugs, or suggest improvements.
Take a look at the [Contributors](CONTRIBUTING.md) and [Developers](DEVELOPERS.md) guides.

## Installation

You can install the latest version of the package directly from GitHub using pip:

```shell
pip install "git+https://github.com/DanielEstrada971102/DTPatternRecognition.git@v2.0.0"
```

Or, if you prefer to use Poetry for dependency and environment management:

```shell
poetry add "git+https://github.com/DanielEstrada971102/DTPatternRecognition.git@v2.0.0"
```

If you want to work with the source and editable installs, you can initialize and build the environment with Poetry:

```shell
poetry install
poetry shell
```

To check if the package was installed successfully, run:

```shell
# If you used pip
pip show DTPatternRecognition

# If you used Poetry
poetry show DTPatternRecognition
```

> [!IMPORTANT]
> The package requires PyROOT, so you should have it installed and available in your environment.  
> If you are working in a Python virtual environment, ensure to include ROOT in it.  
> For example, to create a venv that can access system-wide ROOT:
>
> ```shell  
> python -m venv --system-site-packages ENV_DIR
> ```

test-ntuple: tests/ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root  
plot-functions-folder: dtpr/utils/dt_plot_functions.py