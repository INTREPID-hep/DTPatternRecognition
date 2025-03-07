# Pattern Recognition for DTs

This package provides a set of base tools for implementing pattern recognition algorithms for the CMS DT system
using input data in NTuples format. It also includes visualization tools for DT Ntuples taking into account
the geometrical features of the CMS DT system by using the [mplDTs package](https://github.com/DanielEstrada971102/mplDTs) and custom implementations in the `dtpr` folder.

Comprehensive documentation is available at the [GitHub Pages site](https://danielestrada971102.github.io/DTPatternRecognition/).

The package is just in its early stages, so feel free to contribute, report bugs, or suggest improvements.
Take a look to the [Contributors](CONTRIBUTING.md) and [Developers](DEVELOPERS.md) guides.

## Installation

Download the source files or clone the repository:

```shell
git clone https://github.com/DanielEstrada971102/DTPatternRecognition.git
cd DTPatternRecognition
```

You can then install the package with pip by running:

```shell
pip install .
```

To check if the package was installed successfully, run:

```shell  
pip show DTPatternRecognition
```

> [!IMPORTANT]
> Be aware that the package requires PyROOT, so you should have it installed. If you are working in a Python virtual environment, ensure to include ROOT in it. To do this, use the command:
>
>```shell  
>python -m venv --system-site-packages ROOT ENV_DIR[ENV_DIR ...]
>```
