# Pattern Recognition for DTs
This repository contains tools for implementing pattern recognition algorithms for the CMS DT system. The common tools that have been implemeted can be found in the `src/analysis` directory and are accessible through the `dtpr-analysis` command, as described in the **How to Run section**. These analyses are based on the tools and classes implemented in the [dtpr-package](https://github.com/DanielEstrada971102/dtpr-package.git) (included here as a submodule), and the `src` folder.

## Installation

First, download the source files or clone the repository. 

```shell
git clone https://github.com/DanielEstrada971102/DTPatternRecognition.git --recursive -b destrada-dtpr
```

Next, install the dependencies with pip. It is recommended to use a Python virtual environment

```shell
python3 -m venv --system-site-packages ROOT [NAME-OF-VENV]
source [NAME-OF-VENV]/bin/activate
pip install .
```

> [!TIP]
> If you encounter issues related to the `dtpr` package, try reinstalling it by running:
> ```shell
> pip uninstall dtpr
> cd dtpr-package
> pip install .
> ```

## How to Run

To run an analysis, use the following command:

```
dtpr-analysis [ANALYSIS-TO-DO] [--OPTIONAL-ARGS]
```

The `dtpr-analysis` command is a CLI tool that runs `src/cli.py`. Possible analyses are implemented there, and future analyses could be added there for easy execution.

<!-- The `NTUPLE-TYPE` argument specifies the type of data format to read. Possible data formats should be included in `src/ntuples`. Currently, only `dtntuple` is available. An example of this type of file is `/src/ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_Simulation_99.root`. -->

The `ANALYSIS-TO-DO` argument indicates which of the available analyses to execute. Currently, some of options are the followings: `fill-histos`, `plot-dts`, `plot-dt`, `inspect-event`, and `event-visualizer`. See the Analyses section for further details.

The other optional arguments can be checked by running with `-h/--help`. Some important ones are: `-i/--inpath`, `-o/--outpath`, `--maxfiles`, and `--maxevents`.

## How It Works

The main concept of this framework is to facilitate analysis on input data formats using an event-by-event approach with Python classes. This allows for more intuitive and readable code, such as:

```python
for muon in events.genmuons:
  if muon.pt > 5:
    print(muon.eta)
```

compared to:

```python 
for i in range(len(tree.gen_pdgID)):
  if tree.gen_pt[i] > 5:
    print(tree.gen_eta[i])
```
Reading the input files and mapping the information into the respective classes are handled by a central class inherating from `dtpr.base.NTuple`. For instance, `src/ntuples/dtntuple.py` defines a child class called `DtNtuple`. Multiple implementations of `NTuple` children are possible to accommodate the needs of different input data formats and filtering requirements. These implementations should override the `event_preprocessor` method to control the filtering and creation of events and their associated "particle" objects based on the input file information. For detailed information on this building process, refer to the [dtpr-package documentation](https://danielestrada971102.github.io/dtpr-package/). Once the `Ntuple` child class is instantiated, you can iterate, index, and slice through events using the `.events` attribute.

Objects obtained from data formats will be accessible directly as event attributes, such as Genmuons (`event[i].genmuons`), Segments (`event[i].segments`), Trigger primitives (`event[i].tps`), and Shower objects (`event[i].fwshowers`). The construction of specific objects can be controlled by specifying them in a YAML event config file. Again, see the [dtpr-package documentation](https://danielestrada971102.github.io/dtpr-package/) for more details.

Additionally, this framework includes generic tools for common tasks such as filling ROOT histograms and visualizing input data related to CMS DT detectors. For example, you can draw DT hits in a chamber with geometrically accurate plots. Refer to the **Analyses** section for detailed information. 

To make these implementations as generic as possible, important parameters such as the type of NTuple to process data, the type of particles to build, the histograms to fill, and the matplotlib style parameters to use should be specified via a YAML run config file. This file should be placed in the `--outpath` directory with the name `run_config.yaml`. If not provided, the default configuration in `src/utils/yamls/run_config.yaml` will be used.

Particulary, for DTNtuples (`src/ntuples/dtntuple.py:DtNtuple`) analyses, filter, Segments-TPs matching (following the [Analytical Method procedure](https://github.com/jaimeleonh/DTNtuples/blob/unifiedPerf/test/DTNtupleTPGSimAnalyzer_Efficiency.C)), and [**emulation of muon shower build processes**](src/utils/shower_functions.py) are performed.

## Analyses

### `fill-histos`

This tool allows to recursively fill predefined histograms event by event. Histograms should be contained in a python dictionary called `histo`  follow the following formats:

#### For Efficiencies
```python
histos.update({
  "HISTONAME": {
    "type": "eff",
    "histoDen": r.TH1D("Denominator name", r';TitleX; Events', nBins, firstBin, lastBin),
    "histoNum": r.TH1D("Numerator name", r';TitleX; Events', nBins, firstBin, lastBin),
    "func": __function_for_filling_histograms__,
    "numdef": __condition_for_numerator__
  },
})
```

Where:
* `HISTONAME`: The name that will appear in the output ntuple.
* `func`: A function that provides the value for filling the histogram. It takes the `reader` as input and can fetch all the objects reconstructed by the `reader`. Examples:
  * `lambda reader: reader.genmuons[0].pt` fills a histogram with the leading muon pT.
  * `lambda reader: [seg.wh for seg in fcns.get_best_matches(reader, station=1)]` gets the best matching segments in MB1 (`station=1`) and fills with the wheel value of the matching segment. The `get_best_matches` function is defined in `utils/functions.py`.
* `numdef`: A boolean function to differentiate between the denominator and numerator. The numerator is filled only when this function returns `True`. If `func` returns a list, this must return a list of booleans of the same length.

#### For Flat Distributions

```python
  "HISTONAME": {
    "type": "distribution",
    "histo": r.TH1D("HISTONAME", r';TitleX; Events', nBins, firstBin, lastBin),
    "func": lambda reader: __function_for_filling_histograms__,
  },
```
The `reader` argument in functions represents an `event` entry, which should be an instance of `dtpr.base.Event` created by the implemented `NTuple` child. A set of predefined histograms is available in `src/utils/histograms`.

Once your histograms are defined, include them in the `run_config.yaml` file by specifying:

```yaml
.
.
.
histo_sources:
  # Define the source modules of the histograms
  - src.utils.histograms.baseHistos
  # Add additional source modules as needed
  .
  .
  .

histo_names:
  # List the histograms to fill - Uncomment or add histograms as needed
  # They should exist in any of the source modules
  # ============ efficiencies ============ #
  - seg_eff_MB1
  - seg_eff_MB2
  .
  .
  .
```

Then, run the following command to fill the histograms:

```shell
dtpr-analysis fill-histos -i [INPATH] -o [OUTPATH] ...
```
### `inspect-event`

...

### `plot-dt` and `plot-dts`

These tools allow for easy visualization of DT chamber plots with detailed information at the drift cell level using the Patches module from `dtpr.patches.dt_patch`. Let's illustrate this with a specific example.

Suppose you have access to DIGI information from your input data. By specifying its reconstruction in the `run_config.yaml`, you can access this information through statements like `event[i].digis[j].property`, where `property` could be `time`, `BX`, etc. To visualize the digis patterns in a DT chamber, you can configure the following in your `run_config.yaml`:

```yaml
# ------------------------------- Configuration for DT plots -------------------------------------#
dt_plots_configs:
  .
  .
  .
  dt-cell-info:
    particle_type: 'digis'      # Particle type to use
    cmap_var: 'time'            # Variable to use for the colormap

  cmap-configs:
    cmap: 'viridis'             # Colormap to use
    cmap_under: 'none'          # Color for values under vmin
    norm:
      class: 'matplotlib.colors.Normalize'  # Normalization class
      vmin: 299                             # Minimum value for normalization
      vmax: 1000                            # Maximum value for normalization
  .
  .
  .
```

Then, run the following command:

```bash
dtpr-analysis plot-dt -i [INPATH] -o [OUTPATH] -evn 0 --wheel -2 --sector 1 --station 1
```

This will produce a plot similar to the one below:

<p align="center">
  <img src="_statics/dt_plot_thr6_ev0.svg" width=600x>
</p>

Alternatively, you can run:

```bash
dtpr-analysis plot-dts -i [INPATH] -o [OUTPATH] -evn 0
```

This will generate a plot like the one below:

<p align="center">
  <img src="_statics/dt_plot_thr6_ev0_global.svg" width=800x>
</p>

You can use any "particle" (`DIGI`, `SimHit`, etc.) which can be represented in a DT cell, and also use any of its numerical variables, and customize the color map options. Ensure that information about `sl`, `l`, and `w` is present (`sl`: DT SuperLayer, `l`: DT Layer, `w`: DT cell or wire).

### `event-visualizer`

For debugging or performing multiple visual inspections across several events or DT chambers, running the previous command each time can be cumbersome. To streamline this process, a simple Graphical User Interface (GUI) was implemented. This GUI dynamically loads event information and produces the required plots, as demonstrated in the following short clip:

<p align="center">
  <img src="_statics/gui_visualizer.gif" width=600x>
</p>

To open the GUI, run the following command:

```bash
dtpr-analysis plot-dts -i [INPATH] -o [OUTPATH]
# The OUTPATH argument is only required if a custom run_config.yaml will be used.
```