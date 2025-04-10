# Examples

* `ntuples` contains some DT NTuples for quick tests. `DTDPGNtuple_..._99.root` is an NTuple produced with a sample of $Z'\rightarrow \mu\mu$ (where $m_{Z'} = 6 \text{ TeV}$), 
and `DTDPGNtuple_..._101.root` is an NTuple produced with a Minimum Bias sample. `g4...refactored.root` came from simulation of DT showers using Geant4.
* `AM_fill-histos_test` contains the results of generating AM efficiency and rate plots by filling histograms using the `dtpr fill-histos ....` command.
* The other two `yaml` configuration files, `...4isoplots.yaml` and `...4visualizer.yaml`, are useful for directly running `dtpr plot-dt(s) ...` or `dtpr event-visualizer ...`, respectively.
