# Pattern recognition for DTs
This repo contains a set of tools that can be used to implement pattern recognition algorithms
considering the geometrical features of the CMS DT system.

## How to run 
```
python3 concentrator.py --inpath /lustrefs/hdd_pool_dir/L1T/Filter/ZprimeToMuMu_M-6000/0000/ --maxfiles 20
```

This will command will run over 20 files in the `inpath` folder, and run the analyses defined under `concentrator.py`.

## How it works
The `utils/ntuple_reader.py` code will read DTNtuples and fetch all the different objects that can be fetched from there:
 * genmuons
 * segments
 * trigger primitives
 * shower objects

Then it will match genmuons to segments, and TPs to segments (following the Analytical Method procedure: https://github.com/jaimeleonh/DTNtuples/blob/unifiedPerf/test/DTNtupleTPGSimAnalyzer_Efficiency.C). All these objects are stored in the reader to use for creating histograms.

# How to define histograms
Define your histos in `utils/baseHistos.py` following this format:

## For efficiencies
```
histos.update({
    "HISTONAME" :  {  
      "type" : "eff",
      "histoDen" : r.TH1D("Denominator name", r';TitleX; Events', nBins, firstBin , lastBin),
      "histoNum" : r.TH1D("Numerator name", r';TitleX; Events', nBins, firstBin , lastBin),
      "func"     : lambda reader: __function_for_filing_histograms__,
      "numdef"   : lambda reader: __condition_for_numerator__ 
},
```

Where:
 * `HISTONAME`: is the name that will appear in the output ntuple.
 * `func`: this is the function that gives the value for filling the histogram. It always takes the reader as an input, and from there
 one can fetch all the different objects that the reader reconstructs. Can be a list. Some examples:
    * `lambda reader: reader.genmuons[0].pt ` this will fill a histogram with the leading muon pT.
    * `lambda reader: [seg.wh for seg in fcns.get_best_matches( reader, station = 1 )]`. This will get the best matching segments in MB1 (`station = 1`), and fill with the value of the wheel of the matching segment. The function `get_best_matches` is defined under `utils/functions.py`. 
 * `Numdef`: defines a boolean to differentiate denominator and numerator. The numerator will only be filled when the return of this function is `True`. If the `func` option returns a list, this must return a list of booleans with the same length. 

## For flat distributions
```
  "HISTONAME" : {
    "type" : "distribution",
    "histo" : r.TH1D("HISTONAME", r';TitleX; Events', nBins, firstBin , lastBin),
    "func" : lambda reader: __function_for_filing_histograms__,
  },
```