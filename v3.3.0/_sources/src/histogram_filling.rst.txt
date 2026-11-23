Histogram Filling
=================

The histogram filling tool in DTPatternRecognition automates the process of extracting physics quantities
from your events and populating ROOT histograms for further analysis or plotting. You define *what* to 
fill and *how* to extract the values, and the framework handles the repetitive event loop and output file creation.

You specify which histograms to fill and how to fill them using a combination of your configuration 
file and Python modules that define the histogram logic. This system is highly 
flexible: you can add new histograms, change their binning, or update their filling logic without 
modifying the core analysis code.

.. rubric:: General Format for Histogram Definitions

Histograms should be defined in a Python dictionary called ``histos`` within your source modules. The supported formats are:

**For Flat Distributions (1D, 2D, 3D):**

.. code-block:: python

    "HISTONAME": {
        "type": "distribution",  # also supports "distribution2d", "distribution3d"
        "histo": r.TH1D("HISTONAME", r';TitleX; Events', nBins, firstBin, lastBin),
        "func": __function_for_filling_histograms__,
    }

- ``HISTONAME``: The name that will appear in the output ``.root`` file.
- ``func``: A function that provides the value(s) for filling the histogram. It takes the ``reader`` (an ``Event`` instance) as input.

**For Efficiencies:**

.. code-block:: python

    "HISTONAME": {
        "type": "eff",
        "histoDen": r.TH1D("Denominator name", r';TitleX; Events', nBins, firstBin, lastBin),
        "histoNum": r.TH1D("Numerator name", r';TitleX; Events', nBins, firstBin, lastBin),
        "func": __function_for_filling_histogram__,
        "numdef": __condition_for_numerator__
    }

- ``func``: Returns the value(s) to fill for the denominator.
- ``numdef``: Returns a boolean or list of booleans (same length as ``func`` output) to determine which entries go into the numerator.

.. rubric:: Example

Suppose you want to study:

1. The transverse momentum (:math:`p_T`) of the leading generator muon in each event.

2. The :math:`\Delta R` between the generator muons (see :ref:`preprocessor_selector_examples`).

3. The efficiency of a trigger algorithm (e.g., `AM`) by wheel and station type.

You want to fill these into ROOT histograms and save them for later analysis.

.. rubric:: Step 1: Specify Histogram Sources and Names

First, tell DTPatternRecognition where to find your histogram definitions and which ones to fill. In your ``run_config.yaml``:

.. code-block:: yaml

    histo_sources:
      - dtpr.utils.histograms   # Contains muon-related histograms
      - my.path.to.am_histos    # Contains AM efficiency/rate histograms

    histo_names:
      - LeadingMuon_pt
      - muon_DR
      - seg_eff_MB1

This tells the framework to look for histogram definitions in the specified Python modules and to fill only the listed histograms.

.. rubric:: Step 2: Define Histograms in Python

In your source modules (e.g., ``dtpr/utils/histograms.py``), define your histograms in a dictionary called ``histos``. For example:

.. code-block:: python

    import ROOT as r

    histos = {}

    histos.update({
        "LeadingMuon_pt": {
            "type": "distribution",
            "histo": r.TH1D("LeadingMuon_pt", r';Leading muon p_T; Events', 20, 0, 1000),
            "func": lambda reader: reader.genmuons[0].pt,
        },
        "muon_DR" : {
        "type" : "distribution",
        "histo" : r.TH1D("muon_DR", r';#DeltaR both muons; Events', 20, 1 , 6),
        "func" : lambda reader: reader.dR,
        },
    })

For efficiency histograms, you can use the following structure (and also use auxiliary functions):

.. code-block:: python

    def am_eff_func(reader, station):
        return [seg.wh for seg in list_of_offline_segments_that_match_genmuon if seg.st == station]

    def am_eff_numdef(reader, station):
        return [ len(seg.am_tps_matches) > 0 for seg in list_of_offline_segments_that_match_genmuon if seg.st == station]

    histos.update({
        "seg_eff_MB1": {
            "type": "eff",
            "histoDen": r.TH1D("Denominator", r';p_{T}; Events', 20, 0, 1000),
            "histoNum": r.TH1D("Numerator", r';p_{T}; Events', 20, 0, 1000),
            "func": lambda reader: am_eff_func(reader, station=1),
            "numdef": lambda reader: am_eff_numdef(reader, station=1),
        },
    })

**Key fields:**

- ``type``: The histogram type (`distribution`, `distribution2d`, `distribution3d`, or `eff` for efficiency).

- ``histo``/``histoDen``/``histoNum``: The ROOT histogram objects.

- ``func``: A function that extracts the value(s) to fill from the event (the ``reader``).

- ``numdef``: For efficiency histograms, a function that returns a boolean or list of booleans indicating which entries go into the numerator.

.. rubric:: Step 3: Run the Histogram Filling Tool

Once your configuration and histogram definitions are set, run:

.. code-block:: shell

    dtpr fill-histos -i [INPATH] -o [OUTPATH] -cf [CONFIGFILE] [other options]

You can also run the histogram filling in parallel by specifying the number of CPU cores with the ``-c`` or ``--ncores`` argument:

.. code-block:: shell

    dtpr fill-histos -i [INPATH] -o [OUTPATH] -cf [CONFIGFILE] -c 4

This will process your events using 4 CPU cores, speeding up the filling for large datasets.

.. rubric:: How It Works

- The tool loads the histogram definitions from the modules listed in ``histo_sources``.
- Only the histograms listed in ``histo_names`` are filled.
- For each event, the tool calls the ``func`` for each histogram, passing the event object (``reader``).
- For efficiency histograms, it also evaluates ``numdef`` to determine which entries go into the numerator.
- The filled histograms are saved to a ROOT file for further analysis or plotting.
- When running in parallel, temporary files are merged automatically at the end.

.. rubric:: Advanced Usage

.. mermaid::
    :name: histogram_filling_diagram
    :align: center
    :zoom: true

    sequenceDiagram
        participant User
        participant dtpr CLI
        participant RUN_CONFIG
        participant NTuple
        participant EventList
        participant Event
        participant Histogram Filler

        User->>dtpr CLI: dtpr fill-histos ...
        dtpr CLI->>RUN_CONFIG: Load/Update config (histo_sources, histo_names)
        dtpr CLI->>Histogram Filler: Call fill_histos(inpath, outfolder, ...)
        Histogram Filler->>Histogram Filler: Call set_histograms_dict()
        Histogram Filler->>RUN_CONFIG: Request histo_sources and histo_names
        Note over Histogram Filler: Loads Python modules and initializes ROOT histograms
        Histogram Filler->>NTuple: Create NTuple(inpath)
        NTuple-->>Histogram Filler: NTuple object ready (contains EventList)

        loop For each event in NTuple.events
            Histogram Filler->>EventList: Request next Event
            EventList->>NTuple: Get raw data entry
            NTuple->>Event: Create Event(raw data)
            Note over Event: Event dynamically builds Particles (Chapter 2)
            NTuple->>NTuple: Apply Preprocessors & Filters (Chapter 4)
            NTuple-->>EventList: Return processed Event (or None)
            EventList-->>Histogram Filler: Return Event

            alt If Event is not None
                Histogram Filler->>Histogram Filler: Call fill_histograms(Event, all_histograms)
                loop For each configured histogram
                    Histogram Filler->>Event: Call histogram's 'func' (e.g., event.genmuons[0].pt)
                    Event-->>Histogram Filler: Return value
                    Histogram Filler->>ROOT Histogram: Fill(value)
                end
            end
        end
        Histogram Filler->>Histogram Filler: Call save_histograms()
        Histogram Filler->>ROOT File: Write all filled histograms
        Histogram Filler-->>dtpr CLI: Done
        dtpr CLI-->>User: Output messages

You can also use the **methods** ``fill_histograms`` and ``save_histograms`` from the ``dtpr.analysis.fill_histograms`` module directly in your own scripts for more customized workflows.

.. automodule:: dtpr.analysis.fill_histograms
    :members: fill_histograms, save_histograms
    :member-order: bysource
