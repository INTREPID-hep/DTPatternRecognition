fill-histos
===========

This tool allows you to recursively fill predefined histograms event by event. Histograms should be contained in a Python dictionary called `histo` and follow the formats below:

For Efficiencies
----------------

.. code-block:: python

    histos.update({
        "HISTONAME": {
            "type": "eff",
            "histoDen": r.TH1D("Denominator name", r';TitleX; Events', nBins, firstBin, lastBin),
            "histoNum": r.TH1D("Numerator name", r';TitleX; Events', nBins, firstBin, lastBin),
            "func": __function_for_filling_histogram__,
            "numdef": __condition_for_numerator__
        },
    })

Where:

* ``HISTONAME``: The name that will appear in the output ``.root`` file.

* ``func``: A function that provides the value for filling the histogram. It takes the ``reader`` as input and can fetch all the objects reconstructed by the ``reader``. Examples:
    - ``lambda reader: reader.genmuons[0].pt`` fills a histogram with the leading muon pT.
    - ``lambda reader: [seg.wh for seg in fcns.get_best_matches(reader, station=1)]`` gets the best matching segments in MB1 (``station=1``) and fills with the wheel value of the matching segment. The ``get_best_matches`` function is defined in ``utils/functions.py``.

* ``numdef``: A boolean function to differentiate between the denominator and numerator. The numerator is filled only when this function returns ``True``. If ``func`` returns a list, this must return a list of booleans of the same length.

For Flat Distributions
-----------------------

.. code-block:: python

    "HISTONAME": {
        "type": "distribution",
        "histo": r.TH1D("HISTONAME", r';TitleX; Events', nBins, firstBin, lastBin),
        "func": __function_for_filling_histograms__,
    }

The `reader` argument in functions represents an `event` entry, which should be an instance of ``dtpr.base.Event`` created by the implemented ``NTuple`` child. A set of predefined histograms is available in ``dtpr/utils/histograms``.

Once your histograms are defined, include them in the ``run_config.yaml`` file by specifying:

.. code-block:: yaml

    .
    .
    .

    histo_sources:
        # Define the source modules of the histograms
        - dtpr.utils.histograms.baseHistos
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

Then, run the following command to fill the histograms:

.. code-block:: shell

    dtpr fill-histos -i [INPATH] -o [OUTPATH] ...

