# Module to read DT Ntuples and create Event instances from root entries

# -- Import libraries -- #
import os
import importlib
from dtpr.base import Event, NTuple
from src.utils.genmuon_functions import analyze_genmuon_matches, analyze_genmuon_showers
from src.utils.shower_functions import build_fwshowers, build_real_showers, analyze_fwshowers
from src.utils.config import RUN_CONFIG

class DtNtuple(NTuple):
    def __init__(self, inputFolder, maxfiles=-1):
        """
        Initialize a DtNtuple instance.

        :param inputFolder: The folder containing the input files.
        :type inputFolder: str
        :param selectors: A list of selector functions to apply to the events. See dtpr.utils.filters
        :type selectors: list
        :param maxfiles: The maximum number of files to process. Defaults to -1 (process all files).
        :type maxfiles: int, optional
        """
        # load params from config
        params = RUN_CONFIG.ntuple_params
        self.method = params["genmuon-showered-method"]
        self.threshold = params["shower-threshold"]

        selectors = []
        for source in RUN_CONFIG.ntuple_selectors:
            selector_module, selector_name = source.rsplit('.', 1)
            module = importlib.import_module(selector_module)
            selectors.append(getattr(module, selector_name))

        super().__init__(inputFolder, selectors, maxfiles, tree_name="/dtNtupleProducer/DTTREE")

    def event_preprocessor(self, ev: Event):
        """
        Preprocess the event. Specific to DtNtuple.

        :param ev: The event to preprocess.
        :type ev: Event
        :returns: The preprocessed event if it passes the selection criteria, otherwise None.
        :rtype: Event
        """
        analyze_genmuon_matches(ev)
        analyze_genmuon_showers(ev, method=self.method)
        build_real_showers(ev, threshold=self.threshold) # to build real showers
        build_fwshowers(ev, threshold=self.threshold)  # to build fwshowers
        # analyze_fwshowers(ev) # to label fwshowers as true
        # Apply global selection
        if not self.select_event(ev):
            return None
        else:
            return ev

if __name__ == "__main__":
    """
    Example of how to use the DtNtuple class to read DT Ntuples and analyze events.
    """
    def example_selector(event):
        # Example selector function that always returns True
        return True

    # input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../dtpr-package/dtpr/utils/templates/DTDPGNtuple_12_4_2_Phase2Concentrator_Simulation_101.root"))
    input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "./DTDPGNtuple_12_4_2_Phase2Concentrator_Simulation_99.root"))
    selectors = [example_selector]

    dt_ntuple = DtNtuple(input_folder, selectors,)
    # you can access events by index such as a list
    print(dt_ntuple.events[10])
    print(dt_ntuple.events[3:5])

    # or loop over the events
    for iev, ev in enumerate(dt_ntuple.events):
        print(ev)
        if iev == 0: break

    # or simply use the root tree, which is a ROOT.TChain
    for i, ev in enumerate(dt_ntuple.tree):
        print("event orbit number: ", ev.event_orbitNumber)
        if i == 0: break

