import ROOT as r
import os
from functools import partial
from dtpr.base import Event, EventList
from dtpr.utils.functions import color_msg, get_callable_from_src
from dtpr.utils.config import RUN_CONFIG
from natsort import natsorted


class NTuple(object):
    """
    A class to handle the loading and processing of NTuple's ROOT TTrees.

    Attributes
    ----------
    tree : ROOT.TChain
        The TChain containing the loaded TTrees.
    events : EventList
        The list of events created from the TTree.
    """

    def __init__(self, inputFolder, selectors=[], preprocessors=[], maxfiles=-1):
        """
        Initialize an NTuple instance.

        :param inputFolder: The folder containing the input files.
        :type inputFolder: str
        :param selectors: A list of selector functions to apply to the events. See dtpr.utils.filters
        :type selectors: list of callables
        :param preprocessors: A list of preprocessor functions to apply to the events.
        :type preprocessors: list of callables
        :param maxfiles: The maximum number of files to process. Defaults to -1 (process all files).
        :type maxfiles: int, optional
        :param tree_name: The name of the TTree to load. Defaults to "/TTREE".
        :type tree_name: str, optional
        """
        # Save in attributes
        self._selectors = selectors
        self._preprocessors = preprocessors
        self._maxfiles = maxfiles
        self.tree = r.TChain()
        
        _tree_name = getattr(RUN_CONFIG, 'ntuple_tree_name', None)
        if _tree_name is None:
            Warning.warn(f"No tree name provided in RUN_CONFIG. Defaulting to '/TTREE'.")
            self._tree_name = "/TTREE"
        else:
            self._tree_name = _tree_name

        # Prepare input
        self.load_tree(inputFolder)
        # Load selectors from config
        self._load_from_config("ntuple_selectors")
        # Load preprocessors from config
        self._load_from_config("ntuple_preprocessors")

        self.events = EventList(self.tree, self.event_processor)

    def event_processor(self, ev: Event):
        """
        Preprocess the event.

        :param ev: The event to preprocess.
        :type ev: Event
        :returns: The preprocessed event if it passes the selection criteria, otherwise None.
        :rtype: Event
        """
        if self._preprocessors: # Apply preprocessors if they exist
            self._preprocess_event(ev)
        if self._selectors: # Apply global selection if selectors exist
            if not self._select_event(ev):
                return None
        return ev

    def _preprocess_event(self, ev: Event):
        """
        Preprocess the event.

        :param ev: The event to preprocess.
        :type ev: Event
        :returns: The preprocessed event if it passes the selection criteria, otherwise None.
        :rtype: Event
        """
        for preprocessor in self._preprocessors:
            preprocessor(ev)

    def _select_event(self, ev: Event):
        """
        Apply global cuts on the events using the selectors.

        :param ev: The event to check.
        :type ev: Event
        :returns: True if the event passes all selectors, False otherwise.
        :rtype: bool
        """
        return all(selector(ev) for selector in self._selectors)

    def _load_from_config(self, config_key):
        """
        Load items (selectors or preprocessors) from the configuration file.

        :param config_key: The key in the RUN_CONFIG to look for items.
        :type config_key: str
        """
        if "selector" in config_key:
            target_list = self._selectors
            item_type = "selector"
        elif "preprocessor" in config_key:
            target_list = self._preprocessors
            item_type = "preprocessor"
        else:
            raise ValueError(f"Invalid config_key: {config_key}. Must contain 'selector' or 'preprocessor'.")

        items = []
        for name, item_info in getattr(RUN_CONFIG, config_key, {}).items():
            src = item_info.get("src", None)
            if src is None:
                raise ValueError(f"{item_type.capitalize()} {name} has no src defined in the config.")
            item = get_callable_from_src(src)
            if item is None:
                raise ImportError(f"{item_type.capitalize()} {name} not found in module {src}.")
            kwargs = item_info.get("kwargs", {})
            if kwargs:
                items.append(partial(item, **kwargs))
            else:
                items.append(item)
        if items:
            target_list.extend(items)

    def load_tree(self, inpath):
        """
        Retrieve a chain with all the trees to be analyzed.

        :param inpath: The path to the input files.
        :type inpath: str
        """

        if "root" in inpath:
            color_msg(f"Opening input file {inpath}", "blue", 1)
            self.tree.Add(inpath + self._tree_name)
            self._maxfiles = 1
        else:
            color_msg(f"Opening input files from {inpath}", "blue", 1)
            allFiles = natsorted(os.listdir(inpath))
            nFiles = (
                len(allFiles)
                if self._maxfiles == -1
                else min(len(allFiles), self._maxfiles)
            )
            self._maxfiles = nFiles

            for iF in range(nFiles):
                if "root" not in allFiles[iF]:
                    continue
                color_msg(f"File {allFiles[iF]} added", indentLevel=2)
                self.tree.Add(os.path.join(inpath, allFiles[iF]) + self._tree_name)

if __name__ == "__main__":
    input_file = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "../../test/ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root",
            )
        )
    cf_path = RUN_CONFIG.path
    # [start-example-1]
    from dtpr.utils.config import RUN_CONFIG

    # update the configuration file -> if it is not set, it will use the default one 'run_config.yaml'
    RUN_CONFIG.change_config_file(config_path=cf_path)

    # input_file could be the path to the DT Ntuple, or to a folder with several files
    ntuple = NTuple(input_file) # It also admits passing a list of selectors and preprocessors when instantiating

    # you can access events by index or slice such as a list
    print(ntuple.events[9])
    print(ntuple.events[3:5]) # slicing return a generator

    # you can also loop over the events
    for iev, ev in enumerate(ntuple.events):
        print(ev)
        break

    # or simply, you can still use the TTree (It is a ROOT.TChain)
    for i, ev in enumerate(ntuple.tree):
        print(f"Event orbit number: {ev.event_orbitNumber}")
        break