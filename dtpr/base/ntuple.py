import ROOT as r
import os
from functools import partial
from .event import Event
from .event_list import EventList
from .config import RUN_CONFIG
from ..utils.functions import color_msg, get_callable_from_src
from natsort import natsorted
import warnings


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

    def __init__(self, inputFolder, selectors=None, preprocessors=None, maxfiles=-1, CONFIG=None):
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
        # Save in attributes (avoid sharing mutable defaults)
        self._selectors = list(selectors) if selectors is not None else []
        self._preprocessors = list(preprocessors) if preprocessors is not None else []
        self._maxfiles = maxfiles
        self.tree = r.TChain()
        self.CONFIG = CONFIG if CONFIG is not None else RUN_CONFIG
        _tree_name = getattr(self.CONFIG, "ntuple_tree_name", None)
        if _tree_name is None:
            warnings.warn(f"No tree name provided in CONFIG. Defaulting to '/TTREE'.")
            self._tree_name = "/TTREE"
        else:
            self._tree_name = _tree_name

        # Prepare input
        self.load_tree(inputFolder)
        # Load selectors from config
        self._load_from_config("ntuple_selectors")
        # Load preprocessors from config
        self._load_from_config("ntuple_preprocessors")

        self.events = EventList(self.tree, self.event_processor, CONFIG=self.CONFIG)

    def event_processor(self, ev: Event):
        """
        Preprocess the event.

        :param ev: The event to preprocess.
        :type ev: Event
        :returns: The preprocessed event if it passes the selection criteria, otherwise None.
        :rtype: Event
        """
        # Check if event passes config-based filter (if defined)
        if not getattr(ev, '_passes_filter', True):
            return None
        
        if self._preprocessors:  # Apply preprocessors if they exist
            self._preprocess_event(ev)
        if self._selectors:  # Apply global selection if selectors exist
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

        :param config_key: The key in the CONFIG to look for items.
        :type config_key: str
        """
        if "selector" in config_key:
            target_list = self._selectors
            item_type = "selector"
        elif "preprocessor" in config_key:
            target_list = self._preprocessors
            item_type = "preprocessor"
        else:
            raise ValueError(
                f"Invalid config_key: {config_key}. Must contain 'selector' or 'preprocessor'."
            )

        items = []
        for name, item_info in getattr(self.CONFIG, config_key, {}).items():
            src = item_info.get("src", None)
            if src is None:
                raise ValueError(
                    f"{item_type.capitalize()} {name} has no src defined in the config."
                )
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

        def get_root_files(directory):
            """Helper function to recursively collect .root files using os.scandir."""
            root_files = []
            for entry in os.scandir(directory):
                if entry.is_dir():
                    root_files.extend(get_root_files(entry.path))
                elif entry.is_file() and entry.name.endswith(".root"):
                    root_files.append(entry.path)
            return root_files

        if "root" in inpath:
            color_msg(f"Opening input file {inpath}", "blue", 1)
            self.tree.Add(inpath + self._tree_name)
            self._maxfiles = 1
        else:
            color_msg(f"Opening input files from {inpath}", "blue", 1)
            allFiles = natsorted(get_root_files(inpath))
            nFiles = len(allFiles) if self._maxfiles == -1 else min(len(allFiles), self._maxfiles)
            self._maxfiles = nFiles

            for iF in range(nFiles):
                color_msg(f"File {allFiles[iF].split('/')[-1]} added", indentLevel=2)
                self.tree.Add(allFiles[iF] + self._tree_name)


if __name__ == "__main__":
    input_file = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "../../tests/ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root",
        )
    )
    cf_path = RUN_CONFIG.path
    # [start-example-1]
    from dtpr.base.config import RUN_CONFIG

    # update the configuration file -> if it is not set, it will use the default one 'run_config.yaml'
    RUN_CONFIG.change_config_file(config_path=cf_path)

    # input_file could be the path to the DT Ntuple, or to a folder with several files
    ntuple = NTuple(
        input_file
    )  # It also admits passing a list of selectors and preprocessors when instantiating

    # you can access events by index or slice such as a list
    print(ntuple.events[9])
    print(ntuple.events[3:5])  # slicing return a generator

    # you can also loop over the events
    for iev, ev in enumerate(ntuple.events):
        print(ev)
        break

    # or simply, you can still use the TTree (It is a ROOT.TChain)
    for i, ev in enumerate(ntuple.tree):
        print(f"Event orbit number: {ev.event_orbitNumber}")
        break
