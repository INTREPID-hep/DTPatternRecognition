import ROOT as r
import os
from dtpr.base import Event, EventList
from dtpr.utils.functions import color_msg
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

    def __init__(self, inputFolder, selectors=[], preprocessors=[], maxfiles=-1, tree_name="/TTREE"):
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
        self._tree_name = tree_name
        self.tree = r.TChain()

        # Prepare input
        self.load_tree(inputFolder)
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
    import os
    # path to the DT Ntuple, it could be a folder with several files
    input_folder = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "../../test/ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root",
            )
        )

    ntuple = NTuple(input_folder, tree_name="/dtNtupleProducer/DTTREE")
    # you can access events by index or slice such as a list
    print(ntuple.events[10])
    print(ntuple.events[3:5]) # slicing return a generator

    # or loop over the events
    for iev, ev in enumerate(ntuple.events):
        print(ev)
        break

    # or simply use the TTree
    for i, ev in enumerate(ntuple.tree):
        print(f"Event orbit number: {ev.event_orbitNumber}")
        break