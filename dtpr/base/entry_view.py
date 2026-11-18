"""A cached, plain-Python view of one TTree entry.

Every ``getattr`` on a ``TChain`` re-converts the ROOT vector of the branch, which
is O(N) even when the caller only needs one element. ``EntryView`` converts
each branch at most once per entry and hands the same Python value to every
later reader of that branch.
"""


def materialize(obj):
    """Convert PyROOT/cppyy proxy containers into plain Python values.

    The guard set matters: a string branch must stay a string (iterating it
    would shred it into characters), a ``TClonesArray`` and a
    ``vector<vector<T>>`` need the recursive form, and a scalar must be
    returned as it is.
    """
    if isinstance(obj, (str, bytes, dict)):
        return obj
    if hasattr(obj, "__iter__") or hasattr(obj, "__getitem__") or hasattr(obj, "__len__"):
        return [materialize(item) for item in obj]
    return obj


class EntryView:
    """The current entry of a tree, with each branch converted at most once.

    ``set_entry`` loads an entry and drops the cache; afterwards
    ``getattr(view, branch)`` returns the materialized value of the branch, the
    same object on every call.

    :param tree: The TChain or TTree to read from.
    :type tree: ROOT.TChain or ROOT.TTree
    """

    __slots__ = ("_tree", "_cache")

    def __init__(self, tree):
        self._tree = tree
        self._cache = {}

    def set_entry(self, entry):
        """Load ``entry`` into the tree buffers and forget the previous values.

        :param entry: Entry number in the tree.
        :type entry: int
        """
        if self._tree.GetEntry(entry) <= 0:
            raise RuntimeError(f"Failed to retrieve event at index {entry}")
        self._cache.clear()

    def __getattr__(self, name):
        cache = self._cache
        try:
            return cache[name]
        except KeyError:
            pass
        value = materialize(getattr(self._tree, name))
        cache[name] = value
        return value

    def __repr__(self):
        return f"<EntryView of {self._tree.GetEntries()} entries, {len(self._cache)} cached>"
