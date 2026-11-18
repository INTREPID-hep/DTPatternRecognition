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

    A plain list is returned as it is. It is the hot path: EntryView hands the
    same materialized list to every particle that reads the branch, and walking
    it again for every particle would put the O(N^2) back.
    """
    if isinstance(obj, list):
        return obj
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

    def cumulative_offsets(self, counts_branch):
        """Start offset of the flat values of every particle of a ``[flat, counts]`` pair.

        The flat branch of a pair stores the concatenated values of every
        particle, so particle ``i`` starts at ``sum(counts[:i])``. Computing
        that sum per particle is quadratic; the offsets are computed once per
        entry and cached under the counts branch name.

        :param counts_branch: Name of the branch holding the per-particle counts.
        :type counts_branch: str
        :returns: List of length ``len(counts) + 1``; offsets[i] is the start of particle i.
        :rtype: list of int
        """
        key = ("offsets", counts_branch)
        cache = self._cache
        try:
            return cache[key]
        except KeyError:
            pass
        counts = self.__getattr__(counts_branch)
        offsets = [0] * (len(counts) + 1)
        total = 0
        for index, count in enumerate(counts):
            offsets[index] = total
            total += count
        offsets[len(counts)] = total
        cache[key] = offsets
        return offsets

    def __repr__(self):
        return f"<EntryView of {self._tree.GetEntries()} entries, {len(self._cache)} cached>"
