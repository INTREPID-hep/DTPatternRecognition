from .event import Event
from .config import RUN_CONFIG


class EventList:
    """
    A class to manage events such as a list, but without loading all events in memory.

    The list can be restricted to a contiguous entry range of the tree
    (``begin`` inclusive, ``end`` exclusive), which is how the parallel
    commands split one file into chunks. Indices given to this object are
    relative to the range, while ``Event.index`` keeps the entry number in the
    tree, so slicing a range yields exactly the same events as the full list.
    """

    def __init__(self, tree, processor=None, CONFIG=None, begin=0, end=None):
        """
        Initialize an EventList instance.

        :param tree: The ROOT TTree containing the events information.
        :type tree: ROOT.TChain
        :param processor: The methods to preprocess the events.
        :type processor: function, optional
        :param use_config: Flag to indicate if configuration file should be used to build events (default is False).
        :type use_config: bool, optional
        :param begin: First entry of the tree to expose (inclusive). Default is 0.
        :type begin: int, optional
        :param end: Last entry of the tree to expose (exclusive). Default is None (all entries).
        :type end: int, optional
        """
        self._tree = tree
        self._processor = processor
        self.CONFIG = CONFIG if CONFIG is not None else RUN_CONFIG

        total = tree.GetEntries()
        self._begin = max(0, min(int(begin or 0), total))
        self._end = total if end is None else max(self._begin, min(int(end), total))
        self._length = self._end - self._begin

    def __len__(self):
        """
        Get the number of events in the EventList.
        """
        return self._length

    def _make_event(self, index):
        """Internal helper to build and process the event object.

        :param index: Index inside this list (0 is the first entry of the range).
        :type index: int
        """
        entry = self._begin + index

        # Load the data into the tree's internal buffers
        if self._tree.GetEntry(entry) <= 0:
            raise RuntimeError(f"Failed to retrieve event at index {entry}")

        # Create the event (index and number are entry numbers, not list positions)
        event = Event(self._tree, entry, use_config=True, CONFIG=self.CONFIG)

        # Return processed or raw
        return self._processor(event) if self._processor else event

    def __getitem__(self, index):
        """
        Retrieve an event or a generator of events by index.

        :param index: The index or slice to retrieve the event(s). If an integer is provided, a single
            event is returned. If a slice is provided, a generator of events is returned.
        :type index: int or slice
        :returns: Event or generator of Event: The event(s) corresponding to the given index.
        :rtype: Event or generator of Event

        :raises: IndexError: If the index is out of range.
        :raises: TypeError: If the index type is invalid.
        """
        if isinstance(index, slice):
            return (self[i] for i in range(*index.indices(self._length)))

        if not isinstance(index, int):
            raise TypeError(f"Invalid argument type: {type(index)}")

        # Handle negative indexing
        if index < 0:
            index += self._length

        if index < 0 or index >= self._length:
            raise IndexError("Event index out of range")

        return self._make_event(index)

    def get_by_number(self, number):
        """
        Retrieve an event by its number attribute (event_eventNumber).

        :param number: The number attribute of the event to retrieve.
        :type number: int
        :returns: Event: The event with the specified number.
        :rtype: Event

        :raises: ValueError: If no event with the specified number is found in this list.
        """
        # A missing index comes back as a null cppyy proxy, which is falsy but is not None
        if not self._tree.GetTreeIndex():
            self._tree.BuildIndex("event_eventNumber", "0")

        entry = self._tree.GetEntryNumberWithIndex(number, 0)

        if entry < 0 or entry < self._begin or entry >= self._end:
            raise ValueError(f"No event found with number: {number}")

        return self._make_event(entry - self._begin)

    def __iter__(self):
        """
        Iterate over the events safely.
        """
        for i in range(self._length):
            yield self._make_event(i)

    def __repr__(self):
        return f"<EventList with {self._length} events>"
