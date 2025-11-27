from .event import Event


class EventList:
    """
    A class to manage events such as a list, but without loading all events in memory.
    """

    def __init__(self, tree, processor=None):
        """
        Initialize an EventList instance.

        :param tree: The ROOT TTree containing the events information.
        :type tree: ROOT.TChain
        :param processor: The methods to preprocess the events.
        :type processor: function, optional
        :param use_config: Flag to indicate if configuration file should be used to build events (default is False).
        :type use_config: bool, optional
        """
        self._tree = tree
        self._processor = processor
        self._length = tree.GetEntries()

    def __len__(self):
        """
        Get the number of events in the EventList.
        """
        return self._length

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
            return (self[i] for i in range(*index.indices(self._length)))  # Return a generator
        elif isinstance(index, int):
            if abs(index) >= self._length:
                raise IndexError("Event index out of range")
            if index < 0:
                index += self._length

            for iev, ev in enumerate(self._tree):
                if iev == index:
                    event = Event(ev, iev, use_config=True)
                    if self._processor:
                        return self._processor(event)
                    else:
                        return event
            raise IndexError("Event index out of range")
        else:
            raise TypeError("Invalid argument type")

    def get_by_number(self, number):
        """
        Retrieve an event by its number attribute. Becareful, this method requires to instantiate events one by one
        and can be slow for large datasets.

        :param number: The number attribute of the event to retrieve.
        :type number: int
        :returns: Event: The event with the specified number.
        :rtype: Event

        :raises: ValueError: If no event with the specified number is found.
        """
        for iev, ev in enumerate(self._tree):
            if getattr(ev, "event_eventNumber", None) == number:
                event = Event(ev, iev, use_config=True)
                return self._processor(event)
        raise ValueError(f"No event found with number: {number}")

    def __iter__(self):
        """
        Iterate over the events in the EventList.
        """
        for iev, ev in enumerate(self._tree):
            event = Event(ev, iev, use_config=True)
            yield self._processor(event)

    def __repr__(self):
        """
        Return a string representation of the EventList.
        """
        return f"<EventList with {self._length} events>"
