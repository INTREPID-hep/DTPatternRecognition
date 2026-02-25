"""
EventList — thin proxy over an Awkward Array of EventRecord objects.

Maintains backward-compatible interface:
  len(ntuple.events)            → number of events
  ntuple.events[0]              → EventRecord (single event)
  ntuple.events[2:5]            → ak.Array slice (iterable of EventRecord)
  for ev in ntuple.events       → iterates EventRecord items
  ntuple.events.get_by_number() → lookup by event_eventNumber field
"""

import awkward as ak


class EventList:
    """
    Proxy wrapper over an ``ak.Array`` of :class:`~dtpr.base.event.EventRecord`
    objects produced by :class:`~dtpr.base.ntuple.NTuple`.

    The underlying array is accessible via ``._events`` if needed for advanced
    columnar operations.
    """

    def __init__(self, ak_events: ak.Array):
        """
        Parameters
        ----------
        ak_events : ak.Array
            The full events array returned by ``NanoEventsFactory.events()``,
            after enrichment has been applied.
        """
        self._events = ak_events

    # ------------------------------------------------------------------
    # Core sequence interface
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return len(self._events)

    def __getitem__(self, index):
        """
        Return a single :class:`~dtpr.base.event.EventRecord` (int index) or
        an ``ak.Array`` slice (slice index, iterable of EventRecord).
        """
        if isinstance(index, (int, slice, ak.Array)):
            return self._events[index]
        raise TypeError(f"Invalid index type: {type(index)}")

    def __iter__(self):
        """Iterate over individual EventRecord objects."""
        yield from self._events

    def __repr__(self) -> str:
        return f"<EventList with {len(self._events)} events>"

    # ------------------------------------------------------------------
    # Named lookup
    # ------------------------------------------------------------------

    def get_by_number(self, number: int):
        """
        Return the event whose ``event_eventNumber`` equals *number*.

        Uses vectorised Awkward filtering — much faster than the old
        per-event iteration approach.

        Parameters
        ----------
        number : int
            The event number to look up.

        Raises
        ------
        ValueError
            If no event with the given number is found.
        """
        mask = self._events["event_eventNumber"] == number
        result = self._events[mask]
        if len(result) == 0:
            raise ValueError(f"No event found with event_eventNumber == {number}")
        return result[0]
