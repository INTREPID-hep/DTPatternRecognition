"""
Awkward Array behavior class for event records.

- ``EventRecord``  → :class:`ak.Record` subclass dispatched for ``behavior["Event"]``
- ``behavior``     → dict combining Event + Particle behaviors for :class:`~coffea.nanoevents.NanoEventsFactory`

The class and behavior key names are intentionally generic.
"""

import warnings
import awkward as ak

from ..utils.functions import color_msg
from .particle import behavior as particle_behavior


class EventRecord(ak.Record):
    """Single event record, backed by a lazy Awkward array slice.

    Dispatched automatically by Awkward when the top-level array has
    ``__record__ = "Event"`` in its parameters (set by ``PatternSchema``).

    Equivalent to the old ``Event`` class but without per-event Python objects.
    """

    # ------------------------------------------------------------------
    # Core properties
    # ------------------------------------------------------------------
    def __bool__(self) -> bool:
        """Always truthy — a loaded EventRecord is always a valid event."""
        return True

    @property
    def number(self) -> int:
        """The event number (``event_eventNumber`` branch), or ``None`` if absent."""
        try:
            return int(self["event_eventNumber"])
        except (KeyError, IndexError, ValueError):
            return None

    @property
    def index(self) -> int:
        """Alias kept for backward compatibility — same as ``number``."""
        return self.number

    # ------------------------------------------------------------------
    # Representation
    # ------------------------------------------------------------------
    def __repr__(self) -> str:
        return f"<Event {self.number}>"

    def __str__(self, indentLevel: int = 0) -> str:
        """Verbose summary compatible with the old ``Event.__str__`` output style."""
        header = color_msg(
            f"------ Event {self.number} info ------",
            color="yellow",
            indentLevel=indentLevel,
            return_str=True,
        )
        lines = [header]
        for f in self.fields:
            val = self[f]
            try:
                n = len(val)
                lines.append(
                    color_msg(
                        f"{f}: ({n} items)",
                        indentLevel=indentLevel + 1,
                        return_str=True,
                    )
                )
            except TypeError:
                lines.append(
                    color_msg(
                        f"{f}: {val}",
                        indentLevel=indentLevel + 1,
                        return_str=True,
                    )
                )
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Particle access helpers
    # ------------------------------------------------------------------
    def filter_particles(self, particle_type: str, **kwargs):
        """Return the particles of *particle_type* that match all *kwargs* field values.

        Columnar equivalent of the old ``Event.filter_particles``.

        Parameters
        ----------
        particle_type : str
            Name of the particle collection field (e.g. ``"digis"``).
        **kwargs
            Field/value pairs to filter by (e.g. ``wh=1, sc=2``).

        Returns
        -------
        ak.Array
            A (possibly empty) subset of the collection.
        """
        try:
            collection = self[particle_type]
        except (KeyError, IndexError):
            warnings.warn(
                f"Invalid particle type '{particle_type}'. Available fields: {self.fields}"
            )
            return ak.Array([])

        if not kwargs:
            return collection

        # Validate requested keys against available fields
        available = set(collection.fields)
        invalid = set(kwargs) - available
        if invalid:
            raise ValueError(
                f"Invalid filter keys {invalid}. Valid keys are: {available}"
            )

        mask = ak.ones_like(collection[next(iter(available))], dtype=bool)
        for key, val in kwargs.items():
            mask = mask & (collection[key] == val)
        return collection[mask]


# ---------------------------------------------------------------------------
# Combined behavior dictionary used by NanoEventsFactory.
# Includes both Event and Particle behaviors so each level dispatches correctly.
# ---------------------------------------------------------------------------
behavior: dict = {
    "Event": EventRecord,
    **particle_behavior,
}
