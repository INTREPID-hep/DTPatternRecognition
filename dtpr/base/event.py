"""
Awkward Array behavior class for event records.

- ``EventRecord``  → :class:`ak.Record` subclass dispatched for ``behavior["Event"]``
- ``behavior``     → dict combining Event + Particle behaviors for :class:`~coffea.nanoevents.NanoEventsFactory`

The class and behavior key names are intentionally generic.
"""

import re
from functools import cached_property
import awkward as ak

from ..utils.functions import color_msg, find_field_by_pattern
from .particle import behavior as particle_behavior


class EventRecord(ak.Record):
    """Single event record, backed by a lazy Awkward array slice.

    Dispatched automatically by Awkward when the top-level array has
    ``__record__ = "Event"`` in its parameters (set by ``PatternSchema``).

    Equivalent to the old ``Event`` class but without per-event Python objects.
    """

    # ------------------------------------------------------------------
    # Representation helpers
    # ------------------------------------------------------------------

    _ID_PATTERN = re.compile(r"(ev(ent)?|num(ber)?|index|idx)", re.IGNORECASE)

    @staticmethod
    def _find_id_field(fields: list[str]) -> str | None:
        """Return the first field whose name looks like an event identifier, or None."""
        return find_field_by_pattern(fields, EventRecord._ID_PATTERN)

    @cached_property
    def _event_label(self) -> str:
        """Best human-readable identifier for this event.

        Computed once and cached on the instance.  Tries to find a scalar
        field whose name matches a common event-ID pattern (event, number,
        index, …).  Falls back to the positional index within the parent
        array (``layout.at``).
        """
        id_field = self._find_id_field(self.fields)
        if id_field is not None:
            val = self[id_field]
            # only use it if it's a plain scalar, not a collection
            if not (hasattr(val, "fields") and val.fields):
                return f"{val}"
        return f"{self.layout.at}"

    # ------------------------------------------------------------------
    # Representation
    # ------------------------------------------------------------------
    def __repr__(self) -> str:
        return f"<Event {self._event_label}>"

    def __str__(self, indentLevel: int = 0) -> str:
        """Verbose summary of all fields in this event record.

        Visual convention
        -----------------
        - Yellow  : event header
        - Cyan    : particle collection label  (field has nested sub-fields)
        - Purple  : particle count
        - Green   : scalar field label
        - (none)  : scalar field value
        """
        lines = [
            color_msg(
                f"------ Event {self._event_label} ------",
                color="yellow",
                indentLevel=indentLevel,
                return_str=True,
            )
        ]
        for f in self.fields:
            val = self[f]
            is_collection = hasattr(val, "fields") and len(val.fields) > 0
            if is_collection:
                # Particle collection — cyan label + purple count
                lines.append(
                    color_msg(f"{f}", color="purple", indentLevel=indentLevel + 1, return_str=True)
                    + color_msg(
                        f"({len(val)} items, fields: {val.fields})",
                        color="none",
                        indentLevel=-1,
                        return_str=True,
                    )
                )
            else:
                # Scalar — green label, plain value
                lines.append(
                    color_msg(f"{f}:", color="green", indentLevel=indentLevel + 1, return_str=True)
                    + color_msg(f" {val}", color="none", indentLevel=-1, return_str=True)
                )
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Combined behavior dictionary used by NanoEventsFactory.
# Includes both Event and Particle behaviors so each level dispatches correctly.
# ---------------------------------------------------------------------------
behavior: dict = {
    "Event": EventRecord,
    **particle_behavior,
}

