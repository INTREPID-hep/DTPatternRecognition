"""
Awkward Array behavior classes for particle records and collections.

- ``ParticleRecord``  → :class:`ak.Record` subclass dispatched for ``behavior["Particle"]``
- ``ParticleArray``   → :class:`ak.Array` subclass dispatched for ``behavior["*Particle"]``
- ``behavior``        → dict to be passed to :func:`coffea.nanoevents.NanoEventsFactory.from_root`

The class and behavior key names are intentionally generic so this package is
reusable beyond the CMS DT detector context.
"""

import re
from functools import cached_property
import awkward as ak

from ..utils.functions import color_msg, find_field_by_pattern


class ParticleRecord(ak.Record):
    """Single particle record (one row of a jagged particle collection).

    Dispatched automatically by Awkward when the inner ``__record__`` parameter
    of the collection is ``"Particle"``.

    Equivalent to the old per-event ``Particle`` instance but backed by a lazy
    Awkward slice rather than in-memory Python attributes.
    """

    _IDX_PATTERN = re.compile(r"\b(idx|id|index|num(ber)?)\b", re.IGNORECASE)

    @cached_property
    def _particle_label(self) -> str:
        """Collection name (from schema) + optional index field value."""
        collection = self.layout.parameter("__collection__") or "Particle"
        idx_field = find_field_by_pattern(self.fields, self._IDX_PATTERN)
        if idx_field is not None:
            return f"{collection} {self[idx_field]}"
        return collection

    def __repr__(self) -> str:
        parts = ", ".join(f"{f}={self[f]!r}" for f in self.fields)
        return f"<{self._particle_label} {parts}>"

    def __str__(self, indentLevel: int = 0, include=None, exclude=None, **kwargs) -> str:
        """Human-readable summary, compatible with the old ``Particle.__str__`` style."""
        fields = [
            f
            for f in self.fields
            if (include is None or f in include) and (exclude is None or f not in exclude)
        ]
        header = color_msg(
            f"{self._particle_label} -->",
            color=kwargs.pop("color", "yellow"),
            indentLevel=indentLevel,
            return_str=True,
            **kwargs,
        )
        body = color_msg(
            ", ".join(f"{f.capitalize()}: {self[f]}" for f in fields),
            indentLevel=indentLevel + 1,
            return_str=True,
        )
        return "\n".join([header, body])



class ParticleArray(ak.Array):
    """Collection of particle records.

    Dispatched automatically by Awkward for columnar collection access
    (``events["digis"]``).  Single-event slices (``events[0]["digis"]``)
    return a plain ``ak.Array`` — use columnar access for best results.
    """

    def __repr__(self) -> str:
        return f"<ParticleArray len={len(self)}>"


# ---------------------------------------------------------------------------
# Behavior dictionary — the single source of truth for Awkward dispatch.
# Merge this dict (or import it) into the combined behavior dict passed to
# NanoEventsFactory.
# ---------------------------------------------------------------------------
behavior: dict = {
    "Particle": ParticleRecord,
    "*Particle": ParticleArray,
}

