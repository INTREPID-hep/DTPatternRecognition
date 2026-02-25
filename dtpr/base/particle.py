"""
Awkward Array behavior classes for particle records and collections.

- ``ParticleRecord``  → :class:`ak.Record` subclass dispatched for ``behavior["Particle"]``
- ``ParticleArray``   → :class:`ak.Array` subclass dispatched for ``behavior["*Particle"]``
- ``behavior``        → dict to be passed to :func:`coffea.nanoevents.NanoEventsFactory.from_root`

The class and behavior key names are intentionally generic so this package is
reusable beyond the CMS DT detector context.
"""

import awkward as ak

from ..utils.functions import color_msg


class ParticleRecord(ak.Record):
    """Single particle record (one row of a jagged particle collection).

    Dispatched automatically by Awkward when the inner ``__record__`` parameter
    of the collection is ``"Particle"``.

    Equivalent to the old per-event ``Particle`` instance but backed by a lazy
    Awkward slice rather than in-memory Python attributes.
    """

    def __repr__(self) -> str:
        parts = ", ".join(f"{f}={self[f]!r}" for f in self.fields)
        return f"<Particle {parts}>"

    def __str__(self, indentLevel: int = 0, include=None, exclude=None, **kwargs) -> str:
        """Human-readable summary, compatible with the old ``Particle.__str__`` style."""
        fields = [
            f
            for f in self.fields
            if (include is None or f in include) and (exclude is None or f not in exclude)
        ]
        header = color_msg(
            "Particle info -->",
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

    # ------------------------------------------------------------------
    # Equality helpers (mirrors old Particle.__eq__ / __hash__ semantics)
    # ------------------------------------------------------------------
    def __eq__(self, other) -> bool:
        if not isinstance(other, ParticleRecord):
            return NotImplemented
        return self.fields == other.fields and all(
            self[f] == other[f] for f in self.fields
        )

    def __hash__(self) -> int:  # type: ignore[override]
        try:
            return hash(tuple((f, self[f]) for f in self.fields))
        except TypeError:
            return id(self)

    def to_dict(self) -> dict:
        """Return a plain-Python dict of all field values.

        Values are scalars (int/float/bool) converted from NumPy where necessary.
        Useful for building DataFrames::

            pd.DataFrame([p.to_dict() for p in particles])
        """
        result = {}
        for f in self.fields:
            v = self[f]
            try:
                result[f] = v.item()  # numpy scalar → Python scalar
            except AttributeError:
                result[f] = v
        return result


class ParticleArray(ak.Array):
    """Collection of particle records.

    Dispatched automatically by Awkward when the outer array's ``__record__``
    resolves to ``"*Particle"`` via the behavior dict.

    Equivalent to the old ``list[Particle]`` stored in ``Event._particles``.
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
