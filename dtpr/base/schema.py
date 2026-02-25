"""
PatternSchema — coffea BaseSchema subclass for DT NTuple ROOT files.

**Structural (schema) pass only.**  No actual array data is read here; the
schema operates exclusively on ``base_form`` (the Awkward *description* of
the data).

What it does
------------
For each ``particle_types`` entry in the config it:

1. Collects all attributes that carry a ``branch:`` key (skips ``expr:`` and
   ``src:`` — those are handled later by the enrichment pass).
2. Calls :func:`~coffea.nanoevents.schemas.base.zip_forms` to group the
   individual branch forms into a single **jagged record** named after the
   particle type, with ``__record__ = "Particle"`` so Awkward dispatches to
   :class:`~dtpr.base.particle.ParticleRecord`.
3. Leaves every other branch (event-level scalars, unrecognised names) *flat*
   at the top level.
4. Wraps everything in a top-level ``RecordArray`` with
   ``__record__ = "Event"`` for :class:`~dtpr.base.event.EventRecord`.

Config injection
----------------
Because ``NanoEventsFactory`` calls ``schemaclass(base_form)`` with no extra
arguments, config injection is done via :meth:`PatternSchema.with_config`:

.. code-block:: python

    schema_cls = PatternSchema.with_config(my_config_instance)
    factory = NanoEventsFactory.from_root(files, schemaclass=schema_cls, ...)

The returned class is a proper subclass with the correct ``__dask_capable__``
and ``behavior()`` attributes, so coffea handles it transparently.
"""

from __future__ import annotations

from warnings import warn
from coffea.nanoevents.schemas.base import BaseSchema, zip_forms


class PatternSchema(BaseSchema):
    """Groups flat DT-NTuple branches into nested Awkward particle records.

    Parameters
    ----------
    base_form : dict
        The Awkward form dict produced by uproot/coffea (already pre-filtered
        to the allow-listed branches before this class is instantiated).
    config : Config or None
        A :class:`~dtpr.base.config.Config` instance.  Defaults to the global
        ``RUN_CONFIG`` when ``None``.
    """

    # Inherited from BaseSchema; kept explicit for clarity.
    __dask_capable__ = True

    # Default config — overridden by with_config()
    _config = None

    def __init__(self, base_form: dict, *args, config=None, **kwargs):
        # Resolve config: explicit arg > class-level attr > global RUN_CONFIG
        if config is None:
            config = self.__class__._config
        if config is None:
            from .config import RUN_CONFIG
            config = RUN_CONFIG

        # ------------------------------------------------------------------
        # Build a flat lookup: branch_name → form dict
        # ------------------------------------------------------------------
        branch_forms: dict = dict(
            zip(base_form.get("fields", []), base_form.get("contents", []))
        )

        particle_types: dict = getattr(config, "particle_types", {}) or {}

        consumed_branches: set[str] = set()
        collection_forms: dict[str, dict] = {}

        for ptype, pinfo in particle_types.items():
            if not isinstance(pinfo, dict):
                continue
            attrs: dict = pinfo.get("attributes", {}) or {}

            # Collect attr_name → form for branch-based attributes only
            member_forms: dict[str, dict] = {}
            for attr_name, attr_info in attrs.items():
                if not isinstance(attr_info, dict):
                    # e.g.  matched_genmuons: []  — a default list value
                    continue
                branch = attr_info.get("branch", None)
                if branch is None:
                    # expr: or src: — enrichment pass handles these
                    continue
                if branch not in branch_forms:
                    # Branch absent from this file — skip gracefully
                    warn(f"Branch '{branch}' for {ptype}.{attr_name} not found in schema; skipping.")
                    continue
                member_forms[attr_name] = branch_forms[branch]
                consumed_branches.add(branch)

            if not member_forms:
                # All attributes are computed — nothing structural to zip
                continue

            try:
                coll_form = zip_forms(member_forms, ptype, record_name="Particle")
                collection_forms[ptype] = coll_form
            except (NotImplementedError, ValueError, KeyError):
                # Incompatible form types (shouldn't happen for jagged branches,
                # but be safe); leave the individual branches flat.
                consumed_branches -= set(member_forms.values())  # un-consume

        # ------------------------------------------------------------------
        # Pass-through: every branch not consumed by a particle collection
        # (event_* scalars, environment_*, unrecognised, etc.)
        # ------------------------------------------------------------------
        remaining_forms: dict[str, dict] = {
            name: form
            for name, form in branch_forms.items()
            if name not in consumed_branches
        }

        # ------------------------------------------------------------------
        # Assemble top-level Event RecordArray
        # ------------------------------------------------------------------
        all_fields = list(collection_forms) + list(remaining_forms)
        all_contents = list(collection_forms.values()) + list(remaining_forms.values())

        params: dict = dict(base_form.get("parameters", {}))
        # coffea convention: drop explicit None metadata key
        if params.get("metadata") is None:
            params.pop("metadata", None)
        params.setdefault("metadata", {})
        params["__record__"] = "Event"

        self._form = {
            "class": "RecordArray",
            "fields": all_fields,
            "contents": all_contents,
            "parameters": params,
            "form_key": None,
        }

    # ------------------------------------------------------------------
    # Config injection helper
    # ------------------------------------------------------------------
    @classmethod
    def with_config(cls, config) -> type:
        """Return a subclass of *PatternSchema* pre-bound to *config*.

        Use this when creating a schema for a specific :class:`~dtpr.base.ntuple.NTuple`
        instance that may carry a non-global config:

        .. code-block:: python

            schema_cls = PatternSchema.with_config(ntuple.CONFIG)
            events = NanoEventsFactory.from_root(..., schemaclass=schema_cls)

        The returned class is a fully valid ``BaseSchema`` subclass — coffea
        will accept it transparently.
        """

        class _BoundSchema(cls):  # type: ignore[valid-type]
            _config = config

            def __init__(self, base_form: dict, *args, **kwargs):
                super().__init__(base_form, *args, config=config, **kwargs)

        _BoundSchema.__name__ = cls.__name__
        _BoundSchema.__qualname__ = f"{cls.__qualname__}[bound]"
        return _BoundSchema

    # ------------------------------------------------------------------
    # Behavior — combine Event + Particle behaviors
    # ------------------------------------------------------------------
    @classmethod
    def behavior(cls) -> dict:
        """Return the combined Event + Particle behavior dict."""
        from .event import behavior  # avoids circular import at module load time
        return behavior
