"""The attribute readers must build the same particles as Particle.__init__.

tests/test_event_equivalence.py proves that the built events did not change.
This one compares, on real entries and for every configured collection, the
particle that Particle(index=i, ev=ev, readers=...) constructs with the one that
Particle(index=i, ev=ev, **attributes) constructs from the same definitions: the
branch, the [flat, counts] pair and the expr forms, plus the cross-reference
bookkeeping. Every particle of the entry is compared, including the ones the
collection filter drops from the event.
"""

import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from event_digest import ROOT_DIR, TEST_FILE, canonical  # noqa: E402

INPUT = os.path.join(ROOT_DIR, TEST_FILE)
ENTRIES = 3


def _readers(attributes):
    from dtpr.base.particle import attribute_reader

    return [(attr, attribute_reader(attr, spec)) for attr, spec in attributes.items()]


def _amount(view, spec):
    amount = spec["amount"]
    if isinstance(amount, int):
        return amount
    value = getattr(view, amount)
    return value if isinstance(value, int) else len(value)


def test_readers_match_particle_init_on_real_entries():
    if not os.path.exists(INPUT):
        pytest.skip("Test ROOT file not found: %s" % INPUT)

    from dtpr.base import NTuple
    from dtpr.base.config import RUN_CONFIG
    from dtpr.base.entry_view import EntryView
    from dtpr.base.particle import Particle

    ntuple = NTuple(INPUT)
    view = EntryView(ntuple.tree)

    for entry in range(ENTRIES):
        view.set_entry(entry)
        for ptype, spec in RUN_CONFIG.particle_types.items():
            attributes = spec.get("attributes", {})
            readers = _readers(attributes)
            for index in range(_amount(view, spec)):
                built = Particle(index=index, ev=view, readers=readers)
                expected = Particle(index=index, ev=view, **attributes)

                assert [key for key in built.__dict__ if key != "name"] == [
                    key for key in expected.__dict__ if key != "name"
                ], "the attributes and their order must match"

                for key in built.__dict__:
                    if key.startswith("_") or key == "name":
                        continue
                    assert canonical(getattr(built, key)) == canonical(getattr(expected, key)), (
                        entry,
                        ptype,
                        index,
                        key,
                    )

                assert built._needs_resolution == expected._needs_resolution
                assert built.name == expected.name
