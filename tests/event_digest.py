"""Deterministic digest of built events.

Shared by ``gen_event_snapshot.py`` (writes the fixture) and
``test_event_equivalence.py`` (compares against it). The digest is a sha1 over
a canonical JSON rendering, so any change in a particle count, an attribute
value or the particle ordering shows up as a different hash - and the path of
the difference (event, collection, attribute) is reported by the test.
"""

import hashlib
import json
import os

from dtpr.base.particle import Particle

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
TEST_FILE = os.path.join(
    "tests", "ntuples", "DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root"
)
DEFAULT_CONFIG = os.path.join("dtpr", "utils", "yamls", "run_config.yaml")
ROUND_DIGITS = 12
DEFAULT_ENTRIES = list(range(10))


def canonical(value):
    """Render a built attribute value as a stable JSON string.

    Floats are rounded to ``ROUND_DIGITS`` decimals, so a last-bit change in the
    conversion from a ROOT float32 does not count as a physics change.
    """
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        if value != value:
            return '"nan"'
        return repr(round(value, ROUND_DIGITS))
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, (bytes, bytearray)):
        return '"b:' + bytes(value).hex() + '"'
    if isinstance(value, Particle):
        return "particle:%s:%s" % (value.name, value.index)
    if isinstance(value, dict):
        return "{" + ",".join(
            "%s:%s" % (json.dumps(str(key)), canonical(value[key])) for key in sorted(value)
        ) + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(canonical(item) for item in value) + "]"
    if hasattr(value, "item"):
        return canonical(value.item())
    if hasattr(value, "__iter__"):
        return canonical(list(value))
    return "repr:" + repr(value)


def digest(value):
    """sha1 of the canonical rendering, short enough to keep the fixture readable."""
    return hashlib.sha1(canonical(value).encode("utf-8")).hexdigest()


def digest_event(event):
    """Digest one built event: particle counts plus every attribute of every collection."""
    collections = {}
    for ptype, particles in event._particles.items():
        attributes = {}
        if particles:
            keys = sorted({key for particle in particles for key in particle.__dict__ if not key.startswith("_")})
            for key in keys:
                attributes[key] = digest([getattr(particle, key, None) for particle in particles])
        collections[ptype] = {"count": len(particles), "attributes": attributes}

    event_attributes = {
        key: digest(value)
        for key, value in sorted(event.__dict__.items())
        if not key.startswith("_") and key not in ("index", "number")
    }

    return {
        "index": event.index,
        "number": event.number,
        "attributes": event_attributes,
        "collections": collections,
    }


def snapshot(ntuple, entries=None):
    """Build the given entries and return ``{"meta": ..., "events": [...]}``."""
    entries = DEFAULT_ENTRIES if entries is None else list(entries)
    events = []
    for index in entries:
        event = ntuple.events[index]
        if event is None:
            events.append({"index": index, "skipped_by_selector": True})
        else:
            events.append(digest_event(event))
    return {
        "meta": {
            "input": TEST_FILE,
            "config": DEFAULT_CONFIG,
            "entries": entries,
            "round_digits": ROUND_DIGITS,
            "generator": "tests/gen_event_snapshot.py",
        },
        "events": events,
    }
