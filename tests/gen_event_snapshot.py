#!/usr/bin/env python3
"""Regenerate ``tests/fixtures/event_snapshot.json`` from the current library code.

Run this only when a physics change is intended: the fixture is the reference
that ``test_event_equivalence.py`` checks every reader optimisation against.

    python tests/gen_event_snapshot.py
"""

import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(HERE, ".."))
for path in (ROOT_DIR, HERE):
    if path not in sys.path:
        sys.path.insert(0, path)

from event_digest import DEFAULT_ENTRIES, TEST_FILE, snapshot  # noqa: E402

OUTPUT = os.path.join(HERE, "fixtures", "event_snapshot.json")


def main():
    from dtpr.base import NTuple

    ntuple = NTuple(os.path.join(ROOT_DIR, TEST_FILE))
    data = snapshot(ntuple, DEFAULT_ENTRIES)
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w") as handle:
        json.dump(data, handle, indent=1, sort_keys=True)
        handle.write("\n")
    print("wrote %s (%d events)" % (OUTPUT, len(data["events"])))
    return 0


if __name__ == "__main__":
    sys.exit(main())
