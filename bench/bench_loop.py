#!/usr/bin/env python3
"""Micro-benchmark for the dtpr event loop.

Builds events through ``NTuple.events`` - the exact path that ``fill-histos``,
``dump-events`` and every user script take - and reports the per-event cost
together with the mean particle counts, so a speed change and a physics change
are visible in the same run.

    python bench/bench_loop.py                 # first 60 events of the test file
    python bench/bench_loop.py -n 0            # the whole file (slow)
    python bench/bench_loop.py -n 0 --json     # machine-readable summary
"""

import argparse
import json
import os
import statistics
import sys
import time

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

DEFAULT_INPUT = os.path.join(
    ROOT_DIR,
    "tests",
    "ntuples",
    "DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root",
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Benchmark the dtpr event loop.")
    parser.add_argument(
        "-i",
        "--input",
        default=DEFAULT_INPUT,
        help="Input ROOT file, list of files or folder. Defaults to the test ntuple.",
    )
    parser.add_argument(
        "-n",
        "--count",
        type=int,
        default=60,
        help="Number of events to build. 0 builds every event. Default: 60.",
    )
    parser.add_argument(
        "--first",
        type=int,
        default=0,
        help="Index of the first event to build. Default: 0.",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=0,
        help="Events to build and discard before measuring. Default: 0.",
    )
    parser.add_argument(
        "-cf",
        "--config-file",
        default=None,
        help="Configuration file to use instead of the packaged run_config.yaml.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print a single JSON object instead of the human-readable report.",
    )
    return parser.parse_args(argv)


def percentile(values, fraction):
    """Nearest-rank percentile of an already sorted list."""
    if not values:
        return 0.0
    index = min(len(values) - 1, max(0, int(round(fraction * (len(values) - 1)))))
    return values[index]


def main(argv=None):
    args = parse_args(argv)

    # Imported here so that --help does not pay for the ROOT import.
    from dtpr.base import NTuple
    from dtpr.base.config import RUN_CONFIG

    if args.config_file:
        RUN_CONFIG.change_config_file(config_path=args.config_file)

    started = time.perf_counter()
    ntuple = NTuple(args.input)
    setup_s = time.perf_counter() - started

    entries = len(ntuple.events)
    first = max(0, min(args.first, entries))
    last = entries if args.count <= 0 else min(entries, first + args.count)

    for _ in range(max(0, args.warmup)):
        try:
            ntuple.events[first]
        except IndexError:
            break

    times = []
    counts = {}
    skipped = 0
    for index in range(first, last):
        begin = time.perf_counter()
        event = ntuple.events[index]
        times.append(time.perf_counter() - begin)
        if event is None:
            skipped += 1
            continue
        for ptype, particles in event._particles.items():
            counts[ptype] = counts.get(ptype, 0) + len(particles)

    built = len(times)
    total_s = sum(times)
    ordered = sorted(times)
    slowest = max(range(built), key=times.__getitem__) + first if built else None
    per_event = {ptype: counts[ptype] / built for ptype in counts} if built else {}

    summary = {
        "input": args.input,
        "config": str(getattr(RUN_CONFIG, "path", "")),
        "entries": entries,
        "first": first,
        "built": built,
        "skipped_by_selector": skipped,
        "setup_s": setup_s,
        "total_s": total_s,
        "ms_per_event": {
            "mean": 1e3 * total_s / built if built else 0.0,
            "median": 1e3 * statistics.median(times) if built else 0.0,
            "p95": 1e3 * percentile(ordered, 0.95),
            "max": 1e3 * times[slowest - first] if built else 0.0,
            "max_index": slowest,
        },
        "per_event_counts": per_event,
    }

    if args.json:
        print(json.dumps(summary))
        return 0

    ms = summary["ms_per_event"]
    print(f"input      : {summary['input']}")
    print(f"config     : {summary['config']}")
    print(f"entries    : {entries}")
    print(f"built      : {built} events (first={first}, count={args.count})")
    if skipped:
        print(f"rejected   : {skipped} events (selector returned None)")
    print(f"wall       : {total_s:.3f} s (setup {setup_s:.3f} s)")
    print(
        f"ms/event   : mean {ms['mean']:.2f}  median {ms['median']:.2f}  "
        f"p95 {ms['p95']:.2f}  max {ms['max']:.2f} (#{ms['max_index']})"
    )
    if per_event:
        counts_str = "  ".join(f"{ptype} {value:.1f}" for ptype, value in per_event.items())
        print(f"per event  : {counts_str}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
