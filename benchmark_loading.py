"""
Quick benchmark: new columnar NTuple vs old ROOT.TChain+Event loading.

Usage:
    python benchmark_loading.py [path/to/file.root] [--nevents N]

Measures two things for each approach:
  1. Load time  — how long until you have a handle to all events
  2. Access time — how long to iterate every event and read one particle field
"""

import argparse
import os
import sys
import time
import warnings

warnings.filterwarnings("ignore")

ROOT_FILE = os.path.join(
    os.path.dirname(__file__),
    "tests/ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root",
)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("root_file", nargs="?", default=ROOT_FILE)
    p.add_argument(
        "--nevents", type=int, default=-1,
        help="Max events to iterate in the access benchmark (-1 = all)",
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# NEW approach — coffea NanoEventsFactory + awkward arrays
# ---------------------------------------------------------------------------
def bench_new(root_file, nevents=-1):
    from dtpr.base.ntuple import NTuple

    # --- Load ---
    t0 = time.perf_counter()
    ntuple = NTuple(root_file)
    t_load = time.perf_counter() - t0

    total = len(ntuple.events)

    # --- Access: iterate every event, read digis[0].wh (if any digis) ---
    limit = total if nevents < 0 else min(nevents, total)
    t0 = time.perf_counter()
    hits = 0
    for i in range(limit):
        ev = ntuple.events[i]
        digis = ev["digis"]
        if len(digis) > 0:
            _ = int(digis[0]["wh"])
            hits += 1
    t_access = time.perf_counter() - t0

    return t_load, t_access, total, hits, limit


# ---------------------------------------------------------------------------
# OLD approach — ROOT.TChain + per-event Event objects
# ---------------------------------------------------------------------------
def bench_old(root_file, nevents=-1):
    try:
        import ROOT
    except ImportError:
        print("  [skip] ROOT not available")
        return None, None, None, None, None

    from dtpr.base.old_event import Event

    treepath = "dtNtupleProducer/DTTREE"

    # --- Load: build TChain (accepts file, directory, or glob) ---
    t0 = time.perf_counter()
    chain = ROOT.TChain(treepath)
    if os.path.isdir(root_file):
        for entry in os.scandir(root_file):
            if entry.is_file() and entry.name.endswith(".root"):
                chain.Add(entry.path)
    else:
        chain.Add(root_file)  # single file or glob pattern
    total = chain.GetEntries()
    t_load = time.perf_counter() - t0

    # --- Access: iterate every event, build Event obj, read digis[0].wh ---
    limit = total if nevents < 0 else min(nevents, total)
    t0 = time.perf_counter()
    hits = 0
    for iev, ev in enumerate(chain):
        if iev >= limit:
            break
        event = Event(index=iev, ev=ev, use_config=True)
        digis = getattr(event, "digis", [])
        if digis:
            _ = digis[0].wh
            hits += 1
    t_access = time.perf_counter() - t0

    return t_load, t_access, total, hits, limit


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def fmt(seconds):
    if seconds is None:
        return "  N/A  "
    return f"{seconds:7.3f}s"


def main():
    args = parse_args()
    f = os.path.abspath(args.root_file)
    if not os.path.exists(f):
        sys.exit(f"File not found: {f}")

    print(f"\nBenchmarking on: {os.path.basename(f)}")
    print(f"Max events for access test: {'all' if args.nevents < 0 else args.nevents}\n")
    print(f"{'':20s}  {'load':>8s}  {'access':>8s}  {'events':>8s}  {'hits':>6s}")
    print("-" * 62)

    print("Running NEW (coffea)...", end=" ", flush=True)
    new_load, new_access, new_total, new_hits, new_limit = bench_new(f, args.nevents)
    print("done")

    print("Running OLD (ROOT)...  ", end=" ", flush=True)
    old_load, old_access, old_total, old_hits, old_limit = bench_old(f, args.nevents)
    print("done\n")

    print(f"{'':20s}  {'load':>8s}  {'access':>8s}  {'events':>8s}  {'hits':>6s}")
    print("-" * 62)
    print(f"{'NEW (coffea+awkward)':20s}  {fmt(new_load)}  {fmt(new_access)}  {new_limit:>8d}  {new_hits:>6d}")
    print(f"{'OLD (ROOT.TChain)':20s}  {fmt(old_load)}  {fmt(old_access)}  {str(old_limit or 'N/A'):>8s}  {str(old_hits or 'N/A'):>6s}")

    if old_access and new_access:
        speedup = old_access / new_access
        print(f"\nAccess speedup: {speedup:.1f}x {'faster' if speedup > 1 else 'slower'} (new vs old)")
    if old_load and new_load:
        load_ratio = old_load / new_load
        print(f"Load   speedup: {load_ratio:.1f}x {'faster' if load_ratio > 1 else 'slower'} (new vs old)")


if __name__ == "__main__":
    main()
