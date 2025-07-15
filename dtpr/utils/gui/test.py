from dtpr.base import NTuple
import time

def main():
    ntuple = NTuple(
        inputFolder="../../../test/ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root",
    )

    f = lambda e: e.event_eventNumber
    start_time = time.time()
    a = list(map(f, ntuple.tree))
    print(f"Time taken to process events: {time.time() - start_time:.2f} seconds")
    print(f"Number of events: {len(a)}")

    start_time = time.time()
    nums = [e.event_eventNumber for e in ntuple.tree]
    print(f"Time taken to process events with list comprehension: {time.time() - start_time:.2f} seconds")
    print(f"Number of events: {len(nums)}")

    start_time = time.time()
    nums = [e.number if e else None for e in ntuple.events]
    print(f"Time taken to process events with list comprehension: {time.time() - start_time:.2f} seconds")
    print(f"Number of events: {len(nums)}")

if __name__ == "__main__":
    main()



