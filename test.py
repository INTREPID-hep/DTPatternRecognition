from dtpr.base.config import RUN_CONFIG
from dtpr.base import NTuple
import awkward as ak
from pprint import pprint
from dtpr.utils.io import dump_to_root
import ROOT as r
import uproot as ur
import numpy as np

def main():
    file = ur.open("test_output.root")

    events = file["DTPR/TREE"]
    
    print(events.show())

    print(list(events["tps_quality"]))
    print(list(events["fwshowers_matched_tps_ids"]))

    # infile = "tests/ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root"
    # RUN_CONFIG.change_config_file("dtpr/utils/yamls/run_config.yaml")  # use test config with simple pipeline
    # ntuple = NTuple(infile, CONFIG=RUN_CONFIG)

    # events = ntuple.events.compute()  # materialise first — matching requires eager data

    # # Cross-join every (shower, tp) pair per event, grouped BY shower (nested=True).
    # # Result shape:  events * showers * tps * {shower: record, tp: record}
    # pairs = ak.cartesian(
    #     {"shower": events["fwshowers"], "tp": events["tps"]},
    #     nested=True,
    # )

    # # Matching condition: same DT chamber + quality cut
    # match = (
    #     (pairs["shower"]["wh"] == pairs["tp"]["wh"])
    #     & (pairs["shower"]["sc"] == pairs["tp"]["sc"])
    #     & (pairs["shower"]["st"] == pairs["tp"]["st"])
    #     & (pairs["tp"]["quality"] > 6)
    # )
    # # For each shower, collect the TPs that pass the match.
    # # Shape: events * showers * var * tp_record  (0 or more TPs per shower)
    # matched_tps = pairs["tp"][match]

    # events["fwshowers"] = ak.with_field(events["fwshowers"], matched_tps, "matched_tps")
    # dump_to_root(events, "test_output.root")

if __name__ == "__main__":
    main()