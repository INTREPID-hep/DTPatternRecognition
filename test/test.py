from dtpr.utils.config import RUN_CONFIG
from dtpr.base import NTuple
import os

def main():
    config_file_path = os.path.join(os.path.dirname(__file__), "run_config.yaml")
    input_file_path = os.path.join(os.path.dirname(__file__), "ntuples", "DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root")

    RUN_CONFIG.change_config_file(config_file_path)

    ntuple = NTuple(input_file_path)

    for index, event in enumerate(ntuple.events):
        print(event)
        if index == 3:
            break

if __name__ == "__main__":
    main()