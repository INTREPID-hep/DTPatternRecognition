import uproot as ur
import awkward as ak
import pandas as pd

def main():
    in_file = ur.open("./g4DTSimNtuple_muonTest.root:DTSim")
    
    in_data = ak.to_dataframe(in_file.arrays(library="ak"))
    in_file.close()

    # Group data by 'EventNo' and aggregate as lists
    grouped_data = in_data.groupby("EventNo").agg(lambda x: x.tolist())

    out_file = ur.recreate("./g4DTSimNtuple_muonTest_refactored.root")
    
    out_file["DTSim"] = grouped_data

    out_file.close()
if __name__ == '__main__':
    main()