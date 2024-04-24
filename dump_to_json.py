import pandas as pd
from copy import deepcopy
import numpy as np
import ROOT as r
from root_numpy import tree2array
from warnings import filterwarnings
import json
import sys
import os

filterwarnings(action='ignore', category=DeprecationWarning, message='`np.object` is a deprecated alias')

path = "/eos/cms/store/user/folguera/INTREPID/DTShowers/2024_04_23/muon"


def read_files(path):
    # Make a tchain:
    list_files = [file for file in os.listdir(path) if "root" in file and "DTSimNtuple" in file]
   
    dfs = [] 
    for ifile in list_files[:1]:
        print("Opening: ", path + "/" + ifile)
        tfile = r.TFile.Open( path + "/" + ifile )
        ttree = deepcopy(tfile.Get("DTSim"))
        arr = tree2array(ttree)
        dfs = dfs.append( pd.DataFrame(arr) )
        tfile.Close()
    super_df = pd.concat(dfs, axis = 0, ignore_index = True)
    return super_df 

# -- Convertimos a dataframe
df = read_files(path)
sys.exit()
# Assuming your DataFrame is named df

# Filter and create JSON for each SLHit_SL value
for sl in range(1, 4):  # SLHit_SL values range from 1 to 4
    print(" Getting sl", sl)
    sl_filtered = df[df['SLHit_SL'] == sl]
    
    out = []

    for index, row in sl_filtered.iterrows():
        out.append({
            "event_eventNumber": row['EventNo'],
            "digi_wheel": 0,
            "digi_sector": 12,
            "digi_station": 1,
            "digi_superLayer": row['SLHit_SL'],
            "digi_layer": row['SLHit_Layer'],
            "digi_wire": row['SLHit_Cell'],
            "digi_time": row['SLHit_Time'],
            "digi_PDG": row['SLHit_PDG'],
        }
       )

    filename = "jsonGeant4_sl{sl}.json".format(sl = sl)
    with open(filename, 'w') as f:
        json.dump(out, f, indent=4)
    output_json = json.dumps(out, indent = 2)
    print("JSON for SLHit_SL {sl}:".format(sl = sl))
    print(output_json)
    print("\n")

