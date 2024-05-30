from optparse import OptionParser
import os
import pandas as pd
import numpy as np
import ROOT as r
from root_numpy import tree2array
import uproot
from warnings import filterwarnings
import json
filterwarnings(action='ignore', category=DeprecationWarning, message='`np.object` is a deprecated alias')

def add_parsing_opts():
    ''' Function with base parsing arguments used by any script '''
    parser = OptionParser(usage = "python dump_to_json.py [options]",  description = "Main options for dumping rootfile content into json") 
    # -- Input and outputs
    parser.add_option("--inpath",   "-i",  dest = "inpath",  help = "Path to the folder with the rootfiles. ")
    parser.add_option("--maxFiles", "-m",  dest = "maxfiles",  type = int, default = 1, help = "Maximum files to convert")
    return parser


if __name__ == "__main__": 
    opts, args = add_parsing_opts().parse_args()
    inpath = opts.inpath
    maxFiles = opts.maxfiles
    
    # 1. Get the files
    rfiles = [ rfile for rfile in os.listdir( inpath ) ][:maxFiles]

    # 2. Concatenate them: this is somewhat messy, but works 
    df_ttree = pd.DataFrame()

    for rfile in rfiles:
        with uproot.open( os.path.join(inpath, rfile) ) as open_rfile:
            tree = open_rfile["DTSim"].arrays( library = "pandas" )
            df_ttree = pd.concat([df_ttree, tree])
    
    # 3. Now go SL by SL            
    for sl in range(1, 4):
        sl_filtered = df_ttree[ df_ttree["SLHit_SL"] == sl ].to_json( orient = "records", indent = 2 )    
        
        # 3.1 Dump to json
        filename = f"jsonGeant4_sl{sl}.json"
        with open(filename, 'w') as outjson:
            json.dump(sl_filtered, outjson, indent=2)
        output_json = json.dumps(sl_filtered)
