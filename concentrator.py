''' Script to make concentrator studies '''
import os
from optparse import OptionParser
import ROOT as r

# Geometry stuff for plotting 
from geometry.station import station 

def addConcentratorOptions(pr):
  pr.add_option('--inpath', '-i', type="string", dest = "inpath", default = "./results/")
  pr.add_option('--mode', '-m', type="string", dest = "mode", default = "ntuple")
  return

def color_msg(msg, color = "none", indentLevel = 0):
    """ Prints a message with ANSI coding so it can be printout with colors """
    codes = {
        "none" : "0m",
        "green" : "1;32m",
        "red" : "1;31m",
        "blue" : "1;34m",
        "yellow" : "1;35m"
    }

    indentStr = ""
    if indentLevel == 0: indentStr = ">>"
    if indentLevel == 1: indentStr = "+"
    if indentLevel == 2: indentStr = "*"
    
    print("\033[%s%s %s \033[0m"%(codes[color], "  "*indentLevel + indentStr, msg))
    return

def CMSDT(wheel, sector):
    """
    This is the definition of the CMS DT geometry
    """ 

    globalDTheight = 1.3
    SLgap     = 28.7 - globalDTheight*8 # originally, it was 29 - globalDTheight*8  ????
    nDTMB1 = 80
    nDTMB2 = 59
    nDTMB3 = 73
    nDTMB4 = 102
    
    # == These are used to generate muons
    MB1     = station(wheel = wheel, sector = sector, nDTs = nDTMB1, MBtype="MB1", gap = SLgap, SLShift = 0.5, additional_cells = 0)
    MB2     = station(wheel = wheel, sector = sector, nDTs = nDTMB2, MBtype="MB2", gap = SLgap, SLShift = 1.0, additional_cells = 0)
    MB3     = station(wheel = wheel, sector = sector, nDTs = nDTMB3, MBtype="MB3", gap = SLgap, SLShift = 0.0, additional_cells = 0)
    MB4     = station(wheel = wheel, sector = sector, nDTs = nDTMB4, MBtype="MB4", gap = SLgap, SLShift = 2.0, additional_cells = 0)
    
    stations = {1 : MB1, 
                2 : MB2,
                3 : MB3,
                4 : MB4}

    return stations

def get_tree(inpath, maxfiles = 2):
  """ Simple function to retrieve a chain with all the trees to be analyzed """
  tree = None
  if "root" in inpath:
    color_msg(f"Opening input file {inpath}", "blue", 1)
    f = r.TFile.Open(inpath)
    tree = f.Get("dtNtupleProducer/DTTREE")
    f.Close()
  else:
    color_msg(f"Opening input files from {inpath}", "blue", 1)
    tree = r.TChain()
    allFiles = os.listdir(inpath)
    nFiles = min(maxfiles, len(allFiles))
    for iF in range(nFiles):
      if "root" not in allFiles[iF]: continue
      color_msg(f"File {allFiles[iF]} added", indentLevel=2)
      tree.Add( os.path.join(inpath, allFiles[iF]) + "/dtNtupleProducer/DTTREE"  )
  
  return tree

if __name__ == "__main__":

  pr = OptionParser(usage="%prog [options]")
  addConcentratorOptions(pr)
  (options, args) = pr.parse_args()
  inpath = options.inpath 
  mode = options.mode
  maxevents = 100
  color_msg("SHOWER PERFORMANCE ANALYZER", "green")
  
  if mode == "ntuple":
    # 1. Open input files
    tree = get_tree(inpath)
    
    # 2. Iterate over tree entries
    for iev, ev in enumerate(tree):
      if iev > maxevents: break

            
    
  

