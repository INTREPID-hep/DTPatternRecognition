''' Script to make concentrator studies '''
import os
from optparse import OptionParser
import ROOT as r
import numpy as np

from utils.ntuple_reader import ntuple
from utils.functions import color_msg

def addConcentratorOptions(pr):
  pr.add_option('--inpath', '-i', type="string", dest = "inpath", default = "./results/")
  
  # Additional
  pr.add_option("--outfolder", "-o", type="string", dest = "outfolder", default = "./results")
  pr.add_option('--maxfiles', type=int, dest = "maxfiles", default = -1)
  pr.add_option('--maxevents', type=int, dest = "maxevents", default = -1)

  return

if __name__ == "__main__":
  pr = OptionParser(usage="%prog [options]")
  addConcentratorOptions(pr)
  (options, args) = pr.parse_args()
  inpath = options.inpath
  outfolder = options.outfolder
  maxfiles = options.maxfiles
  maxevents = options.maxevents
  
  
  color_msg("SHOWER PERFORMANCE ANALYZER", "green")
  ntuplizer = ntuple(inpath, outfolder = outfolder, maxevents = maxevents, maxfiles = maxfiles)
  ntuplizer.run()