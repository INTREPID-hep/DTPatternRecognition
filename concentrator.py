''' Script to make concentrator studies '''
import pandas as pd
import os
from optparse import OptionParser
import numpy as np
import glob

# Geometry stuff for plotting 
from geometry.station import station
from geometry.layer import layer
from geometry.cell import cell 


def addConcentratorOptions(pr):
  pr.add_option('--inpath', '-i', type="string", dest = "inpath", default = "./results/")
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

if __name__ == "__main__":

  pr = OptionParser(usage="%prog [options]")
  addConcentratorOptions(pr)
  (options, args) = pr.parse_args()
  inpath = options.inpath 

  print(CMSDT(10, 1)) 
