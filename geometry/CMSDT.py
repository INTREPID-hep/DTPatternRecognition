from geometry.MBstation import *
from geometry.Layer import *
from geometry.DriftCell import *
from particle_objects.Primitive import *
from particle_objects.Pattern import *
from particle_objects.Muon import *


def CMSDT(wheel, sector, station):
    globalDTheight = 1.3
    SLgap     = 28.7 - globalDTheight*8 # originally, it was 29 - globalDTheight*8  ????
    nDTMB1 = 80
    nDTMB2 = 59
    nDTMB3 = 73
    nDTMB4 = 102
    
    # == These are used to generate muons
    MB1     = MBstation(wheel = wheel, sector = sector, nDTs = nDTMB1, MBtype="MB1", gap = SLgap, SLShift = 0.5, additional_cells = 0)
    MB2     = MBstation(wheel = wheel, sector = sector, nDTs = nDTMB2, MBtype="MB2", gap = SLgap, SLShift = 1.0, additional_cells = 0)
    MB3     = MBstation(wheel = wheel, sector = sector, nDTs = nDTMB3, MBtype="MB3", gap = SLgap, SLShift = 0.0, additional_cells = 0)
    MB4     = MBstation(wheel = wheel, sector = sector, nDTs = nDTMB4, MBtype="MB4", gap = SLgap, SLShift = 2.0, additional_cells = 0)

    # == These are used to check if the generated muon falls in the contiguous chamber -- used for PseudoBayes training, commented for now
#    MB1f    = MBstation(wheel = wheel, sector = sector, nDTs = nDTMB1, MBtype="MB1", gap = SLgap, SLShift = 0.5, additional_cells = 30)
#    MB2f    = MBstation(wheel = wheel, sector = sector, nDTs = nDTMB2, MBtype="MB2", gap = SLgap, SLShift = 1.0, additional_cells = 30)
#    MB3f    = MBstation(wheel = wheel, sector = sector, nDTs = nDTMB3, MBtype="MB3", gap = SLgap, SLShift = 0.0, additional_cells = 30)
#    MB4f    = MBstation(wheel = wheel, sector = sector, nDTs = nDTMB4, MBtype="MB4", gap = SLgap, SLShift = 2.0, additional_cells = 30)
    
    
    stations = {1 : MB1, 
                2 : MB2,
                3 : MB3,
                4 : MB4,}

    return stations[station]
