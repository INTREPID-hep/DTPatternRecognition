from objects.MBstation import *

globalDTheight = 1.3 # cm
globalDTwidth = 4.2 # cm
SLgap     = 28.7 - globalDTheight*8 # originally, it was 29 - globalDTheight*8  ????

nDTMB1 = 47
nDTMB2 = 59
nDTMB3 = 73
nDTMB4 = 102
wheel = -2
sector = 11

MB1   = MBstation(wheel = wheel, sector = sector, MBtype="MB1", gap = SLgap, SLShift = 0.5, nDriftCells = nDTMB1, additional_cells = 0,  cellHeight = globalDTheight, cellWidth = globalDTwidth)
MB2   = MBstation(wheel = wheel, sector = sector, MBtype="MB2", gap = SLgap, SLShift = 1.0, nDriftCells = nDTMB2, additional_cells = 0,  cellHeight = globalDTheight, cellWidth = globalDTwidth)
MB3   = MBstation(wheel = wheel, sector = sector, MBtype="MB3", gap = SLgap, SLShift = 0.0, nDriftCells = nDTMB3, additional_cells = 0,  cellHeight = globalDTheight, cellWidth = globalDTwidth)
MB4   = MBstation(wheel = wheel, sector = sector, MBtype="MB4", gap = SLgap, SLShift = 2.0, nDriftCells = nDTMB4, additional_cells = 0,  cellHeight = globalDTheight, cellWidth = globalDTwidth)


