from geometry.station import station 
from utils.functions import color_msg
import sys

def dt(wh, sc, st):
        """ This method creates CMS DT station objects """
        globalDTheight = 1.3
        SLgap         = 28.7 - globalDTheight*8 

        nDTMB1 = 47
        nDTMB2 = 60
        nDTMB3 = 73
        nDTMB4 = 102
        
        if st == 1:
            return station(wheel = wh, sector = sc, nDTs = nDTMB1, MBtype="MB1", gap = SLgap, SLShift = 0.5, additional_cells = 0)
        elif st == 2:
            return station(wheel = wh, sector = sc, nDTs = nDTMB2, MBtype="MB2", gap = SLgap, SLShift = 1.0, additional_cells = 0)
        elif st == 3:
            return station(wheel = wh, sector = sc, nDTs = nDTMB3, MBtype="MB3", gap = SLgap, SLShift = 0.0, additional_cells = 0)
        elif st == 4:
            return station(wheel = wh, sector = sc, nDTs = nDTMB4, MBtype="MB4", gap = SLgap, SLShift = 2.0, additional_cells = 0)
        else:
            color_msg(f"Wrong station {st}", "red", indentLeveL = 0)
            sys.exit()
        