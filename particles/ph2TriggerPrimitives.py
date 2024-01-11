import math

class ph2tpg(object):
    def __init__(self, ev, itp):
        self.index = itp
        self.wh = ev.ph2TpgPhiEmuAm_wheel[itp]
        self.sc = ev.ph2TpgPhiEmuAm_sector[itp]
        self.st = ev.ph2TpgPhiEmuAm_station[itp]
        self.phi = ev.ph2TpgPhiEmuAm_phi[itp]
        self.BX = ev.ph2TpgPhiEmuAm_BX[itp] - 20 # Correct to center BX at 0
        self.quality = ev.ph2TpgPhiEmuAm_quality[itp]
        self.rpcFlag = ev.ph2TpgPhiEmuAm_rpcFlag[itp]
        
        # Constants
        self.phires_conv =  65536. / 0.5
        self.matches = []
        self.matches_with_segment = False
        return


            