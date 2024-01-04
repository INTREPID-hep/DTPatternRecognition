import math

class shower(object):
    def __init__(self, ev, iShower):
        self.index = iShower
        self.wh = ev.ph2Shower_wheel[iShower]
        self.sc = ev.ph2Shower_sector[iShower]
        self.st = ev.ph2Shower_station[iShower]
        self.BX = ev.ph2Shower_BX[iShower]
        self.nDigis = ev.ph2Shower_ndigis[iShower]
        self.avg_pos = ev.ph2Shower_avg_pos[iShower]
        self.avg_time = ev.ph2Shower_avg_time[iShower]
        return
