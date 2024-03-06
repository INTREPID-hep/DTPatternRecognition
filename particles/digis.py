import math

class digi(object):
    def __init__(self, ev, idigi):
        self.index = idigi
        self.wh = ev.digi_wheel[idigi]
        self.sc = ev.digi_sector[idigi]
        self.st = ev.digi_station[idigi]
        self.sl = int(ev.digi_superLayer[idigi])
        self.w = int(ev.digi_wire[idigi])
        self.l = int(ev.digi_layer[idigi])-1 # Layers start at 1
        self.time = int(ev.digi_time[idigi])
