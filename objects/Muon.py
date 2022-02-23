import numpy as np
import geometry as geom

class Muon(object):
    driftVel = 54e-4 #cm/ns
    
    def __init__(self, x0, y0, m):
        ''' Constructor '''
        # == Set the position and slope of this Trigger Primitive (muon)
        self.set_position(x0, y0)
        self.set_slope(m)

        # == Containers to store hits 
        self.hits = []
        self.lateralities = []
    
        return

    # == Setters and Getters
    def set_position(self, x0, y0):
        ''' Set position of the primitive '''
        self.x0 = x0
        self.y0 = y0
        return
    def set_slope(self, m):
        ''' Set slope of the primitive'''
        self.m = m
        return
    def get_slope(self):
        return self.m

    def get_x_seed(self):            
        ''' Get X position'''
        return self.x0

    def get_y_seed(self):
        ''' Get Y position'''
        return self.y0

    def getY(self, x, ydef):
        ''' This method returns the y position
        of the muon with respect a given X.'''
        m = self.get_slope()
        x0 = self.get_x_seed()
        y0 = self.get_y_seed()

        return m*(x-x0)+y0

    def get_position(self):
        ''' Get both position coordinates '''
        return (self.x0, self.y0)

    def get_semicells(self):
        ''' Return semicells'''
        return self.semicells
    
    def add_hit(self, id_cell):
        self.hits.append(id_cell)
        return

    def add_lat(self, lat):
        self.lateralities.append(lat)
        return
    def get_hits(self):
        return self.hits

    def set_trueTime(self):
        self.trueTimes = [(tdc - self.t0) for tdc in self.tdcs]
        return

    def getPattern(self):
        self.pattern = []
        for i in range(len(self.hits)):
            self.pattern.append([self.hits[i].parent.idy, self.hits[i].idx, self.lateralities[i]])
        return self.pattern

    def getRecoPattern(self):
        self.recopattern = []
        for i in range(len(self.hits)):
            self.recopattern.append([self.hits[i].parent.idy, self.hits[i].idx])
        return self.recopattern
    
    
      
    

          


    
