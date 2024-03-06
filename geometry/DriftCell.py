"""
----------------------------------------------------------
        Class definition of a Drift Cell 
----------------------------------------------------------
parent: layer in which the cell is ensembled
idx   : identifier along the X axis -- from 1 to nDriftCells
          _________________________
         |                         |
         |                         |
  1.3 cm |                         |
         |                         |
         |_________________________|
          <------- 4.2 cm --------->

----------------------------------------------------------
"""
import numpy as np

class DriftCell(object):
    
    x = 0
    y = 0 
    height = 1.3
    width = 4.2

    def __init__(self, parent, idx=-1):
        ''' Constructor '''
        self.idx = idx
        self.parent = parent
        return

    # == Getters ==
    def id(self):
        return self.idx
        
    def get_parent(self):
        return self.parent

    def get_width(self):
        return self.width

    def get_height(self):
        return self.height

    def get_position_at_min(self):
        return (self.x, self.y)

    def get_center(self):
        halfwidth = self.get_width()*0.5
        halfheight = self.get_height()*0.5
        xmin, ymin = self.get_position_at_min()
        return (xmin + halfwidth, ymin + halfheight)
    
    # == Setters ==
    def set_width(self, width):
        self.width = width
        return 
    def set_height(self, height):
        self.height = height
        return 
    def set_position_at_min(self, x, y):
        self.x = x
        self.y = y
        return     
