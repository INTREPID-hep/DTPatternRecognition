import numpy as np

class DriftCell(object):
    ''' Class definition of a drift cell '''
    
    # - Attributes
    x = 0
    y = 0 
    height = 0
    width = 0

    def __init__(self, height, width, parent, idx=-1):
        ''' Constructor '''
        self.height = height
        self.width = width
        self.idx = idx
        self.parent = parent
        return

    def get_width(self):
        return self.width
    def get_height(self):
        return self.height

    def set_position_at_min(self, x, y):
        ''' Set the position of the cell within the MB '''
        self.x = x
        self.y = y
        return     

    def get_position_at_min(self):
        ''' Set the position of the cell within the MB '''
        return (self.x, self.y)

    def get_center(self):
        ''' Return the center of the cell '''
        x = self.x+self.width/2.0 
        y = self.y+self.height/2.0
        return x, y

    def sweep_cell(self, xleft, xright, muon):
        ''' Method to sweep through a cell and see if the muon has passed through there '''

        # -- First, check in the whole cell
        xr = np.linspace(xleft, xright, 100)
        x_cell, y_cell = self.get_position_at_min()
        height = self.get_height()
        # -- Generate the line that defines the muon track inside the cell
        y_values = muon.getY(xr, y_cell + height/2.)        

        sweep = []
        for y in y_values:
            # If the y position is within the cell, store True 
            isIn = (y >= y_cell) and y <= (y_cell+height)
            sweep.append(isIn)

        sweep = np.asarray(sweep)

        # If any of the checks is TRUE, then the muon has passed through there
        isMIn = any(sweep)
        return isMIn

    def isIn(self, muon):
        ''' Method to explicitly check if a Muon is inside a cell'''
        semiCellLeft = False
        semiCellRight = False
        
        # -- Get position of the cell
        x_cell, y_cell = self.get_position_at_min()
        width = self.get_width()
        height = self.get_height()
        
        #print("First Checks")
        # Here you don't know the laterality of the muon,
        # so you have to check both

        y_position_left  = muon.getY(x_cell, y_cell + height/2. )         # Get y position at the left of the cell
        y_position_right = muon.getY(x_cell + width, y_cell + height/2.) # Get y position at the right

        # If wether at the left or right side of the cell, the muon
        # has an y position smaller than the y position of the cell
        # within the chamber, then the muon has not passed through
        # the cell
        below_cell = max(y_position_left, y_position_right) < y_cell

        # Same applies for the upper part of the cell
        above_cell = min(y_position_left, y_position_right) > y_cell + height

        if below_cell or above_cell:
            return (False, False, False)

	# If you ara here, then the muon IS inside the CELL

        # -- Check how IN is it
        isMIn_global = self.sweep_cell(x_cell, x_cell + width, muon)
        semiCellLeft = self.sweep_cell(x_cell, x_cell + width/2.0, muon)
        semiCellRight = self.sweep_cell(x_cell+width/2.0, x_cell + width, muon)
       
        
       

        return (isMIn_global, semiCellLeft, semiCellRight)
