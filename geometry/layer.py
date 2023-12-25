"""
----------------------------------------------------------
        Class definition of a Drift Layer
----------------------------------------------------------
nDriftCells      : Number of cells that make up the layer
idy              : identifier along the Y axis from 0 to 7.
additional_cells : Number of cells added to check if generated
                   muons lie within the next chamber.

idy = 3    | 1 | 2 |   |   |... 
idy = 2      | 1 | 2 |   |   |... 
idy = 1    | 1 | 2 |   |   |... 
idy = 0      | 1 | 2 |   |...
             
----------------------------------------------------------
"""

from geometry.DriftCell import DriftCell

class layer(object):    
    """ Class definition of a layer """
    
    def __init__(self, nDriftCells, idy, additional_cells = 0):
        """ Constructor """
        self.nDriftCells = nDriftCells
        self.DriftCells = []
        self.additional_cells = additional_cells
        self.idy = idy
        self.create_layer()
        return

    # == Getters ==
    def id(self):
        return self.idy

    def get_ncells(self):
        nominal = self.nDriftCells
        additional = self.additional_cells 
        total = nominal+additional
        return (nominal, additional, total)

    def get_cell(self, cell_id):
        """ Method to get a cell from the list of cells """
        cell = self.get_cells()[cell_id-1]
        # This can be vastly improved with lambda functions as callbacks...
        return cell

    def get_cells(self):
        return self.DriftCells

    def add_cell(self, cell):
        """ Add a new cell to the layer """
        self.DriftCells.append(cell)
        return

    def create_layer(self):
        """ Method to ensemble layer """
        ncells_nom, ncells_add, ncells_tot = self.get_ncells()
        
        for cell in range(ncells_nom):
            idx = cell - ncells_add
            unit = DriftCell(self, idx = idx) 
            w = unit.get_width()
            unit.set_position_at_min(cell*w, 0) 
            self.add_cell(unit) 
        return
            
    def shift_layer(self, shiftx, shifty):
        """ Method to shift layers inside a DT chamber """
        cells = self.get_cells()
        for cell in cells:
            x, y = cell.get_position_at_min()
            w = cell.get_width()
            h = cell.get_height()
            cell.set_position_at_min(x + shiftx*w, y + shifty*h)
        return
