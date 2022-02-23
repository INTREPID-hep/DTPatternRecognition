from objects.DriftCell import DriftCell

class Layer(object):    
    ''' Implementation of a Layer object '''
    def __init__(self, nDriftCells, additional_cells, cellWidth, cellHeight, idy):
        ''' Constructor '''
        self.nDriftCells = nDriftCells
        self.DriftCells = []
        self.additional_cells = additional_cells
        self.cellWidth = cellWidth
        self.cellHeight = cellHeight
        self.idy = idy
        self.create_layer()
        return

    def create_layer(self):
        ''' Build the layer '''
        for cell in range(self.nDriftCells):
            unit = DriftCell(self.cellHeight, self.cellWidth, self, idx = cell - self.additional_cells) # Create a unit
            unit.set_position_at_min(self.cellWidth*cell, 0) # Place at a position
            self.DriftCells.append(unit) # Add to list
        return

    def get_cells(self):
        ''' Method to return the whole set of cells inside a layer '''
        return self.DriftCells

    def get_cell(self, cell_id):
        ''' Get the cell from the list of cells that make up the layer '''
        cells = self.get_cells()
        return cells[cell_id]


    def shift_layer(self, shiftx, shifty):
        ''' Method to shift layers inside the DT Chamber '''
        # == We basically go layer by layer changing the position
        #    of the cells that compose the layer.
        cells = self.get_cells()
        for cell in cells:
            x = cell.x
            y = cell.y
            w = cell.width
            h = cell.height
            # -- Call the method to change local position of cells (defined in DriftCells.py)
            cell.set_position_at_min(x+shiftx*w, y+shifty*h) 
        return
