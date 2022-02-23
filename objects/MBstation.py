from objects.Layer import *

class MBstation(object):
    nLayers = 8        
    
    
    """ 
         ::    Depending on the wheel the sign changes 
             -half width of a Drift cell  # mm for MB1 
              full width of a Drift cell  # mm for MB2
              0                           # mm for MB3
              twice width of a Drift cell # mm for MB4
             ----> This is a given as a parameter to 
                   the constructor. Just posting here for
                   bookeeping
         :: Positive sign: SL1 displaced to the left of SL 3  
            (low cells of SL3 correlate with high cells of SL1)
        
         :: Negative sign: SL1 displaced to the right of SL3 
            (low cells of SL1 correlate with high cells of SL3)
        
         :: MB1 is negative in the positive wheels (and positive sectors of Wh0)
         :: MB2 is positive in the positive wheels (and positive sectors of Wh0)
         :: MB3 has always 0 SL shift
         :: MB4 is a mess"""

    shift_signs = {"Wh<0": {"MB1" : [+1, +1, +1,     +1, +1, +1, +1, +1, +1, +1,      +1, +1],
                            "MB2" : [-1, -1, -1,     -1, -1, -1, -1, -1, -1, -1,      -1, -1],
                            "MB3" : [ 0,  0,  0,      0,  0,  0,  0,  0,  0,  0,       0,  0],
                            "MB4" : [-1, -1, -1, (0, 0), +1, +1, +1, +1,  0, (-1, +1), 0, -1]},
                   "Wh=0": {"MB1" : [+1, -1, -1,     +1, +1, -1, -1, +1, +1, -1,      -1, +1],
                            "MB2" : [-1, +1, +1,     -1, -1, +1, +1, -1, -1, +1,      +1, -1],
                            "MB3" : [ 0,  0,  0,      0,  0,  0,  0,  0,  0,  0,       0,  0],
                            "MB4" : [-1, +1, +1, (0, 0), +1, -1, -1, +1,  0, (+1, -1), 0, -1]},
                   "Wh>0": {"MB1" : [-1, -1, -1,     -1, -1, -1, -1, -1, -1, -1,      -1, -1],
                            "MB2" : [+1, +1, +1,     +1, +1, +1, +1, +1, +1, +1,      +1, +1],
                            "MB3" : [ 0,  0,  0,      0,  0,  0,  0,  0,  0,  0,       0,  0],
                            "MB4" : [+1, +1, +1, (0, 0), -1, -1, -1, -1,  0, (+1, -1), 0, +1]},         

                  }
    def __init__(self, wheel, sector, MBtype, gap, SLShift, nDriftCells, additional_cells, cellWidth, cellHeight): 
        ''' Constructor '''
        self.Layers = []
    
        # == Chamber related parameters
        self.set_wheel(wheel) 
        self.set_sector(sector)
        self.set_MBtype(MBtype)

        # == set_(Layer related parameters
        self.set_SLShift(SLShift) 
        self.set_SL_shift_sign()
        
        self.set_nDriftCells(nDriftCells)
        self.set_gap(gap)   
        self.set_additional_cells(additional_cells) 

        # == set_(Cell related parameters
        self.set_cellWidth(cellWidth) 
        self.set_cellHeight(cellHeight)

        # == set_(Build the station
        self.build_station(additional_cells)

        # == Initialize a muon container
        self.muons = []
        return

    def add_muon(self, muon):    
        self.muons.append(muon)
        return

    def get_muons(self):
        return self.muons

    def set_additional_cells(self, additional_cells):
        self.additional_cells = additional_cells
        return
    def get_nLayers(self):
        ''' Return the number of Layers in this chamber '''
        return self.nLayers

    def get_layers(self):
        ''' Method to return the object in which Layers are stored '''
        return self.Layers
    def get_layer(self, layer_id):
        ''' Get a layer from its ID list of layers '''
        layers = self.get_layers()
        return layers[layer_id]

    def add_layer(self, layer):
        ''' Method to add a new layer '''
        layers = self.get_layers()
        layers.append(layer)
        return

    def set_nDriftCells(self, nDriftCells):
        ''' Set the number of Drift Cells for this MB station '''
        self.nDriftCells = nDriftCells
        return         
    def get_nDriftCells(self):
        ''' Return the number of DT cells per layer '''
        return self.nDriftCells
    
    def set_cellWidth(self, cellWidth):
        ''' Set the width of a cell '''
        self.cellWidth = cellWidth
        return
    def get_cellWidth(self):
        ''' Get the width of a cell '''
        return self.cellWidth

    def set_cellHeight(self, cellHeight):
        ''' Set the height of a cell '''
        self.cellHeight = cellHeight
        return
    def get_cellHeight(self):
        ''' Set the height of a cell'''
        return self.cellHeight

    def set_wheel(self, wheel):
        ''' Set the wheel to which this station belongs '''
        self.wheel = wheel
        return
    def get_wheel(self):
        ''' Get the wheel to which this station belongs '''
        return self.wheel

    def set_sector(self, sector):
        ''' Set the sector to which this station belongs '''
        self.sector = sector
        return 
    def get_sector(self):
        ''' Get the sector to which this station belongs '''
        return self.sector

    def set_MBtype(self, MBtype):
        ''' Set the type of station '''
        self.MBtype = MBtype
        return 

    def get_MBtype(self):
        ''' Get the type of station '''
        return self.MBtype

    def get_shift_signs(self):
        ''' Get the shift sign for SLs'''
        return self.shift_signs

    def set_SL_shift_sign(self):
        ''' Get the correct sign in the x-shift between SLs'''
        wheel = self.get_wheel()
        # -- This -1 is to adapt to python list indexing
        sector = self.get_sector()-1 
        station = self.get_MBtype()

        entryname = "Wh"
        if wheel > 0: 
            entryname +=">0"
        elif wheel < 0:
            entryname += "<0"
        else:
            entryname += "0"
        
        shift_signs = self.get_shift_signs()
        sign  = shift_signs[entryname][station][sector]
        self.shift_sign = sign
        return 

    def get_SL_shift_sign(self):
        ''' Get the SL shift sign '''
        return self.shift_sign

    def set_SLShift(self, shift):
        ''' Set the shift (in units of DriftCell heights) between SLs '''
        self.SLShift = shift
        return

    def get_SLshift(self):
        ''' Get the shift (in units of DriftCell heights) between SLs '''
        return self.SLShift

    def set_gap(self, gap):
        ''' Set the gap space between SLs'''
        self.gap = gap
        return

    def get_gap(self):
        ''' Get the gap space between SLs'''
        return self.gap

    def build_station(self, adc):
        ''' Method to build up the station '''
        # == First: Generate 8 generic layers
        nLayers = self.get_nLayers()
        for layer in range(nLayers):
            # -- Create a Layer object
            nDriftCells = self.get_nDriftCells()
            cellWidth   = self.get_cellWidth()
            cellHeight  = self.get_cellHeight()

            # -- Create a Layer object (defined in Layers.py)
            new_layer = Layer(nDriftCells, adc, cellWidth, cellHeight, layer+1)
            self.add_layer(new_layer)
        
        # == Second: Place them at the correct position within the chamber
        shift_sign = self.get_SL_shift_sign()
        SLshift    = self.get_SLshift()
        shift      = shift_sign*SLshift
        gap        = self.get_gap()
        space_SL   = gap/cellHeight 

        # -- Shifts are done in units of drift cell width and height
        self.get_layer(0).shift_layer(-adc          , 0)
        self.get_layer(1).shift_layer(-adc-0.5      , 1)
        self.get_layer(2).shift_layer(-adc          , 2)
        self.get_layer(3).shift_layer(-adc-0.5      , 3)
        self.get_layer(4).shift_layer(-adc+shift    , space_SL+4)
        self.get_layer(5).shift_layer(-adc-0.5+shift, space_SL+5)
        self.get_layer(6).shift_layer(-adc+shift    , space_SL+6)
        self.get_layer(7).shift_layer(-adc-0.5+shift, space_SL+7)

        self.set_center()
        return

    def set_center(self):
        ''' Set the geometric center of a DT station'''
        # == Definition of the center of the chamber:
        # -------------------- IMPORTANT ------------------------
        # One has to take into account that the middle is not given by 
        # SL1, SL3, but rather it is define also taking into account SL2!!!!
        # This is way this is not GAP/2.0, that would not be taking into 
        # account SL2
        # -------------------- IMPORTANT ------------------------
        # The center in the Y axis varies 1.8 cm for MB3 and MB4
        # because there is no RPC there


        centery = 11.75 - 1.8*(self.MBtype in ["MB3", "MB4"])
        

        len_layer = self.nDriftCells*self.cellWidth

        centerx = (len_layer+self.cellWidth)/2.0
        

        self.center = (centerx, centery)
        return

    def get_center(self):
        return self.center
  
    def check_in(self, muon):
        ''' Method to check if a generated muon falls inside a chamber '''
        layers = self.get_layers()
        for layer in layers:
            cells = layer.get_cells()
            for cell in cells:
	            # -- Check if this muon passes through this cell
                isMIn_global, semiCellLeft, semiCellRight = cell.isIn(muon)
                if isMIn_global:
                    muon.add_hit(cell) 
                    if semiCellLeft and semiCellRight: muon.add_lat(0)
                    if semiCellLeft:  muon.add_lat(-1.)
                    if semiCellRight: muon.add_lat(1.)
        return
