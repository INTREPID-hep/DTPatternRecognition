"""
----------------------------------------------------------
    Class definition of a Drift Station
----------------------------------------------------------
wheel, sector  : geometrical position within CMS  
nDTs, MBType   : number of cells for an MB of type MBType
gap        : space between superlayers
SLShift      : shift in X axis between both superlayers
additional_cells : Number of cells added to check if generated
           muons lie within the next chamber.
----------------------------------------------------------
"""
from geometry.layer import *
from utils.functions import color_msg

class station(object):
  nLayers = 8    
  """ 
  +  Depending on the wheel the sign changes 
    -half width of a Drift cell  # mm for MB1 
    full width of a Drift cell  # mm for MB2
    0               # mm for MB3
    twice width of a Drift cell # mm for MB4
    ----> This is a given as a parameter to 
        the constructor. Just posting here for
        bookeeping
  + Positive sign: SL1 displaced to the left of SL 3  
    (low cells of SL3 correlate with high cells of SL1)

  + Negative sign: SL1 displaced to the right of SL3 
    (low cells of SL1 correlate with high cells of SL3)

  + MB1 is negative in the positive wheels (and positive sectors of Wh0)
  + MB2 is positive in the positive wheels (and positive sectors of Wh0)
  + MB3 has always 0 SL shift
  + MB4 is a mess
  """

  shift_signs = {
    "Wh<0": {
      "MB1" : [+1, +1, +1,   +1, +1, +1, +1, +1, +1, +1,    +1, +1],
      "MB2" : [-1, -1, -1,   -1, -1, -1, -1, -1, -1, -1,    -1, -1],
      "MB3" : [ 0,  0,  0,    0,  0,  0,  0,  0,  0,  0,     0,  0],
      "MB4" : [-1, -1, -1, (0, 0), +1, +1, +1, +1,  0, (-1, +1), 0, -1]
    },
    "Wh0": {
      "MB1" : [+1, -1, -1,   +1, +1, -1, -1, +1, +1, -1,    -1, +1],
      "MB2" : [-1, +1, +1,   -1, -1, +1, +1, -1, -1, +1,    +1, -1],
      "MB3" : [ 0,  0,  0,    0,  0,  0,  0,  0,  0,  0,     0,  0],
      "MB4" : [-1, +1, +1, (0, 0), +1, -1, -1, +1,  0, (+1, -1), 0, -1]
    },
    "Wh>0": {
      "MB1" : [-1, -1, -1,   -1, -1, -1, -1, -1, -1, -1,    -1, -1],
      "MB2" : [+1, +1, +1,   +1, +1, +1, +1, +1, +1, +1,    +1, +1],
      "MB3" : [ 0,  0,  0,    0,  0,  0,  0,  0,  0,  0,     0,  0],
      "MB4" : [+1, +1, +1, (0, 0), -1, -1, -1, -1,  0, (+1, -1), 0, +1]
    }     
  }
  
  def __init__(self, wheel, sector, nDTs, MBtype, gap, SLShift, additional_cells): 
    """ Constructor """
    self.Layers = []

    # == Chamber related parameters
    self.wheel = wheel 
    
    if sector == 13:
      self.sector = 4
    elif sector == 14:
      self.sector = 10
    else:
      self.sector = sector
       
    self.MBtype = MBtype

    self.name = f"Wheel {self.wheel} Sector {self.sector} {self.MBtype} "
    # == set_(Layer related parameters
    self.SLShift =  SLShift 
    self.set_SL_shift_sign()
    
    self.nDriftCells = nDTs
    self.gap = gap   
    self.additional_cells = additional_cells 

    # == set_(Build the station
    self.build_station(additional_cells)
    self.clear()
  
  def set_SL_shift_sign(self):
    """ Get the correct sign in the x-shift between SLs"""
    wheel = self.wheel
    # -- This -1 is to adapt to python list indexing
    sector = self.sector-1 
    station = self.MBtype

    entryname = "Wh"
    if wheel > 0: 
      entryname +=">0"
    elif wheel < 0:
      entryname += "<0"
    else:
      entryname += "0"
    
    shift_signs = self.shift_signs
    sign  = shift_signs[entryname][station][sector]
    
    # FIXME: quick workaround for MB4 sc4 and 10...
    if isinstance(sign, tuple): sign = sign[0]
    self.shift_sign = sign
    
  def build_station(self, adc):
    """ Method to build up the station """
    
    # == First: Generate 8 generic layers
    nLayers = self.nLayers
    nDriftCells = self.nDriftCells
    for idy in range(nLayers):
      new_layer = layer(nDriftCells, idy, adc)
      self.add_layer(new_layer)
    
    # == Second: Place them at the correct position within the chamber
    shift_sign = self.shift_sign
    shift      = shift_sign*self.SLShift
    gap        = self.gap
    cellHeight = self.get_layer(0).get_cell(1).get_height()
    space_SL   = gap/cellHeight 

    # -- Shifts are done in units of drift cell width and height
    self.get_layer(0).shift_layer(-adc           , 0)
    self.get_layer(1).shift_layer(-adc-0.5       , 1)
    self.get_layer(2).shift_layer(-adc           , 2)
    self.get_layer(3).shift_layer(-adc-0.5       , 3)
    self.get_layer(4).shift_layer(-adc+shift     , space_SL+4)
    self.get_layer(5).shift_layer(-adc-0.5+shift , space_SL+5)
    self.get_layer(6).shift_layer(-adc+shift     , space_SL+6)
    self.get_layer(7).shift_layer(-adc-0.5+shift , space_SL+7)

    self.set_center()
  
  def set_center(self):
    """ Set the geometric center of a DT station """
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
    cellWidth = self.get_layer(0).get_cell(1).get_width()
    len_layer = self.nDriftCells*cellWidth
    centerx = (len_layer+cellWidth)/2.0
    self.center = (centerx, centery)
    return
  
  # -------------- Objects in the station
  def clear(self):
    self.showers = []
    self.segments = []
    self.digis = []
    self.genmuons = []
  
  def add_digi(self, digi):
    if digi not in self.digis: self.digis.append(digi)
  
  def add_segment(self, segment):
    if segment not in self.segments: self.segments.append(segment)
  
  def add_genmuon(self, genmuon):
    if genmuon not in self.genmuons: self.genmuons.append(genmuon)
  
  def add_shower(self, shower):
    if shower not in self.showers: self.showers.append(shower)
    
  def summarize(self):
    """ Method to summarize the contents inside a given chamber """ 
    nDigis = len(self.digis)
    nSegments = len(self.segments)
    nShowers = len(self.showers)
    hasMatchingMuon = len(self.genmuons) > 0
    color_msg(f" ---------------- Station {self.name} ----------------", "green", indentLevel = 1)
    color_msg(f"Number of digis: {nDigis}", indentLevel = 2)
    color_msg(f"Number of segments: {nSegments}", indentLevel = 2)
    color_msg(f"Algo found shower?: {nShowers > 0}", indentLevel = 2)
    color_msg(f"has matching muon?: {hasMatchingMuon} ", indentLevel = 2)
    for igm, gm in enumerate(self.genmuons):
      color_msg(f"Muon properties: genPart {gm.idm}, pT {gm.pt}, eta {gm.eta} ", indentLevel = 3)

  
  # -------------- Geometry stuff
  def get_nLayers(self):
    """ Return the number of Layers in this chamber """
    return self.nLayers

  def get_layers(self):
    """ Method to return the object in which Layers are stored """
    return self.Layers
  
  def get_layer(self, layer_id):
    """ Get a layer from its ID list of layers """
    layers = self.get_layers()
    return layers[layer_id]

  def add_layer(self, layer):
    """ Method to add a new layer """
    layers = self.get_layers()
    layers.append(layer)
    return

  def get_center(self):
    return self.center
  
