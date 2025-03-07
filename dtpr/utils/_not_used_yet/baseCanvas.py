# Code for getting a general canvas 
# used for producing CMS plots

import ROOT as r
import pandas as pd
import numpy as np
from copy import deepcopy
import sys, os, re
import array

r.gROOT.SetBatch(1)
r.gStyle.SetOptStat(0)
pd.options.display.float_format = "{:,.2f}".format

class baseCanvas(object):
       leg_width = 0.6
       leg_textsize = 0.028
       leg_columns = 1

       cmsprel_x1, cmsprel_y1, cmsprel_x2, cmsprel_y2 = .08, .90, .5, .95
       lumi_x1, lumi_y1, lumi_x2, lumi_y2 = .53, .91, .79, .97
       _noDelete = {"lines" : []}

       def __init__(self, name):
              self.name = name
              self.graphs = {}
              self.histos = {}

              self.new_canvas()
              return        
       
       def SetLogy(self):
              self.p1.SetLogy()
              self.name = self.name+"_logY"

       def make_legend(self, x0, y0, x1, y1):                                                                                                         
              l = r.TLegend(x0, y0, x1, y1)
              l.SetName("l_%s"%(self.name))
              l.SetBorderSize(0)
              l.SetFillColor(0)
              l.SetTextFont(42)
              l.SetTextSize(self.leg_textsize)
              l.SetNColumns(self.leg_columns)
              return l
       
       def add_text(self, text, coords, textSize = 0.07):
              x1, y1, x2, y2 = coords
              align=12 
              note = r.TPaveText(x1, y1, x2, y2,"NDC")
              note.SetTextSize(textSize)
              note.SetFillColor(0)
              note.SetFillStyle(0)
              note.SetLineWidth(0)
              note.SetLineColor(0)
              note.SetTextAlign(align)
              note.SetTextFont(42)
              note.AddText(text)
              self._noDelete[text] = note 
              note.Draw("same")
              
       def add_line(self, xpos0, ypos0, xpos1, ypos1):
              line = r.TLine(xpos0, ypos0, xpos1, ypos1)
              line.SetLineWidth(3)
              self._noDelete["lines"].append(line)
              line.Draw("same") 

       def doSpam(self, textSize = 0.05):
              align=12 
              text = "#bf{CMS} Phase-2 Simulation"
              self.add_text(text, (self.cmsprel_x1, self.cmsprel_y1, self.cmsprel_x2, self.cmsprel_y2), textSize)

       def new_canvas(self, nPads = 1, w = 900, h = 900):
              ''' Method to create a custom canvas (hardcoded) '''

              # By default, just create the canvas
              c = r.TCanvas("c_%s"%(self.name), "", w, h)
              if nPads == 2:
                     # This creates a typical ratio plot
                     c.Divide(1, 2)

                     # ------------------ UPPER PAD -------------- #
                     p1 = c.GetPad(1)
                     p1.SetPad(0, 0.30, 1, 1)
                     p1.SetTopMargin(p1.GetTopMargin()*1.1)
                     p1.SetBottomMargin(0.025)

                     # ------------------ LOWER PAD -------------- #
                     p2 = c.GetPad(2)
                     p2.SetPad(0, 0, 1, 0.30)
                     p2.SetTopMargin(0.06)
                     p2.SetBottomMargin(0.3)

                     self.c = c 
                     self.p1 = p1
                     self.p2 = p2
                     return c, p1, p2
              if nPads == 4:
                     # This creates a canvas with 4 plots piled up together
                     c.Divide(1, 4)

                     # ------------------ FIRST PAD -------------- #
                     p1 = c.GetPad(1)
                     p1.SetGrid()
                     p1.SetPad(0, 0.7, 1, 0.9)
                     p1.SetTopMargin(0.1)
                     p1.SetBottomMargin(0)

                     # ------------------ SECOND PAD -------------- #
                     p2 = c.GetPad(2)
                     p2.SetGrid()
                     p2.SetPad(0, 0.5, 1, 0.7)
                     p2.SetTopMargin(0)
                     p2.SetBottomMargin(0)

                     # ------------------ THIRD PAD -------------- #
                     p3 = c.GetPad(3)
                     p3.SetGrid()
                     p3.SetPad(0, 0.3, 1, 0.5)
                     p3.SetTopMargin(0)
                     p3.SetBottomMargin(0)

                     # ------------------ FOURTH PAD -------------- #
                     p4 = c.GetPad(4)
                     p4.SetGrid()
                     p4.SetPad(0, 0.1, 1, 0.3)
                     p4.SetTopMargin(0)
                     p4.SetBottomMargin(0.1)

                     self.c = c 
                     self.p1 = p1
                     self.p2 = p2
                     self.p3 = p3
                     self.p4 = p4
                     return c, p1, p2, p3, p4
              self.c = c
              return c

       def save_canvas(self, outpath):
              if not os.path.exists(outpath):    os.system("mkdir -p %s"%outpath)
              self.c.SaveAs(outpath+"/%s.png"%self.name)

              # Also save the index.php file so it can be seen from a web
              if not os.path.isfile(outpath+"/index.php"):
                     os.system("cp utils/index.php %s"%outpath)
