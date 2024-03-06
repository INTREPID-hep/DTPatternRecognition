""" 
            Implementation of the DT plotter class
---------------------------------------------------------------------
 Used for plotting hits, segments, and generated muons for a given event.
 It is not recommended to run this code for multiple events, since it is
 very disk consuming (plots are quite full of stuff).
---------------------------------------------------------------------
"""
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import os

class dtpatch(object):
    def __init__(self, MB):
        self.patterns = []
        self.pattern_axes = []
        self.pattern_labels = []

        self.current_DT = MB
        self.create_canvas() 
        self.cells = []
        self.plot_DT()
        return


    def show(self):
        # Plot the legend  
        bbox = (0.4, 0.65, 0.3, 0.2)
        axes_with_labels = [ax[0] for ax in self.pattern_axes]
        self.axes.legend(axes_with_labels, self.pattern_labels, mode = "expand", bbox_to_anchor = bbox, shadow = True)
        plt.ion()
        plt.show()
        return

    def create_canvas(self):
        fig = plt.figure(figsize=(10, 8))
        self.fig = fig
        axes = self.fig.add_subplot(111)

        axes.set_xlabel("x[cm]")
        axes.set_ylabel("y[cm]")
        axes.set_ylim(-0.5, 50)
        axes.set_xlim(-10, self.current_DT.nDriftCells*self.current_DT.cellWidth*1.05)
        self.axes = axes
        return

    def plot_cell(self, xmin, ymin, width, height, linewidth = 1, edgecolor = 'k', color = 'none', alpha=1):
        cell = patches.Rectangle((xmin, ymin), width, height, linewidth=linewidth, edgecolor=edgecolor, facecolor=color, alpha = alpha)
        self.cells.append(cell)
        self.axes.add_patch(cell)
        return

    def plot_DT(self):        
        MB = self.current_DT
        for layer in MB.Layers:
            for cell in layer.DriftCells:
                xmin = cell.x
                ymin = cell.y
                width = cell.width
                height = cell.height
                
                self.plot_cell(xmin, ymin, width, height, edgecolor = 'k')
        return 

    def save_canvas(self, name, path = "./results"):
        if not os.path.exists(path):
            os.system("mkdir -p %s"%(path))
        self.fig.savefig(path + "/" + name+".pdf")
        self.fig.savefig(path + "/" + name+".png")
        return