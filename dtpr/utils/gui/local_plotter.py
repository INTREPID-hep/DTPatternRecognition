import os
import sys
from PyQt5.QtWidgets import QApplication, QDialog
from PyQt5.QtCore import Qt, QTimer
from PyQt5.uic import loadUi
from PyQt5.QtGui import QCursor, QKeySequence

from dtpr.utils.gui.mplwidget import PlotWidget  # Import PlotWidget

class LocalPlotter(QDialog):
    def __init__(self, parent=None, event=None, station=None):
        super().__init__(parent)
        loadUi(os.path.abspath("./local_plotter.ui"), self)

    def initialize_ui_elements(self):
        # get the axes for the plots
        self.plot_widgets = {"phi": self.plot_widget_phi, "eta": self.plot_widget_eta}
        self.axes = {"phi": self.plot_widget_phi.canvas.axes, "eta": self.plot_widget_eta.canvas.axes}

        # Create checkboxes for aditional artists
        self.additional_artists_checkboxes = {}
        for name in self.artist_builders.keys():
            if name in ["dt-station-global", "cms-shadow-global"]:
                continue
            pattern = r"^dt-(.+)-global$"
            match = re.match(pattern, name)
            if match:
                checkbox_str = match.group(1).replace('-', ' ').capitalize()
                checkbox = QCheckBox(f"{checkbox_str}")
                checkbox.setObjectName(name)
                checkbox.setChecked(True)  # Default to checked
                self.additional_artists_layout.addWidget(checkbox)
                self.additional_artists_checkboxes[name] = checkbox



if __name__ == "__main__":
    app = QApplication(sys.argv)
    ex = LocalPlotter()
    ex.show()
    sys.exit(app.exec_())