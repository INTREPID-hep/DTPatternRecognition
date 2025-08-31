import os
import sys
import re
from PyQt5.QtWidgets import QApplication, QDialog, QCheckBox
from PyQt5.QtCore import Qt
from PyQt5.uic import loadUi
from dtpr.utils.gui.artist_gui_manager import ArtistManager

class LocalPlotter(QDialog):
    def __init__(self, parent=None, event=None, station=None):
        super().__init__()
        if event is None or station is None:
            raise ValueError("Event and station must be provided to LocalPlotter.")
        self.event = event
        self.station = station

        # Use parent's artist_builders if available, else parse configs
        artist_builders = parent.artist_manager.artist_builders if parent and hasattr(parent, "artist_manager") else None

        # Initialize ArtistManager with current axes (will be set after UI loads)
        self.artist_manager = ArtistManager(artist_builders=artist_builders)

        if "dt-station-local" not in self.artist_manager.artist_builders:
            raise ValueError("Required artist 'dt-station-local' not found in the configuration file.")

        loadUi(os.path.abspath(os.path.join(os.path.dirname(__file__), "local_plotter.ui")), self)
        self.initialize_ui_elements()
        self.connect_signals()
        self._make_plots()

    def initialize_ui_elements(self):
        # Set the title label
        self.title_label.setText(f"Event {self.event.number} | {self.station.name}")
        self.title_label.setStyleSheet("font-weight: bold; font-size: 14pt; qproperty-alignment: 'AlignCenter';")
        # get the axes for the plots
        self.plot_widgets = {"phi": self.plot_widget_phi, "eta": self.plot_widget_eta}
        self.axes = {"phi": self.plot_widget_phi.canvas.axes, "eta": self.plot_widget_eta.canvas.axes}
        self.axes["phi"].set_title(r'$\phi$ View')
        self.axes["eta"].set_title(r'$\eta$ View')

        # Set axes in artist_manager
        self.artist_manager.ax_phi = self.axes["phi"]
        self.artist_manager.ax_eta = self.axes["eta"]

        # Create checkboxes for additional artists
        self.additional_artists_checkboxes = {}
        for name in self.artist_manager.artist_builders.keys():
            if name == "dt-station-local":
                continue
            pattern = r"^dt-(.+)-local$"
            match = re.match(pattern, name)
            if match:
                checkbox_str = match.group(1).replace('-', ' ').capitalize()
                checkbox = QCheckBox(f"{checkbox_str}")
                checkbox.setObjectName(name)
                checkbox.setChecked(True)  # Default to checked
                self.additional_artists_layout.addWidget(checkbox)
                self.additional_artists_checkboxes[name] = checkbox

    def connect_signals(self):
        # checkboxes for additional artists
        for name, checkbox in self.additional_artists_checkboxes.items():
            checkbox.stateChanged.connect(
                lambda state, name=name: self.checkbox_changed(state, name)
            )

    def _make_plots(self):
        _artist2include = ["dt-station-local"]
        _artist2include += [name for name, checkbox in self.additional_artists_checkboxes.items() if checkbox.isChecked()]
        self._embed_artists(_artist2include)

    def checkbox_changed(self, state, name):
        if state == Qt.CheckState.Checked:
            self._embed_artists([name])
        else:
            self._delete_artists([name])

    def _embed_artists(self, artist2include=[""]):
        wheel, sector, station = self.station.wheel, self.station.sector, self.station.number
        kwargs = {
            "ev": self.event,
            "wheel": wheel,
            "sector": sector,
            "station": station,
        }
        self.artist_manager.embed_artists(artist2include, kwargs)

    def _delete_artists(self, artist2delete=[""]):
        self.artist_manager.delete_artists(artist2delete)

if __name__ == "__main__":
    from dtpr.base import NTuple
    from mpldts.geometry import Station

    ntuple = NTuple(
        os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../tests/ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root"))
    )
    event = ntuple.events[9]
    station = Station(-2, 5, 2)
    app = QApplication(sys.argv)
    ex = LocalPlotter(event=event, station=station)
    ex.show()
    sys.exit(app.exec_())