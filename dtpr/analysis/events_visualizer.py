import sys
import os
import importlib
import matplotlib.pyplot as plt
from PyQt5.QtWidgets import QApplication, QMainWindow, QWidget, QHBoxLayout, QListWidgetItem
from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtGui import QCursor
from PyQt5.uic import loadUi
from pandas import DataFrame
from copy import deepcopy
from mplhep import style

from dtpr.utils.gui.mplwidget import PlotWidget  # Import PlotWidget
from dtpr.base import NTuple
from dtpr.analysis.plot_dt_chambers import embed_dtwheel2axes
from dtpr.analysis.plot_dt_chamber import embed_dt2axes
from dtpr.utils.config import RUN_CONFIG


class EventsVisualizer(QMainWindow):
    def __init__(self, inpath, maxfiles=-1):
        super().__init__()

        # Create the Ntuple object
        self.ntuple = NTuple(
            inputFolder=inpath,
            maxfiles=maxfiles,
        )

        # load configs for plotting
        self.bounds_kwargs = deepcopy(RUN_CONFIG.dt_plots_configs["bounds-kwargs"])
        self.cells_kwargs = deepcopy(RUN_CONFIG.dt_plots_configs["cells-kwargs"])

        cmap_configs = deepcopy(RUN_CONFIG.dt_plots_configs["cmap-configs"])

        cmap = plt.get_cmap(cmap_configs["cmap"]).copy()
        cmap.set_under(cmap_configs["cmap_under"])

        norm_module, norm_name = cmap_configs["norm"].pop("class").rsplit('.', 1)
        module = importlib.import_module(norm_module)
        norm = getattr(module, norm_name)(**cmap_configs["norm"])

        self.cells_kwargs.update({"cmap": cmap, "norm": norm})

        loadUi(os.path.abspath(os.path.join(os.path.dirname(__file__), "../utils/gui/events_visualizer.ui")), self)
        self.initialize_ui_elements()
        self.connect_signals()

    def initialize_ui_elements(self):
        self.events_loaded = 100
        self.populate_event_list()
        self.current_event = None
        self.mpl_connection_id = None
        self.wheel_changed_timer = QTimer()
        self.wheel_changed_timer.setSingleShot(True)

    def connect_signals(self):
        self.search_bar.textChanged.connect(self.on_search_text_changed)
        self.wheel_selector.valueChanged.connect(self.on_wheel_changed)
        self.wheel_changed_timer.timeout.connect(self.plot_event)
        self.event_list.itemClicked.connect(self.on_event_list_item_clicked)

    def on_search_text_changed(self, text):
        for i in range(self.event_list.count()):
            item = self.event_list.item(i)
            if text.lower() in item.text().lower():
                item.setHidden(False)
            else:
                item.setHidden(True)

    def plot_event_aux(self):
        self.wheel_changed_timer.start(500)

    def on_wheel_changed(self):
        current_item = self.event_list.currentItem()
        if (current_item and current_item.text() != "More..."):
            self.plot_event_aux()

    def populate_event_list(self):
        current_count = self.event_list.count()
        for i, event in enumerate(self.ntuple.events[current_count:self.events_loaded]):
            if event is None:
                prev_item = self.event_list.item(self.event_list.count() - 1)
                prev_event_num = int(prev_item.text().split()[1]) if prev_item else 0
                event_num = prev_event_num + 1
                item = QListWidgetItem(f"event {event_num}")
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEnabled & ~Qt.ItemFlag.ItemIsSelectable)
            else:
                item = QListWidgetItem(f"event {event.index}")
                event_showered = bool(event.filter_particles("genmuons", showered=True) and True)
                if event_showered:
                    item.setForeground(Qt.GlobalColor.green)
                else:
                    item.setForeground(Qt.GlobalColor.red)
            self.event_list.addItem(item)
        if len(self.ntuple.events) > self.events_loaded:
            self.event_list.addItem("More...")

    def on_event_list_item_clicked(self, item):
        if item.text() == "More...":
            self.load_more_events()
        else:
            if not item.flags() & Qt.ItemFlag.ItemIsEnabled:
                return
            self.current_event = self.ntuple.events[int(item.text().split()[1])]
            self.event_inspector.add_event_to_tree(self.current_event)
            self.plot_event_aux()


    def load_more_events(self):
        QApplication.setOverrideCursor(QCursor(Qt.CursorShape.WaitCursor))
        self.event_list.takeItem(self.events_loaded)
        self.events_loaded += 100
        self.populate_event_list()
        QApplication.restoreOverrideCursor()

    def plot_event(self):
        QApplication.setOverrideCursor(QCursor(Qt.CursorShape.WaitCursor))
        event = self.current_event
        wheel = self.wheel_selector.value()  # Get the selected wheel value
        ax = self.plot_widget.canvas.axes
        ax.clear()  # Clear previous plot

        ax = embed_dtwheel2axes(event, wheel, ax, bounds_kwargs=self.bounds_kwargs, cells_kwargs=self.cells_kwargs)  # Use create_wheel_axes to plot the wheel
        self.plot_widget.canvas.draw()  # Redraw the canvas
        # Connect the click event to the callback function
        if self.mpl_connection_id is not None:
            self.plot_widget.canvas.mpl_disconnect(self.mpl_connection_id)  # Disconnect previous event
        self.mpl_connection_id = self.plot_widget.canvas.mpl_connect('pick_event', self.on_pick)  # Connect new event
        QApplication.restoreOverrideCursor()

    def on_pick(self, mpl_event):
        artist = mpl_event.artist
        station = artist.station
        self.show_local_plot(station)

    def show_local_plot(self, station):
        QApplication.setOverrideCursor(QCursor(Qt.CursorShape.WaitCursor))
        event = self.current_event
        window = QMainWindow(self)
        window.setWindowTitle(f"Event {event.index} - Local Plot for {station.name}")
        central_widget = QWidget(window)
        window.setCentralWidget(central_widget)
        layout = QHBoxLayout(central_widget)

        plot_widget_phi = PlotWidget(window)
        plot_widget_eta = PlotWidget(window)
        layout.addWidget(plot_widget_phi)
        layout.addWidget(plot_widget_eta)

        ax1 = plot_widget_phi.canvas.axes
        ax2 = plot_widget_eta.canvas.axes

        ax1, patch1 = embed_dt2axes(station, "phi", ax1, bounds_kwargs=self.bounds_kwargs, cells_kwargs=self.cells_kwargs)
        ax2, patch2 = embed_dt2axes(station, "eta", ax2, bounds_kwargs=self.bounds_kwargs, cells_kwargs=self.cells_kwargs)

        wh = station.wheel
        sc = station.sector
        st = station.number

        for sl in [1, 2, 3]:
            # circules in simhits
            simhits_info = DataFrame([simhit.__dict__ for simhit in event.filter_particles("simhits", wh=wh, sc=sc, st=st, sl=sl)], columns=["l", "w", "particle_type"])
            for row in simhits_info.itertuples():
                l, w, particle_type = row.l, row.w, abs(row.particle_type)
                if particle_type == 13:
                    color = "red"
                    size = 35
                    marker = "*"
                else:
                    color = "yellow"
                    size = 10
                    marker = "o"
                center = station.super_layer(sl).layer(l).cell(w).local_center
                if sl==2:
                    ax2.scatter(center[0], center[2], color=color, s=size, marker=marker)
                else:
                    ax1.scatter(center[0], center[2], color=color, s=size, marker=marker)

        segments = [seg for gm in event.genmuons for seg in gm.matches if seg.wh==wh and seg.sc==sc and seg.st==st]
        other_segments = [seg for seg in event.segments if seg not in segments and seg.wh==wh and seg.sc==sc and seg.st==st]
        sl1_center = station.super_layer(1).local_center
        sl3_center = station.super_layer(3).local_center
        for seg in segments:
            ax1.axline(xy1=(seg.pos_locx_sl1, sl1_center[2]), xy2=(seg.pos_locx_sl3, sl3_center[2]), color="red", linestyle="-")
        for seg in other_segments:
            ax1.axline(xy1=(seg.pos_locx_sl1, sl1_center[2]), xy2=(seg.pos_locx_sl3, sl3_center[2]), color="blue", linestyle="--")

        _, cmap_var = RUN_CONFIG.dt_plots_configs["dt-cell-info"].values()
        plot_widget_phi.canvas.figure.colorbar(patch1.cells_collection, ax=ax1, label=f"{cmap_var}")
        plot_widget_eta.canvas.figure.colorbar(patch2.cells_collection, ax=ax2, label=f"{cmap_var}")

        plot_widget_phi.canvas.draw()
        plot_widget_eta.canvas.draw()
        window.show()
        QApplication.restoreOverrideCursor()

def launch_visualizer(inpath, maxfiles=-1):
    mplhep_style = RUN_CONFIG.dt_plots_configs.get("mplhep-style", None)
    if mplhep_style and hasattr(style, mplhep_style):
        with plt.style.context(getattr(style, mplhep_style)):
            plt.rcParams.update(RUN_CONFIG.dt_plots_configs["figure-configs"])
            app = QApplication(sys.argv)
            ex = EventsVisualizer(inpath, maxfiles)
            ex.show()
            sys.exit(app.exec())
    else:
        plt.rcParams.update(RUN_CONFIG.dt_plots_configs["figure-configs"])
        app = QApplication(sys.argv)
        ex = EventsVisualizer(inpath, maxfiles)
        ex.show()
        sys.exit(app.exec())

if __name__ == '__main__':
    input_folder = "../../test/ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root"
    maxfiles = -1
    launch_visualizer(input_folder, maxfiles)
