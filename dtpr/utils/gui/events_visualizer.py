import sys
import os
import re
from functools import cache

from PyQt5.QtWidgets import QApplication, QMainWindow, QListWidgetItem, QShortcut, QProgressBar, QCheckBox
from PyQt5.QtCore import Qt, QTimer
from PyQt5.uic import loadUi
from PyQt5.QtGui import QCursor, QKeySequence

from dtpr.utils.gui.local_plotter import LocalPlotter
from dtpr.utils.gui.artist_gui_manager import ArtistManager
from dtpr.utils.functions import parse_filter_text_4gui
from dtpr.utils.gui.progressbar_manager import ProgressBarManager
from dtpr.base import NTuple


class EventsVisualizer(QMainWindow):
    def __init__(self, inpath, maxfiles=-1):
        super().__init__()
        self.inpath = inpath
        self.maxfiles = maxfiles

        self.artist_manager = ArtistManager()

        if "dt-station-global" not in self.artist_manager.artist_builders:
            raise ValueError("Required artists 'dt-station-global' not found in the configuration file.")

        loadUi(os.path.abspath(os.path.join(os.path.dirname(__file__), "events_visualizer.ui")), self)
        self.initialize_ui_elements()
        self.reset_ui_context()
        self.connect_signals()

    @cache
    def _load_event(self, index):
        # Load the event from the Ntuple
        event = self.ntuple.events[index]
        return event

    def initialize_ui_elements(self):
        # get the axes for the plots
        self.plot_widgets = {"phi": self.plot_widget_phi, "eta": self.plot_widget_eta}
        self.axes = {"phi": self.plot_widget_phi.canvas.axes, "eta": self.plot_widget_eta.canvas.axes}
        self.artist_manager.ax_phi = self.axes["phi"]
        self.artist_manager.ax_eta = self.axes["eta"]
        # Enable nested docking to prevent blocking issues
        self.setDockNestingEnabled(True)
        # Initialize selector states based on current tab
        self.update_selector_states()
        # Initialize progress bar in status bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setMaximumWidth(200)
        self.statusBar.addPermanentWidget(self.progress_bar)
        # Timers for plot updates
        self._wheel_update_timer = QTimer()
        self._wheel_update_timer.setSingleShot(True)
        self._sector_update_timer = QTimer()
        self._sector_update_timer.setSingleShot(True)

        # Create checkboxes for aditional artists
        self.additional_artists_checkboxes = {}
        for name in self.artist_manager.artist_builders.keys():
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

    def reset_ui_context(self):
        """Reset the UI context to the initial state."""
        # init variables
        self.current_event = None
        self.mpl_connection_id = {"phi": None, "eta": None}
        self._progress_context = None
        self._eventlist_search_bar_prevtext = ""
        self._eventtree_search_bar_prevtext = ""

        self.axes["phi"].clear()
        self.axes["eta"].clear()
        self.event_inspector.tree_widget.clear()

        # Create the Ntuple object
        self.ntuple = NTuple(
            inputFolder=self.inpath,
            maxfiles=self.maxfiles,
        )
        self.populate_event_list()

    def populate_event_list(self):
        self.events_list.clear()
        # Get total number of events for progress tracking
        total_events = self.ntuple.tree.GetEntries()
        with ProgressBarManager(
            self.progress_bar,
            self.show_status_message,
            total_steps=total_events,
            message=f"Loading {total_events} events..."
        ) as pb:
            for i, ev in enumerate(self.ntuple.tree):
                item = QListWidgetItem(f"Event {i}")
                item.setToolTip(f"{ev.event_eventNumber}")
                item.setData(Qt.UserRole, (i, ev.event_eventNumber))
                self.events_list.addItem(item)
                # Update progress periodically
                if i % max(1, total_events // 20) == 0:
                    pb.update(i - pb.current_step, f"Loading events... {i + 1}/{total_events}")
            pb.update(total_events - pb.current_step, f"Successfully loaded {total_events} events")
            self.show_status_message(f"Event list populated with {total_events} events", 2000, "success")

    def connect_signals(self):
        self.eventslist_search_bar.editingFinished.connect(self.filter_event_list)
        self.eventtree_search_bar.editingFinished.connect(self.filter_event_tree)
        self.events_list.itemDoubleClicked.connect(self.event_list_item_inspection)
        self.actionEvents_Box.triggered.connect(lambda checked: self.set_dock_widget_visibility(checked, "ev-box"))
        self.actionEvent_inspector.triggered.connect(lambda checked: self.set_dock_widget_visibility(checked, "ev-inspector"))
        
        # checkboxes for additional artists
        for name, checkbox in self.additional_artists_checkboxes.items():
            checkbox.stateChanged.connect(
                lambda state, name=name: self.checkbox_changed(state, name)
            )

        # Connect dock widget visibility changes to menu actions
        self.eventsBox_dockWidget.visibilityChanged.connect(self.actionEvents_Box.setChecked)
        self.event_inspector_dockWidget.visibilityChanged.connect(self.actionEvent_inspector.setChecked)

        # Connect tab widget changes to update selector states
        self.tabWidget.currentChanged.connect(self.update_selector_states)

        # Add keyboard shortcut for resetting dock layout if they get stuck
        reset_shortcut = QShortcut(QKeySequence("Ctrl+R"), self)
        reset_shortcut.activated.connect(self.reset_dock_layout)

        # Connect selector value changes to plot updates with delay
        self.wheel_selector.valueChanged.connect(self.wheel_changed)
        self.sector_selector.valueChanged.connect(self.sector_changed)
        self._wheel_update_timer.timeout.connect(lambda: self._make_plots("phi"))
        self._sector_update_timer.timeout.connect(lambda: self._make_plots("eta"))

    def set_dock_widget_visibility(self, checked, dockwidget):
        """Handle dock widget visibility changes from menu actions"""
        _dock_widget = {"ev-box": self.eventsBox_dockWidget, "ev-inspector": self.event_inspector_dockWidget}.get(dockwidget, None)
        if _dock_widget:
            _dock_widget.setVisible(checked)

    def update_selector_states(self):
        """Update wheel and sector selector states based on current tab"""
        current_tab_index = self.tabWidget.currentIndex()
        
        # Tab 0 is XY (tab_xy), Tab 1 is Zr (tab_zr)
        if current_tab_index == 0:  # XY tab
            # Enable wheel selector, disable sector selector
            self.wheel_selector.setEnabled(True)
            self.sector_selector.setEnabled(False)
        elif current_tab_index == 1:  # Zr tab
            # Disable wheel selector, enable sector selector
            self.wheel_selector.setEnabled(False)
            self.sector_selector.setEnabled(True)

    def filter_event_list(self):
        filter_text = self.eventslist_search_bar.text()
        if filter_text == self._eventlist_search_bar_prevtext:
            return
        self._eventlist_search_bar_prevtext = filter_text
        filter_kwargs = parse_filter_text_4gui(self.eventslist_search_bar.text())
        
        # If no filter is provided, show all items
        if not filter_kwargs:
            for i in range(self.events_list.count()):
                self.events_list.item(i).setHidden(False)
            return

        goal_index = filter_kwargs.pop("index", None)
        goal_number = filter_kwargs.pop("number", None)

        for i in range(self.events_list.count()):
            item = self.events_list.item(i)
            index, ev_number = item.data(Qt.UserRole)
            
            # Check for specific index filter
            if goal_index is not None:
                if goal_index != index:
                    item.setHidden(True)
                    continue
                else:
                    item.setHidden(False)
                    continue
            
            # Check for specific event number filter
            if goal_number is not None:
                if goal_number != ev_number:
                    item.setHidden(True)
                    continue
                else:
                    item.setHidden(False)
                    continue
            
            # NOT IMPLEMENTED - NOT NEEDED AT THE MOMENT
            # # Check for other attribute filters
            # conditions = []
            # ev = self._load_event(index)
            # for key, value in filter_kwargs.items():
            #     attr_value = getattr(ev, key, None)
            #     if attr_value is not None:
            #         conditions.append(attr_value == value)
            #     else:
            #         conditions.append(True)  # If attribute doesn't exist, condition is ignored
            # if all(conditions):
            #     item.setHidden(False)
            # else:
            #     item.setHidden(True)

    def filter_event_tree(self):
        filter_text = self.eventtree_search_bar.text()
        if filter_text == self._eventtree_search_bar_prevtext:
            return
        self._eventtree_search_bar_prevtext = filter_text
        QTimer.singleShot(0, lambda: self.event_inspector.add_event_to_tree(self.current_event, filter_text))

    def event_list_item_inspection(self, item):
        ev_index, ev_number = item.data(Qt.UserRole)
        if ev_index == getattr(self.current_event, "index", -1):  # Check if the event is already loaded
            self.show_status_message(f"Event {ev_number} is already loaded", 2000, "warning")
            return

        with ProgressBarManager(self.progress_bar, self.show_status_message, total_steps=100, message=f"Loading event {ev_number}...") as pb:
            QApplication.setOverrideCursor(QCursor(Qt.CursorShape.WaitCursor))
            try:
                self.current_event = self._load_event(ev_index)
                pb.update(25, f"Event {ev_number} loaded from cache...")

                if self.current_event is None:
                    self.show_status_message("This event did not pass the filters", 5000, "warning")
                    QApplication.restoreOverrideCursor()
                    return

                pb.update(25, f"Adding event {ev_number} to inspector...")
                QTimer.singleShot(0, lambda: self.event_inspector.add_event_to_tree(self.current_event))

                pb.update(10, "Starting plot generation...")
                self._make_plots()
                pb.update(40, "Plots done")

                pb.update(100 - pb.current_step, f"Event {ev_number} loaded successfully")
                self.show_status_message(f"Event {ev_number} loaded successfully", 2000, "success")
            except Exception as e:
                self.show_status_message(f"Error loading event: {e}", 5000, "error")
            finally:
                QApplication.restoreOverrideCursor()

    def wheel_changed(self):
        """Handle wheel selector value changes with delay"""
        if self.current_event is None:
            return
        # Stop any pending timer and start a new one with delay
        self._wheel_update_timer.stop()
        self._wheel_update_timer.start(500)  # 500ms delay

    def sector_changed(self):
        """Handle sector selector value changes with delay"""
        if self.current_event is None:
            return
        # Stop any pending timer and start a new one with delay
        self._sector_update_timer.stop()
        self._sector_update_timer.start(500)  # 500ms delay

    def checkbox_changed(self, state, name):
        """Handle checkbox state changes for additional artists"""
        if self.current_event is None:
            return
        with ProgressBarManager(self.progress_bar, self.show_status_message, total_steps=100, message=f"{'Adding' if state == Qt.CheckState.Checked else 'Removing'} {name} artist...") as pb:
            if QApplication.overrideCursor() is None:
                QApplication.setOverrideCursor(QCursor(Qt.CursorShape.WaitCursor))

            if state == Qt.CheckState.Checked:
                self._embed_artists([name])
                pb.update(100, f"{name} artist added to plot")
            else:
                self.artist_manager.delete_artists([name])
                pb.update(100, f"{name} artist removed from plot")

            if QApplication.overrideCursor() is not None:
                QApplication.restoreOverrideCursor()

    def _make_plots(self, faceview: str = None):
        if QApplication.overrideCursor() is None:
            QApplication.setOverrideCursor(QCursor(Qt.CursorShape.WaitCursor))
        # Needed to avoid callback using the previous event

        for _faceview, mpl_connection_id in self.mpl_connection_id.items():
            if faceview is not None and _faceview != faceview:
                continue
            if mpl_connection_id is not None:
                self.plot_widgets[_faceview].canvas.mpl_disconnect(mpl_connection_id)

            self.axes[_faceview].clear()
            # Reset artists for new plot
            self.artist_manager.artists_included[_faceview] = {}

        with ProgressBarManager(self.progress_bar, self.show_status_message, total_steps=100, message=f"Plotting...") as pb:
            _artist2include = ["cms-shadow-global", "dt-station-global"]
            _artist2include += [name for name, checkbox in self.additional_artists_checkboxes.items() if checkbox.isChecked()]
            self._embed_artists(_artist2include, faceview=faceview)
            pb.update(100, "Plotting done")

        for _faceview in ["phi", "eta"]:
            if faceview is not None and _faceview != faceview:
                continue
            self.mpl_connection_id[_faceview] = self.plot_widgets[_faceview].canvas.mpl_connect(
                'pick_event',
                lambda mpl_event: self.open_local_plotter(mpl_event.artist.station)
            )

        if QApplication.overrideCursor() is not None:
            QApplication.restoreOverrideCursor()

    def _embed_artists(self, artist2include=[""], faceview=None):
        kwargs = {
            "ev": self.current_event,
            "wheel": self.wheel_selector.value(),
            "sector": self.sector_selector.value(),
        }
        self.artist_manager.embed_artists(artist2include, builder_kwargs=kwargs, faceview=faceview)

    def open_local_plotter(self, station):
        local_window = LocalPlotter(parent=self, event=self.current_event, station=station)
        local_window.show()

    def show_status_message(self, message, timeout=2000, type=None, show_progress=False):
        prefix = ""
        if type == "warning":
            self.statusBar.setStyleSheet("color: black; background-color: #fff3cd;")  # light yellow
            prefix = "⚠️ Warning: "
        elif type == "error":
            self.statusBar.setStyleSheet("color: red; background-color: #f8d7da;")  # light red
            prefix = "❗ Error: "
        elif type == "success":
            self.statusBar.setStyleSheet("color: green; background-color: #d4edda;")  # light green
            prefix = "✅ Success: "
        else:
            self.statusBar.setStyleSheet("")  # Reset to default
            
        self.statusBar.showMessage(f"{prefix}{message}", timeout)

        # Only show progress bar if explicitly requested or if we have an active progress context
        if (show_progress or self._progress_context is not None) and not self.progress_bar.isVisible():
            self.progress_bar.setVisible(True)
            self.progress_bar.setRange(0, 100)
        
        QTimer.singleShot(timeout, lambda: self.statusBar.setStyleSheet(""))

    def reset_dock_layout(self):
        """Reset dock widgets to their default positions if they get stuck"""
        # Simple reset: just set them back to not floating
        self.eventsBox_dockWidget.setFloating(False)
        self.event_inspector_dockWidget.setFloating(False)
        
        # Ensure they're visible
        self.eventsBox_dockWidget.setVisible(True)
        self.event_inspector_dockWidget.setVisible(True)
        
        # Update menu actions
        self.actionEvents_Box.setChecked(True)
        self.actionEvent_inspector.setChecked(True)

def launch_visualizer(inpath, maxfiles=-1):
    app = QApplication(sys.argv)
    ex = EventsVisualizer(inpath, maxfiles)
    ex.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    input_folder = os.path.abspath(os.path.join(__file__, "../../../../test/ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root"))
    maxfiles = -1
    launch_visualizer(input_folder, maxfiles)
