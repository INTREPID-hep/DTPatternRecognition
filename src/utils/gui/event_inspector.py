from PyQt5.QtWidgets import QTreeWidget, QTreeWidgetItem, QVBoxLayout, QWidget, QMainWindow, QHeaderView, QLineEdit
from PyQt5.QtCore import Qt

class EventInspector(QWidget):
    def __init__(self, parent=None):
        super(EventInspector, self).__init__(parent)
        self.layout = QVBoxLayout()
        self.setLayout(self.layout)

        self.search_bar = QLineEdit()
        self.search_bar.setPlaceholderText("Search by property, e.g, wh=-2; st=1...")
        self.search_bar.textChanged.connect(self.filter_tree)
        self.layout.addWidget(self.search_bar)

        self.tree_widget = QTreeWidget()
        self.tree_widget.setHeaderLabels(["Property", "Value"])
        self.tree_widget.header().setDefaultAlignment(Qt.AlignLeft)
        self.tree_widget.header().setStretchLastSection(False)
        self.tree_widget.header().setSectionResizeMode(1, QHeaderView.Stretch)
        self.tree_widget.setStyleSheet("QTreeWidget::item { border-bottom: 1px solid #dcdcdc; border-right: 1px solid #dcdcdc; }")
        self.layout.addWidget(self.tree_widget)

    def add_event_to_tree(self, event, filter_text=""):
        filter_kwargs = self.parse_filter_text(filter_text)
        if not filter_kwargs and filter_text:
            return
        self.tree_widget.clear()  # Clear the tree before adding a new event
        self.current_event = event  # Store the current event for filtering
        event_item = QTreeWidgetItem([f"Event {event.iev}", ""])
        self.tree_widget.addTopLevelItem(event_item)

        for particle_name, particle_list in event._particles.items():
            filtered_particles = self.get_filtered_particles(event, particle_name, filter_kwargs, particle_list)
            particle_item = QTreeWidgetItem([particle_name, ""])
            event_item.addChild(particle_item)
            self.add_particles_to_tree(particle_item, filtered_particles)

    def get_filtered_particles(self, event, particle_name, filter_kwargs, particle_list):
        try:
            return event.filter_particles(particle_name, **filter_kwargs)
        except:
            return particle_list

    def add_particles_to_tree(self, parent_item, particles):
        for particle in particles:
            particle_item = QTreeWidgetItem([str(particle.index), ""])
            parent_item.addChild(particle_item)
            self.add_properties_to_tree(particle_item, particle)

    def add_properties_to_tree(self, parent_item, particle):
        for key, value in particle.to_dict().items():
            if key == "index":
                continue
            self.add_property_item(parent_item, key, value)

    def add_property_item(self, parent_item, key, value):
        if isinstance(value, list):
            list_item = QTreeWidgetItem([key, ""])
            parent_item.addChild(list_item)
            self.add_list_items(list_item, key, value)
        else:
            item = QTreeWidgetItem([key, str(value)])
            parent_item.addChild(item)

    def add_list_items(self, list_item, key, value):
        if value and isinstance(value[0], (int, float, str, tuple)):
            for i, item in enumerate(value):
                item_item = QTreeWidgetItem([f"{key}[{i}]", str(item)])
                list_item.addChild(item_item)
        else:
            for i, item in enumerate(value):
                item_item = QTreeWidgetItem([f"{key}[{i}]", ""])
                list_item.addChild(item_item)
                self.add_properties_to_tree(item_item, item)

    def filter_tree(self):
        filter_text = self.search_bar.text()
        self.add_event_to_tree(self.current_event, filter_text)

    def parse_filter_text(self, filter_text):
        filter_kwargs = {}
        if filter_text:
            try:
                for part in filter_text.split(";"):
                    if not part:
                        continue
                    key, value = part.split("=")
                    filter_kwargs[key.strip()] = eval(value.strip())
            except:
                pass
        return filter_kwargs

if __name__ == "__main__":
    import sys
    from PyQt5.QtWidgets import QApplication
    from src.ntuples.dtntuple import DtNtuple
    from src.utils.filters import EventFilter

    filter_function = EventFilter.filter_functions().get("no-filter")

    ntuple = DtNtuple(
        inputFolder="../../ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_Simulation_99.root",
        selectors=[filter_function],
        maxfiles=-1,
    )

    event = ntuple.events[4]

    app = QApplication(sys.argv)
    ui = QMainWindow()
    inspector = EventInspector()
    inspector.add_event_to_tree(event)
    ui.setCentralWidget(inspector)
    ui.show()
    sys.exit(app.exec_())