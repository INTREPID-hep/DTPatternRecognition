from PyQt5.QtWidgets import QTreeWidget, QTreeWidgetItem, QVBoxLayout, QWidget, QHeaderView
from PyQt5.QtCore import Qt
from ..functions import parse_filter_text_4gui


class EventTreeInspector(QWidget):
    def __init__(self, parent=None):
        super(EventTreeInspector, self).__init__(parent)
        self.vertical_layout = QVBoxLayout()
        self.vertical_layout.setContentsMargins(0, 0, 0, 0)
        self.tree_widget = QTreeWidget()
        self.vertical_layout.addWidget(self.tree_widget)
        self.setLayout(self.vertical_layout)

        self.tree_widget.setHeaderLabels(["Property", "Value"])
        self.tree_widget.header().setDefaultAlignment(Qt.AlignLeft)
        self.tree_widget.header().setStretchLastSection(False)
        self.tree_widget.header().setSectionResizeMode(1, QHeaderView.Stretch)
        self.tree_widget.setStyleSheet(
            "QTreeWidget::item { border-bottom: 1px solid #dcdcdc; border-right: 1px solid #dcdcdc; }"
        )

    def add_event_to_tree(self, event, filter_text=""):
        filter_kwargs = parse_filter_text_4gui(filter_text)
        if not filter_kwargs and filter_text:
            return
        self.tree_widget.clear()  # Clear the tree before adding a new event
        self.current_event = event  # Store the current event for filtering
        event_item = QTreeWidgetItem([f"Event {event.number}", ""])
        self.tree_widget.addTopLevelItem(event_item)

        for particle_name, particle_list in event._particles.items():
            filtered_particles = self.get_filtered_particles(
                event, particle_name, filter_kwargs, particle_list
            )
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
            particle_item = QTreeWidgetItem([f"[{particle.index}]", ""])
            parent_item.addChild(particle_item)
            self.add_properties_to_tree(particle_item, particle, depth=0)

    def add_properties_to_tree(self, parent_item, particle, depth=0, max_depth=2):
        # Prevent infinite recursion by limiting depth
        if depth > max_depth:
            return

        for key, value in particle.__dict__.items():
            if key in ["index", "name"]:
                continue
            self.add_property_item(parent_item, key, value, depth)

    def add_property_item(self, parent_item, key, value, depth=0):
        if isinstance(value, list):
            list_item = QTreeWidgetItem([key, ""])
            parent_item.addChild(list_item)
            self.add_list_items(list_item, key, value, depth)
        else:
            item = QTreeWidgetItem([key, str(value)])
            parent_item.addChild(item)

    def add_list_items(self, list_item, key, value, depth=0, max_depth=2):
        if value and isinstance(value[0], (int, float, str, tuple)):
            for i, item in enumerate(value):
                item_item = QTreeWidgetItem([f"{key}[{i}]", str(item)])
                list_item.addChild(item_item)
        else:
            for i, item in enumerate(value):
                item_item = QTreeWidgetItem([f"{key}[{i}]", ""])
                list_item.addChild(item_item)

                # Check if we're at max depth and the item has an index (likely a particle)
                if depth >= max_depth and hasattr(item, "index"):
                    # Show only the index to prevent recursion
                    index_item = QTreeWidgetItem(["index", str(item.index)])
                    item_item.addChild(index_item)
                else:
                    # Continue recursion with increased depth
                    self.add_properties_to_tree(item_item, item, depth + 1, max_depth)
