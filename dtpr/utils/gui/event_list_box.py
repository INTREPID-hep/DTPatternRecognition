from PyQt5.QtWidgets import (
    QListWidget,
    QListWidgetItem,
    QVBoxLayout,
    QWidget,
)
from PyQt5.QtCore import Qt


class EventListBox(QWidget):
    def __init__(self, parent=None):
        super(EventListBox, self).__init__(parent)
        self.vertical_layout = QVBoxLayout()
        self.list_widget = QListWidget()
        self.vertical_layout.addWidget(self.list_widget)
        self.setLayout(self.vertical_layout)

        self.list_widget.setStyleSheet("QListWidget::item { border-bottom: 1px solid #dcdcdc; border-right: 1px solid #dcdcdc; }")

    def populate_events_list(self, ntuple):
        self.ntuple = ntuple
        self.list_widget.clear()
        for i, event in enumerate(ntuple.tree):
            item = QListWidgetItem(f"({i}) Event {event.event_eventNumber}")
            item.setData(Qt.UserRole, f"self.ntuple.events[{i}]")
            self.list_widget.addItem(item)

