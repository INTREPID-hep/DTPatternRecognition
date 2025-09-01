from PyQt5.QtWidgets import QProgressBar
from PyQt5.QtCore import QTimer


class ProgressBarManager:
    """
    Context manager for handling progress bar updates in a PyQt5 application.
    Usage:
        with ProgressBarManager(progress_bar, status_callback, total_steps=100, message="Loading...") as pb:
            pb.update(10, "Step 1")
            pb.update(20, "Step 2")
    """

    def __init__(
        self,
        progress_bar: QProgressBar,
        status_callback=None,
        total_steps=100,
        message=None,
        auto_hide=True,
    ):
        self.progress_bar = progress_bar
        self.status_callback = status_callback  # Function to show status messages
        self.total_steps = total_steps
        self.current_step = 0
        self.auto_hide = auto_hide
        self.message = message

    def __enter__(self):
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, self.total_steps)
        self.progress_bar.setValue(0)
        if self.message and self.status_callback:
            self.status_callback(self.message, show_progress=True)
        return self

    def update(self, step_increment=1, message=None):
        self.current_step += step_increment
        progress = min(self.current_step, self.total_steps)
        self.progress_bar.setValue(progress)
        if message and self.status_callback:
            self.status_callback(message, show_progress=True)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.progress_bar.setValue(self.total_steps)
        if self.auto_hide:
            QTimer.singleShot(1000, lambda: self.progress_bar.setVisible(False))
