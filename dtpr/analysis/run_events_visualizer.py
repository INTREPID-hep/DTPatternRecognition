import os
from dtpr.utils.functions import color_msg
from dtpr.utils.gui.events_visualizer import launch_visualizer
import subprocess as bash

def open_visualizer(inpath: str, use_executable: bool = False,  maxfiles: int = -1) -> None:
    color_msg(f"Launching events visualizer for input path: {inpath}", color="yellow")
    if use_executable:
        color_msg("Using pre-built executable.", color="yellow")
        # Temporarily simulate running the executable by invoking the script via the command line.
        bash.call(f"python {os.path.abspath(os.path.join(__file__,f'../../utils/gui/events_visualizer.py'))} {inpath}", shell=True)
    else:
        launch_visualizer(inpath, maxfiles=maxfiles)
