import os
from dtpr.utils.functions import color_msg
from dtpr.utils.gui.events_visualizer import launch_visualizer
import subprocess as bash
from typing import Optional

def open_visualizer(inpath: str, use_executable: Optional[bool] = False,  maxfiles: Optional[int] = -1) -> None:
    """
    Launch the events visualizer for a given input path.

    :param inpath: Path to the input file or directory to visualize.
    :type inpath: str
    :param use_executable: If True, use the pre-built executable; otherwise, launch the Python visualizer script. Default is False.
    :type use_executable: Optional[bool]
    :param maxfiles: Maximum number of files to load. Default is -1 (no limit).
    :type maxfiles: Optional[int]
    :return: None
    :rtype: None
    """
    color_msg(f"Launching events visualizer for input path: {inpath}", color="yellow")
    if use_executable:
        color_msg("Using pre-built executable.", color="yellow")
        # Temporarily simulate running the executable by invoking the script via the command line.
        bash.call(f"python {os.path.abspath(os.path.join(__file__,f'../../utils/gui/events_visualizer.py'))} {inpath}", shell=True)
    else:
        launch_visualizer(inpath, maxfiles=maxfiles)
