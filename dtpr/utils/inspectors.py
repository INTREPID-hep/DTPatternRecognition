from dtpr.base import Event
from dtpr.utils.functions import color_msg
from tqdm import tqdm
from typing import Optional


def test_inspector(event: Event, tqdm_pbar: Optional[tqdm] = None):
    if tqdm_pbar is None:
        printer = print
    else:
        printer = tqdm_pbar.write

    printer(color_msg("Inspecting event:", color="yellow", return_str=True, indentLevel=1))
    printer(
        color_msg(f"Event number: {event.number}", color="green", indentLevel=2, return_str=True)
    )
    printer(
        color_msg(
            f"Number of GenMuons: {len(event.genmuons)}",
            color="blue",
            indentLevel=2,
            return_str=True,
        )
    )
    printer(
        color_msg(
            f"Number of showers: {len(event.fwshowers)}",
            color="purple",
            indentLevel=2,
            return_str=True,
        )
    )
