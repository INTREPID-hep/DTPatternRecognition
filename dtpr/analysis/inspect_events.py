from dtpr.base import NTuple
from dtpr.base.config import RUN_CONFIG
from dtpr.utils.functions import color_msg, get_callable_from_src
from functools import partial
from tqdm import tqdm


def inspect_events(inpath: str, maxfiles: int, event_number: int):
    """
    Inspect a specific event from NTuples.

    :param inpath: Path to the input folder containing the ntuples.
    :type inpath: (str)
    :param outfolder: Path to the output folder where debug information will be saved.
    :type outfolder: (str)
    :param filter_type: Type of event filter to apply.
    :type filter_type: (str)
    :param maxfiles: Maximum number of files to process.
    :type maxfiles: (int)
    :param event_number: The event number to inspect or a slice string indicating the slice, e.g. 1:10:2. Default is 0.
    :type event_number: (int or str)
    :param debug: If True, enables debug mode. Default is False.
    :type debug: (bool)
    """
    # Start of the analysis
    color_msg(f"Inpecting event {event_number} from NTuples", "green")

    # Create the Ntuple object
    ntuple = NTuple(
        inputFolder=inpath,
        maxfiles=maxfiles,
    )

    # getting the method to inspect the events
    inspector_functions = []
    for insp, insp_info in getattr(RUN_CONFIG, "inspector-functions", {}).items():
        src = insp_info.get("src", None)
        inspector = get_callable_from_src(src)
        if inspector is None:
            raise ValueError(f"Inspector function {insp} not found in {src}")
        kwargs = insp_info.get("kwargs", {})
        if kwargs:
            inspector_functions.append(partial(inspector, **kwargs))
        else:
            inspector_functions.append(inspector)

    if not inspector_functions:
        inspector_functions = [lambda ev: tqdm.write(ev.__str__())]

    if isinstance(event_number, str):
        event_indices = eval(f"slice({event_number.replace(':', ',')})")
        events = ntuple.events[event_indices]
        beg, *end = event_number.split(":")
        try:
            total = (int(end[0]) - int(beg)) // int(end[1])
        except IndexError:
            total = int(end[0]) - int(beg)
    else:
        if event_number == -1:
            events = ntuple.events
            total = len(ntuple.events)
        else:
            events = [ntuple.events[event_number]]
            total = 1

    with tqdm(
        total=total,
        desc=color_msg("Running:", color="purple", indentLevel=1, return_str=True),
        ncols=100,
        ascii=True,
        unit=" event",
    ) as pbar:
        for ev in events:
            if not ev:
                tqdm.write(color_msg(f"Event not pass filter: {ev}", color="red", return_str=True))
                continue
            if total < 10:
                pbar.update(1)
            elif ev.index % (total // 10) == 0:
                pbar.update(total // 10)

            if inspector_functions:
                for inspector in inspector_functions:
                    inspector(ev, tqdm_pbar=pbar)
            else:
                tqdm.write(ev.__str__())

    color_msg(f"Done!", color="green")
