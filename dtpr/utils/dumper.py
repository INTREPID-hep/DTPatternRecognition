import uproot
import awkward as ak
from dtpr.base.particle import Particle
from dtpr.utils.functions import color_msg


def _sanitize_for_awkward(data):
    """
    Recursively traverse dictionaries and lists. If a Particle instance
    is found, replace it with its 'index' attribute to prevent awkward array 
    from crashing on custom Python objects and avoid duplicating data.
    """
    if isinstance(data, list):
        return [_sanitize_for_awkward(item) for item in data]
    elif isinstance(data, dict):
        # We also filter out "name" and private attributes starting with "_"
        return {
            key: _sanitize_for_awkward(value) 
            for key, value in data.items() 
            if key != "name" and not key.startswith("_")
        }
    elif isinstance(data, Particle):
        return data.index
    else:
        # Base case: ints, floats, strings, booleans, etc.
        return data

def _flatten_awkward_to_dict(
    array: ak.Array,
    prefix: str = "",
    depth: int = 0,
    skip_empty: bool = True
) -> dict[str, ak.Array ]:
    """Recursively flatten an Awkward array into a dictionary of 1D columns.

    Parameters
    ----------
    array : ak.Array or dak.Array
        The array (or sub-array) to flatten.
    prefix : str, optional
        The accumulated branch name from parent records (e.g., "muons_matched").
    depth : int, optional
        Current recursion depth. 0 = top-level events, 1 = main collections, etc.

    Returns
    -------
    dict
        A dictionary mapping flattened branch names (strings) to Awkward arrays.
    """
    skip_empty = True # Whether to skip branches that are completely empty since not supported by uproot. 
    branches = {}

    for field in ak.fields(array):
        col = array[field]
        subfields = ak.fields(col)

        # Build the new branch name (e.g., "muons" + "pt" -> "muons_pt")
        new_name = f"{prefix}_{field}" if prefix else field

        if not subfields:
            # Base Case: It's a plain scalar or 1D jagged array (leaf node)
            _new_name = "event_" + new_name if depth == 0 else new_name

            type_str = str(ak.type(col))
            if "unknown" in type_str:
                if skip_empty:
                    #SHOULD SKIP EMPTY BRANCHES, NOT POSSIBLE TO CAST TO [] of INT64
                    color_msg(f"Skipping empty branch '{new_name}' (type unknown).", "yellow")
                    continue

            if type_str.count("var *") > 1:
                flat_col = ak.flatten(col, axis=2)
                counts_col = ak.num(col, axis=2)

                branches[f"{new_name}_flat_ids"] = flat_col
                branches[f"{new_name}_flat_counts"] = counts_col
                continue

            branches[_new_name] = col

        else:
            # Recursive Case: It's a collection / record

            # Top-level collections (depth 0) OR fallback if ID extraction is missing.
            branches.update(_flatten_awkward_to_dict(col, prefix=new_name, depth=depth + 1, skip_empty=skip_empty))

    return branches


def dump_events(events, outpath: str, fRNTuple = False, include_emptybranches: bool = False):
    """
    Dump a list of Event objects to an RNTuple (or TTree) in a ROOT file.
    """
    color_msg(f"Dumping events to {outpath}", "green")

    # 1. Convert Event objects to raw dictionaries using your class's built-in method
    raw_event_dicts = [ev.to_dict() for ev in events if ev is not None]

    if not raw_event_dicts:
        color_msg("No events to dump.", "yellow")
        return

    # 2. Sanitize: Clean up nested Particle instances (converts them to index integers)
    clean_event_dicts = _sanitize_for_awkward(raw_event_dicts)

    # 3. Let Awkward C++ backend handle the heavy lifting (Row -> Column pivot)
    events_array = ak.from_iter(clean_event_dicts)

    # 4. Fast branch renaming (Flattening nested Awkward structures)
    output_data = _flatten_awkward_to_dict(events_array, skip_empty=not include_emptybranches)

    # 5. Write to ROOT
    with uproot.recreate(outpath) as f:
        if fRNTuple:
            f.mkrntuple("dtprDumper/Events", output_data)
        else:
            f.mktree("dtprDumper/Events", output_data)

        color_msg(f"Successfully saved {len(clean_event_dicts)} events", "green")
