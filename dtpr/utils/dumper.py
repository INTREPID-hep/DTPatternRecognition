import os
import warnings
import uproot
import awkward as ak
import yaml
from dtpr.base.particle import Particle
from dtpr.utils.functions import color_msg, create_outfolder


def _sanitize_for_awkward(data):
    """
    Recursively traverse dictionaries and lists. If a Particle instance
    is found, replace it with its 'index' attribute to prevent awkward array
    from crashing on custom Python objects and avoid duplicating data.
    """
    if isinstance(data, list):
        return [_sanitize_for_awkward(item) for item in data]
    elif isinstance(data, dict):
        # We also filter out private attributes starting with "_"
        return {
            key: _sanitize_for_awkward(value)
            for key, value in data.items()
            if not key.startswith("_")
        }
    elif isinstance(data, Particle):
        return data.index
    else:
        # Base case: ints, floats, strings, booleans, etc.
        return data


def _clean_awkward_nulls(array: ak.Array) -> ak.Array:
    """
    Recursively traverses an Awkward Record Array.
    Replaces None with [] for list fields and -999 for scalar fields.
    """
    # Recursive Case: It's a Record array (has fields like 'pt', 'eta', etc.)
    if array.fields:
        cleaned_fields = {}
        for field in array.fields:
            # Recursively drill down into each field
            cleaned_fields[field] = _clean_awkward_nulls(array[field])

        # Recombine the cleaned sub-fields back into a Record array at this level
        return ak.zip(cleaned_fields, depth_limit=1)

    # Base Case: It's a leaf node (actual data array)
    else:
        # ndim > 1 indicates a jagged array (lists). Replace None with empty list []
        if array.ndim > 1:
            return ak.fill_none(array, [], axis=1)
        # ndim == 1 indicates a flat array (scalars). Replace None with -999
        else:
            return ak.fill_none(array, -999)


def _flatten_awkward_to_dict(
    array: ak.Array, prefix: str = "", depth: int = 0, skip_empty: bool = True
) -> dict[str, ak.Array]:
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
    skip_empty = (
        True  # Whether to skip branches that are completely empty since not supported by uproot.
    )
    branches = {}

    for field in ak.fields(array):
        if field == "name":
            continue  # Skip the 'name' field
        col = array[field]
        subfields = ak.fields(col)

        # Build the new branch name (e.g., "muons" + "pt" -> "muons_pt")
        new_name = f"{prefix}_{field}" if prefix else field

        if not subfields:
            # Base Case: It's a plain scalar or 1D jagged array (leaf node)
            _new_name = "event_event" + new_name.capitalize() if depth == 0 else new_name

            type_str = str(ak.type(col))
            if "unknown" in type_str:
                if skip_empty:
                    # SHOULD SKIP EMPTY BRANCHES, NOT POSSIBLE TO CAST TO [] of INT64
                    color_msg(f"Skipping empty branch '{new_name}' (type unknown).", "yellow")
                    continue

            if type_str.count("var *") > 1:
                flat_col = ak.flatten(col, axis=2)
                counts_col = ak.num(col, axis=2)
                # encode target collection in branch name for schema generation if col refers to a Particle collection
                branches[f"{new_name}_flat"] = flat_col
                branches[f"{new_name}_counts"] = counts_col
                continue

            branches[_new_name] = col

        else:
            # Recursive Case: It's a collection / record

            # Top-level collections (depth 0) OR fallback if ID extraction is missing.
            branches.update(
                _flatten_awkward_to_dict(
                    col, prefix=new_name, depth=depth + 1, skip_empty=skip_empty
                )
            )

    return branches


def _build_yaml_schema_from_branches(branches: dict[str, ak.Array]) -> dict:
    """Build a YAML-serializable schema from flattened output branch names."""
    particle_types: dict[str, dict[str, dict[str, str]]] = {}
    _flatten_warning_emitted = False

    for branch_name in branches:
        # Event-level branches are not particle attributes.
        if branch_name.startswith("event_"):
            continue

        if "_" not in branch_name:
            continue

        particle_type, attr_name = branch_name.split("_", 1)
        if particle_type not in particle_types:
            particle_types[particle_type] = {"amount": "n" + branch_name, "attributes": {}}

        if attr_name == "index":
            attr_name = "idx"

        if "_counts" in attr_name:
            continue

        if "_flat" in attr_name:
            attr_name = attr_name.replace("_flat", "")
            particle_types[particle_type]["attributes"][attr_name] = {
                "target": "INCLUDE TARGET IF NEEDED",
                "identifier": "DEFINE IDENTIFIER IF NEEDED",
                "branch": [branch_name, branch_name.replace("_flat", "_counts")],
            }
            if not _flatten_warning_emitted:
                warnings.warn(
                    "Data appears to be flattened, ensure to fix the 'target' and 'identifier' fields in the YAML schema"
                )
                _flatten_warning_emitted = True
            continue

        particle_types[particle_type]["attributes"][attr_name] = {"branch": branch_name}

    return {
        "ntuple_tree_name": "dtprDumper/Events",
        "particle_types": particle_types,
    }


def resolve_output_file(outpath: str, tag: str = "") -> str:
    """
    Path of the ROOT file to write for a dump.

    :param outpath: A file path, or a folder to put the default file name in
    :type outpath: str
    :param tag: Tag appended to the file name
    :type tag: str
    :return: The path of the file to write
    :rtype: str
    """
    if ".root" not in outpath:
        outpath = os.path.join(outpath, "dtpr_events_dumped.root")
    file_name = outpath.replace(".root", f"{tag}.root")
    create_outfolder(os.path.dirname(os.path.abspath(file_name)))
    return file_name


def write_events_file(
    events,
    file_name: str,
    fRNTuple: bool = False,
    include_emptybranches: bool = False,
) -> dict:
    """
    Dump a list of Event objects into one ROOT file, as a TTree or an RNTuple.

    :param events: The events to dump
    :param file_name: Path of the ROOT file to write
    :type file_name: str
    :param fRNTuple: Write an RNTuple instead of a TTree
    :type fRNTuple: bool
    :param include_emptybranches: Keep the branches that are empty in every event
    :type include_emptybranches: bool
    :return: The flattened columns that were written
    :rtype: dict
    """
    raw_event_dicts = [ev.to_dict() for ev in events if ev is not None]
    if not raw_event_dicts:
        color_msg("No events to dump.", "yellow")
        return {}

    # 1. Sanitize: Clean up nested Particle instances (converts them to index integers)
    clean_event_dicts = _sanitize_for_awkward(raw_event_dicts)

    # 2. Let Awkward C++ backend handle the heavy lifting (Row -> Column pivot)
    events_array = ak.from_iter(clean_event_dicts)

    # 3. Recursively clean None values using Awkward
    events_array = _clean_awkward_nulls(events_array)

    # 4. Fast branch renaming (Flattening nested Awkward structures)
    output_data = _flatten_awkward_to_dict(events_array, skip_empty=not include_emptybranches)

    # 5. Write to ROOT
    with uproot.recreate(file_name) as f:
        if fRNTuple:
            f.mkrntuple("dtprDumper/Events", output_data)
        else:
            f.mktree("dtprDumper/Events", output_data)

    color_msg(f"Successfully saved {len(clean_event_dicts)} events to {file_name}", "green")
    return output_data


def remove_nbranches_from_file(file_name: str) -> None:
    """
    Rewrite a dumped file without the branches whose name starts with 'n'.

    :param file_name: Path of the ROOT file to clean
    :type file_name: str
    :return: None
    :rtype: None
    """
    color_msg(f"Removing branches starting with 'n' from {file_name}", "yellow")
    with uproot.open(file_name) as in_file:
        tree = in_file["dtprDumper/Events"]

        # Find branches to keep (those NOT starting with 'n')
        branches_to_keep = [b for b in tree.keys() if not b.startswith("n")]

        # Calculate total uncompressed size in MB
        uncompressed_bytes = sum(tree[b].uncompressed_bytes for b in branches_to_keep)
        uncompressed_mb = uncompressed_bytes / (1024**2)
        new_file_name = file_name.replace(".root", "_clean.root")
        with uproot.recreate(new_file_name) as f:
            # If the total size is less than 1 GB, load all at once
            if uncompressed_mb < 1024:
                f["dtprDumper/Events"] = tree.arrays(branches_to_keep)
            else:
                is_first_chunk = True
                for chunk in tree.iterate(branches_to_keep, step_size="200 MB"):
                    if is_first_chunk:
                        f["dtprDumper/Events"] = chunk
                        is_first_chunk = False
                    else:
                        f["dtprDumper/Events"].extend(chunk)

    os.replace(
        new_file_name, file_name
    )  # Overwrite the original file with the new one without 'n' branches


def write_yaml_schema(
    branch_names,
    file_name: str,
    tag: str = "",
    force_overwrite_yaml: bool = False,
) -> None:
    """
    Write the YAML schema that the dumped file needs to be read back by dtpr.

    :param branch_names: Names of the branches of the dumped file
    :param file_name: Path of the dumped ROOT file (the schema goes next to it)
    :type file_name: str
    :param tag: Tag appended to the schema file name
    :type tag: str
    :param force_overwrite_yaml: Overwrite an existing schema
    :type force_overwrite_yaml: bool
    :return: None
    :rtype: None
    """
    yaml_path = os.path.join(os.path.dirname(file_name), f"dumps_events_config{tag}.yaml")
    if os.path.exists(yaml_path) and not force_overwrite_yaml:
        color_msg(f"YAML schema already exists at {yaml_path}, skipping dump.", "yellow")
        return

    yaml_schema = _build_yaml_schema_from_branches({name: None for name in branch_names})
    with open(yaml_path, "w", encoding="utf-8") as yaml_file:
        yaml.safe_dump(yaml_schema, yaml_file, sort_keys=False)
    color_msg(f"Saved YAML schema to {yaml_path}", "green")


def dumped_branch_names(file_name: str):
    """
    Names of the branches of a dumped ROOT file.

    :param file_name: Path of the dumped file
    :type file_name: str
    :return: The branch names
    :rtype: list of str
    """
    with uproot.open(file_name) as handle:
        return list(handle["dtprDumper/Events"].keys())


def dump_events(
    events,
    outpath: str,
    tag: str = "",
    fRNTuple=False,
    include_emptybranches: bool = False,
    dump_yaml_schema: bool = False,
    force_overwrite_yaml: bool = False,
    remove_nbranches: bool = False,
):
    """
    Dump a list of Event objects to an RNTuple (or TTree) in a ROOT file.
    """
    color_msg(f"Dumping events to {outpath}", "green")

    file_name = resolve_output_file(outpath, tag)
    output_data = write_events_file(
        events, file_name, fRNTuple=fRNTuple, include_emptybranches=include_emptybranches
    )
    if not output_data:
        return

    if remove_nbranches:
        remove_nbranches_from_file(file_name)

    if dump_yaml_schema:
        write_yaml_schema(
            output_data.keys(),
            file_name,
            tag=tag,
            force_overwrite_yaml=force_overwrite_yaml,
        )
