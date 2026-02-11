# Dump events analysis generated on 2025-11-27
# 
# Author:
#     Daniel Estrada
# Version:
#     0.1
#
# This function dumps processed events back into a flat ROOT ntuple for later analysis.

import os
import ROOT as r
from typing import List, Dict, Any, Optional
from dtpr.base import Event, NTuple, Particle
from dtpr.base.config import RUN_CONFIG
from dtpr.utils.functions import color_msg, create_outfolder, singularize_pname
from tqdm import tqdm


# Configuration constants
MAX_SAMPLE_EVENTS = 10
MAX_SAMPLE_PARTICLES = 10
PROGRESS_UPDATE_DIVISOR = 10

# Branch name components
EVENT_PREFIX = "event_"
TYPE_SUFFIX = "_type"
INDEX_SUFFIX = "_Idx"

# ROOT type mappings
ROOT_TYPE_MAP = {
    'I': r.std.vector('int'),
    'D': r.std.vector('double'),
    'O': r.std.vector('bool'),
    'C': r.std.vector('string'),
}

VECTOR_TYPE_MAP = {
    'vector<int>': r.std.vector('vector<int>'),
    'vector<double>': r.std.vector('vector<double>'),
    'vector<bool>': r.std.vector('vector<bool>'),
}


def _get_root_type_code(value: Any) -> Optional[str]:
    """
    Get ROOT type code for a Python value.
    
    :param value: Python value to analyze
    :return: ROOT type code ('I', 'D', 'O', 'C') or None
    """
    if isinstance(value, bool):
        return 'O'
    elif isinstance(value, int):
        return 'I'
    elif isinstance(value, float):
        return 'D'
    elif isinstance(value, str):
        return 'C'
    return None


def _get_vector_type_code(elements: List[Any]) -> str:
    """
    Get ROOT vector type for list elements.
    
    :param elements: List elements to analyze
    :return: ROOT vector type string
    """
    if not elements:
        return 'vector<double>'  # Default
    
    first_elem = elements[0]
    if isinstance(first_elem, int):
        return 'vector<int>'
    elif isinstance(first_elem, float):
        return 'vector<double>'
    elif isinstance(first_elem, bool):
        return 'vector<bool>'
    return 'vector<double>'  # Default


def _create_branch_holder(root_type: str) -> Any:
    """
    Create ROOT branch holder for given type.
    
    :param root_type: ROOT type code or vector type string
    :return: ROOT vector holder
    """
    if root_type in ROOT_TYPE_MAP:
        return ROOT_TYPE_MAP[root_type]()
    elif root_type in VECTOR_TYPE_MAP:
        return VECTOR_TYPE_MAP[root_type]()
    return None


def _create_vector_for_values(values: List[Any]) -> Any:
    """
    Create and fill a ROOT vector from Python values.
    
    :param values: Python values to store
    :return: Filled ROOT vector
    """
    if not values:
        return r.std.vector('double')()
    
    if isinstance(values[0], int):
        vec = r.std.vector('int')()
    elif isinstance(values[0], float):
        vec = r.std.vector('double')()
    elif isinstance(values[0], bool):
        vec = r.std.vector('bool')()
    else:
        return r.std.vector('double')()  # Default empty
    
    for val in values:
        vec.push_back(val)
    return vec


def _find_representative_sample(samples: List[Any]) -> Optional[Any]:
    """
    Find first non-empty/non-None sample from list.
    
    :param samples: List of sample values to search
    :return: First valid representative sample or None
    """
    for sample in samples:
        if isinstance(sample, list):
            if len(sample) > 0:
                return sample
        elif sample is not None:
            return sample
    return None


def _determine_branch_type(attr_name: str, representative: Any) -> Dict[str, str]:
    """
    Determine ROOT branch type(s) for given representative value.
    
    :param attr_name: Attribute name
    :param representative: Representative value to analyze
    :return: Dictionary of branch_name -> ROOT type
    """
    if isinstance(representative, Particle):
        # Single particle reference
        return {
            attr_name + INDEX_SUFFIX: 'I',
            attr_name + TYPE_SUFFIX: 'C'
        }
    
    elif isinstance(representative, list):
        if isinstance(representative[0], Particle):
            # List of particle references
            return {
                attr_name + INDEX_SUFFIX: 'vector<int>',
                attr_name + TYPE_SUFFIX: 'C'
            }
        else:
            # Regular list
            return {attr_name: _get_vector_type_code(representative)}
    
    else:
        # Scalar value
        root_type = _get_root_type_code(representative)
        return {attr_name: root_type} if root_type else {}


def _get_particle_branch_structure(particles: List[Particle]) -> Dict[str, str]:
    """
    Analyze a list of particles to determine the branch structure needed for ROOT.
    
    :param particles: List of particles to analyze
    :return: Dictionary mapping attribute names to ROOT types
    """
    if not particles:
        return {}
    
    # Phase 1: Collect all attributes and their observed values
    attr_samples = {}
    for sample in particles[:min(MAX_SAMPLE_PARTICLES, len(particles))]:
        for attr_name, attr_value in sample.__dict__.items():
            if attr_name != 'name' and not attr_name.startswith('_'):
                attr_samples.setdefault(attr_name, []).append(attr_value)
    
    # Phase 2: Determine branch types from collected samples
    branch_types = {}
    for attr_name, samples in attr_samples.items():
        representative = _find_representative_sample(samples)
        
        # if not representative ignore it
        # if representative is None:
        #     # All samples were None or empty lists - default to vector<int>
        #     branch_types[attr_name] = 'vector<int>'
        # else:
        branch_types.update(_determine_branch_type(attr_name, representative))
    
    return branch_types


def _create_branches(tree: r.TTree, sample_events: List[Event], particle_types: List[str]) -> Dict[str, Any]:
    """
    Create branches in the ROOT TTree based on event structure.
    
    :param tree: ROOT TTree object
    :param sample_events: List of sample events to determine structure
    :param particle_types: List of particle type names to include
    :return: Dictionary of branch holders for filling
    """
    branch_holders = {}
    
    # Event-level branches (non-particle attributes) - use first event
    first_event = sample_events[0]
    for attr_name, attr_value in first_event.__dict__.items():
        if attr_name.startswith('_'):
            continue
        
        root_type = _get_root_type_code(attr_value)
        if root_type:
            holder = _create_branch_holder(root_type)
            if holder is not None:
                holder.push_back(0 if root_type == 'I' else 0.0)
                branch_name = EVENT_PREFIX + attr_name
                branch_holders[branch_name] = holder
                tree.Branch(branch_name, holder)
    
    # Particle-level branches - collect particles from all sample events
    for ptype in particle_types:
        all_particles = []
        for event in sample_events:
            particles = getattr(event, ptype, [])
            if particles:
                all_particles.extend(particles)
        
        if not all_particles:
            color_msg(f"Warning: No particles of type '{ptype}' found in sample events", "yellow", indentLevel=2)
            continue
        
        # Use singular form for branch names
        singular_name = singularize_pname(ptype)
        
        # Number of particles of this type
        n_holder = r.std.vector('int')() #<-- this shoudl be scalar also
        n_holder.push_back(0)
        branch_holders[f'{singular_name}_n{ptype.capitalize()}'] = n_holder
        tree.Branch(f'{singular_name}_n{ptype.capitalize()}', n_holder)
        
        # Get branch structure from collected particles
        branch_structure = _get_particle_branch_structure(all_particles)
        
        # Create branches for each attribute
        for attr_name, root_type in branch_structure.items():
            branch_name = f'{singular_name}_{attr_name}'
            holder = _create_branch_holder(root_type)
            if holder is not None:
                branch_holders[branch_name] = holder
                tree.Branch(branch_name, holder)
    
    return branch_holders


def _fill_scalar_attribute(branch_holders: Dict[str, Any], branch_name: str, value: Any) -> None:
    """
    Fill scalar value (int, float, bool, str) into branch.
    
    :param branch_holders: Dictionary of branch holders
    :param branch_name: Name of the branch
    :param value: Scalar value to fill
    """
    if branch_name in branch_holders:
        branch_holders[branch_name].push_back(value)


def _fill_list_attribute(branch_holders: Dict[str, Any], branch_name: str, 
                         values: List[Any], index_key: str, type_key: str) -> None:
    """
    Fill list attribute (particle references or regular list) into branches.
    
    :param branch_holders: Dictionary of branch holders
    :param branch_name: Base name of the branch
    :param values: List values to fill
    :param index_key: Branch name for particle indices
    :param type_key: Branch name for particle type
    """
    if index_key in branch_holders:
        # Particle reference list
        vec_indices = r.std.vector('int')()
        particle_type_name = ""
        if len(values) > 0 and isinstance(values[0], Particle):
            for p in values:
                vec_indices.push_back(p.index)
            particle_type_name = values[0].name
        branch_holders[index_key].push_back(vec_indices)
        branch_holders[type_key].push_back(particle_type_name)
    elif branch_name in branch_holders:
        # Regular list
        vec = _create_vector_for_values(values)
        branch_holders[branch_name].push_back(vec)


def _fill_particle_attribute(branch_holders: Dict[str, Any], 
                              index_key: str, type_key: str, particle: Particle) -> None:
    """
    Fill single particle reference into branches.
    
    :param branch_holders: Dictionary of branch holders
    :param index_key: Branch name for particle index
    :param type_key: Branch name for particle type
    :param particle: Particle to reference
    """
    if index_key in branch_holders:
        branch_holders[index_key].push_back(particle.index)
    if type_key in branch_holders:
        branch_holders[type_key].push_back(particle.name)


def _fill_particle_branches(branch_holders: Dict[str, Any], ptype: str, particles: List[Particle]) -> None:
    """
    Fill all branches for particles of given type.
    
    :param branch_holders: Dictionary of branch holders
    :param ptype: Particle type name
    :param particles: List of particles to fill
    """
    singular_name = singularize_pname(ptype)
    for particle in particles:
        for attr_name, attr_value in particle.__dict__.items():
            if attr_name == 'name' or attr_name.startswith('_'):
                continue
            
            branch_name = f'{singular_name}_{attr_name}'
            index_key = branch_name + INDEX_SUFFIX
            type_key = branch_name + TYPE_SUFFIX
            
            # Dispatch to appropriate handler
            if isinstance(attr_value, (int, float, bool, str)):
                _fill_scalar_attribute(branch_holders, branch_name, attr_value)
            elif isinstance(attr_value, list):
                _fill_list_attribute(branch_holders, branch_name, attr_value, index_key, type_key)
            elif isinstance(attr_value, Particle):
                _fill_particle_attribute(branch_holders, index_key, type_key, attr_value)


def _fill_branches(branch_holders: Dict[str, Any], event: Event, particle_types: List[str]) -> None:
    """
    Fill branch holders with event data.
    
    :param branch_holders: Dictionary of branch holders
    :param event: Event to dump
    :param particle_types: List of particle type names
    """
    # Clear all holders
    for holder in branch_holders.values():
        holder.clear()
    
    # Fill event-level branches
    for attr_name, attr_value in event.__dict__.items():
        branch_name = EVENT_PREFIX + attr_name
        if attr_name.startswith('_') or branch_name not in branch_holders:
            continue
        branch_holders[branch_name].push_back(attr_value)
    
    # Fill particle-level branches
    for ptype in particle_types:
        particles = getattr(event, ptype, [])
        
        # Fill number of particles (use singular form)
        singular_name = singularize_pname(ptype)
        n_key = f'{singular_name}_n{singular_name.capitalize()}'
        if n_key in branch_holders:
            branch_holders[n_key].push_back(len(particles))
        
        _fill_particle_branches(branch_holders, ptype, particles)


def dump_events(
        inpath: str,
        outfolder: str,
        tag: str,
        maxfiles: int,
        maxevents: int,
        tree_name: str = "EVENTS",
        particle_types: Optional[List[str]] = None,
    ) -> None:
    """
    Dump processed events back into a flat ROOT ntuple.
    
    This function reads events from input ntuples, processes them according to the
    configuration (preprocessors/selectors), and writes them to a new ROOT file.
    
    Particle references (e.g., matched_segments) are stored as:
    - A vector of indices: {ptype}_{attr_name}_indices
    - A particle type name: {ptype}_{attr_name}_type
    
    This allows reconstruction of relationships when reading the dumped ntuple later.

    :param inpath: Path to the input folder containing the ntuples.
    :type inpath: str
    :param outfolder: Path to the output folder where results will be saved.
    :type outfolder: str
    :param tag: Tag to identify the output file.
    :type tag: str
    :param maxfiles: Maximum number of files to process (-1 for all).
    :type maxfiles: int
    :param maxevents: Maximum number of events to process (-1 for all).
    :type maxevents: int
    :param tree_name: Name of the output TTree. Defaults to "EVENTS".
    :type tree_name: str
    :param particle_types: List of particle types to dump. If None, dumps all particle types.
    :type particle_types: Optional[List[str]]
    :return: None
    :rtype: None
    """

    # Start of the analysis 
    color_msg(f"Running event dumping...", "green")
    create_outfolder(outfolder)

    # Create the Ntuple object
    ntuple = NTuple(
        inputFolder=inpath,
        maxfiles=maxfiles,
    )

    # Get first valid event to determine structure (before creating output file)
    color_msg("Analyzing event structure...", "purple", indentLevel=1)
    first_event = None
    sample_events = []
    
    # Collect multiple sample events to ensure we capture all particle types and attributes
    for ev in ntuple.events:
        if ev:
            sample_events.append(ev)
            if first_event is None:
                first_event = ev
            if len(sample_events) >= MAX_SAMPLE_EVENTS:
                break
    
    if first_event is None:
        color_msg("No valid events found! Skipping file creation.", "red")
        return
    
    # Prepare output file (only after confirming we have valid events)
    output_filename = os.path.join(outfolder, f"dumped_events{tag}.root")
    output_file = r.TFile(output_filename, "RECREATE")
    output_tree = r.TTree(tree_name, "Dumped events after processing")
    
    # Determine particle types to dump
    if particle_types is None:
        # Collect all particle types from sample events
        all_particle_types = set()
        for ev in sample_events:
            all_particle_types.update(ev._particles.keys())
        particle_types = list(all_particle_types)
    
    color_msg(f"Particle types to dump: {', '.join(particle_types)}", "yellow", indentLevel=1)
    
    # Create branches based on sample events structure
    branch_holders = _create_branches(output_tree, sample_events, particle_types)

    # Process events
    color_msg("Processing and dumping events...", "purple", indentLevel=1)
    
    _maxevents = min(maxevents if maxevents > 0 else len(ntuple.events), len(ntuple.events)) - 1
    
    with tqdm(
        total=_maxevents + 1,
        desc=color_msg(f"Dumping:", color="purple", indentLevel=2, return_str=True),
        ncols=100,
        ascii=True,
        unit=" event"
    ) as pbar:
        each_print = (_maxevents + 1) // PROGRESS_UPDATE_DIVISOR if (_maxevents + 1) > PROGRESS_UPDATE_DIVISOR else 1
        for iev, ev in enumerate(ntuple.events):
            if iev > _maxevents:
                pbar.update(_maxevents + 1 - pbar.n)
                break
            if not ev:
                continue
            if iev > 0 and iev % each_print == 0:
                pbar.update(each_print)

            # Fill branches with event data
            _fill_branches(branch_holders, ev, particle_types)
            
            # Fill the tree
            output_tree.Fill()
    
    # Write and close
    color_msg(f"Writing output file...", "purple", indentLevel=1)
    output_file.cd()
    output_tree.Write()
    output_file.Close()
    
    color_msg(f"Done!", color="green")


if __name__ == "__main__":
    # Example usage
    dump_events(
        inpath=os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "tests", "ntuples", "DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root")),
        outfolder="./results",
        tag="_test",
        maxfiles=1,
        maxevents=10,
        tree_name="EVENTS",
        particle_types=None,  # Dump all particle types
    )
