# Dump events analysis - Version 2
# 
# Author:
#     Daniel Estrada
# Version:
#     0.2
#
# This function dumps processed events back into a flat ROOT ntuple for later analysis.
# Refactored to handle event scalars, event vectors, particle vectors, and particle vector-of-vectors.

import os
import array
import ROOT as r
from typing import List, Dict, Any, Optional, Tuple
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

# ============================================================================
# TYPE SYSTEM: Comprehensive mapping for all possible types
# ============================================================================

# Scalar types (for event attributes and particle counts)
# Format: type_code -> (array_typecode, ROOT_descriptor)
SCALAR_TYPES = {
    'int': ('i', 'I'),
    'float': ('f', 'F'),
    'double': ('d', 'D'),
    'bool': ('b', 'O'),
    'char': ('b', 'B'),
    'short': ('h', 'S'),
    'long': ('l', 'L'),
    'uchar': ('B', 'b'),
    'ushort': ('H', 's'),
    'uint': ('I', 'i'),
    'ulong': ('L', 'l'),
}

# Vector types (for particle attributes - multiple values per event)
# Format: type_code -> ROOT vector class
VECTOR_TYPES = {
    'vector_int': lambda: r.std.vector('int')(),
    'vector_float': lambda: r.std.vector('float')(),
    'vector_double': lambda: r.std.vector('double')(),
    'vector_bool': lambda: r.std.vector('bool')(),
    'vector_char': lambda: r.std.vector('char')(),
    'vector_short': lambda: r.std.vector('short')(),
    'vector_long': lambda: r.std.vector('long')(),
    'vector_uchar': lambda: r.std.vector('unsigned char')(),
    'vector_ushort': lambda: r.std.vector('unsigned short')(),
    'vector_uint': lambda: r.std.vector('unsigned int')(),
    'vector_ulong': lambda: r.std.vector('unsigned long')(),
    'vector_string': lambda: r.std.vector('string')(),
}

# Vector-of-vector types (for particle attributes that are lists)
# Format: type_code -> ROOT vector<vector> class
VECTOR_VECTOR_TYPES = {
    'vector_vector_int': lambda: r.std.vector('vector<int>')(),
    'vector_vector_float': lambda: r.std.vector('vector<float>')(),
    'vector_vector_double': lambda: r.std.vector('vector<double>')(),
    'vector_vector_bool': lambda: r.std.vector('vector<bool>')(),
    'vector_vector_char': lambda: r.std.vector('vector<char>')(),
    'vector_vector_short': lambda: r.std.vector('vector<short>')(),
    'vector_vector_long': lambda: r.std.vector('vector<long>')(),
    'vector_vector_uchar': lambda: r.std.vector('vector<unsigned char>')(),
    'vector_vector_ushort': lambda: r.std.vector('vector<unsigned short>')(),
    'vector_vector_uint': lambda: r.std.vector('vector<unsigned int>')(),
    'vector_vector_ulong': lambda: r.std.vector('vector<unsigned long>')(),
    'vector_vector_string': lambda: r.std.vector('vector<string>')(),
}


def _python_type_to_base_type(value: Any) -> Optional[str]:
    """
    Map Python type to base type name.
    
    :param value: Python value to analyze
    :return: Base type name ('int', 'float', 'double', 'bool', 'string') or None
    """
    if isinstance(value, bool):
        return 'bool'
    elif isinstance(value, int):
        return 'int'
    elif isinstance(value, float):
        return 'double'
    elif isinstance(value, str):
        return 'string'
    return None


def _determine_branch_type(attr_name: str, representative: Any, is_event_attr: bool = False) -> Dict[str, str]:
    """
    Determine ROOT branch type(s) for given representative value.
    Works for both event and particle attributes.
    
    :param attr_name: Attribute name
    :param representative: Representative value to analyze
    :param is_event_attr: True if this is an event-level attribute (scalar), False for particle attribute (vector)
    :return: Dictionary of branch_name -> type_code
    """
    if isinstance(representative, Particle):
        # Single particle reference (only in particle attributes)
        return {
            attr_name + INDEX_SUFFIX: 'vector_int' if not is_event_attr else 'int',
            attr_name + TYPE_SUFFIX: 'vector_string' if not is_event_attr else 'string'
        }
    
    elif isinstance(representative, list):
        if len(representative) == 0:
            # Empty list - default to double
            if is_event_attr:
                return {attr_name: 'vector_double'}
            else:
                return {attr_name: 'vector_vector_double'}
        
        first_elem = representative[0]
        
        if isinstance(first_elem, Particle):
            # List of particle references
            if is_event_attr:
                return {
                    attr_name + INDEX_SUFFIX: 'vector_int',
                    attr_name + TYPE_SUFFIX: 'string'
                }
            else:
                return {
                    attr_name + INDEX_SUFFIX: 'vector_vector_int',
                    attr_name + TYPE_SUFFIX: 'vector_string'
                }
        else:
            # Regular list of primitives
            base_type = _python_type_to_base_type(first_elem)
            if base_type is None:
                base_type = 'double'
            
            if is_event_attr:
                # Event attribute that is a list -> vector<T>
                return {attr_name: f'vector_{base_type}'}
            else:
                # Particle attribute that is a list -> vector<vector<T>>
                return {attr_name: f'vector_vector_{base_type}'}
    
    else:
        # Scalar value
        base_type = _python_type_to_base_type(representative)
        if base_type is None:
            return {}
        
        if is_event_attr:
            # Event scalar
            return {attr_name: base_type}
        else:
            # Particle scalar -> stored as vector
            return {attr_name: f'vector_{base_type}'}


def _create_branch_holder(type_code: str) -> Any:
    """
    Create ROOT branch holder for given type code.
    
    :param type_code: Type code (e.g., 'int', 'vector_int', 'vector_vector_int', 'string')
    :return: Branch holder object
    """
    # Scalar types
    if type_code in SCALAR_TYPES:
        array_type, _ = SCALAR_TYPES[type_code]
        return array.array(array_type, [0])
    
    # String scalar
    if type_code == 'string':
        return r.std.string()
    
    # Vector types
    if type_code in VECTOR_TYPES:
        return VECTOR_TYPES[type_code]()
    
    # Vector-of-vector types
    if type_code in VECTOR_VECTOR_TYPES:
        return VECTOR_VECTOR_TYPES[type_code]()
    
    return None


def _create_vector_for_values(values: List[Any]) -> Any:
    """
    Create and fill a ROOT vector from Python values.
    
    :param values: Python values to store
    :return: Filled ROOT vector
    """
    if not values:
        return r.std.vector('double')()
    
    base_type = _python_type_to_base_type(values[0])
    if base_type is None:
        base_type = 'double'
    
    type_code = f'vector_{base_type}'
    if type_code in VECTOR_TYPES:
        vec = VECTOR_TYPES[type_code]()
        for val in values:
            vec.push_back(val)
        return vec
    
    return r.std.vector('double')()


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


def _get_particle_branch_structure(particles: List[Particle]) -> Dict[str, str]:
    """
    Analyze a list of particles to determine the branch structure needed for ROOT.
    
    :param particles: List of particles to analyze
    :return: Dictionary mapping attribute names to type codes
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
        
        if representative is None:
            # All samples were None or empty lists - default to vector<int>
            branch_types[attr_name] = 'vector_int'
        else:
            branch_types.update(_determine_branch_type(attr_name, representative, is_event_attr=False))
    
    return branch_types


def _create_branches(tree: r.TTree, sample_events: List[Event], particle_types: List[str]) -> Dict[str, Tuple[Any, str]]:
    """
    Create branches in the ROOT TTree based on event structure.
    
    :param tree: ROOT TTree object
    :param sample_events: List of sample events to determine structure
    :param particle_types: List of particle type names to include
    :return: Dictionary of branch holders and their type codes for filling
    """
    branch_holders = {}
    
    # Event-level branches (non-particle attributes) - use first event
    first_event = sample_events[0]
    for attr_name, attr_value in first_event.__dict__.items():
        if attr_name.startswith('_'):
            continue
        
        # Determine type using unified function
        branch_types = _determine_branch_type(attr_name, attr_value, is_event_attr=True)
        
        for branch_key, type_code in branch_types.items():
            branch_name = EVENT_PREFIX + branch_key
            holder = _create_branch_holder(type_code)
            
            if holder is not None:
                branch_holders[branch_name] = (holder, type_code)
                
                # Create branch with appropriate descriptor
                if type_code in SCALAR_TYPES:
                    _, descriptor = SCALAR_TYPES[type_code]
                    # For scalars: use branch name (not branch_key) in descriptor
                    tree.Branch(branch_name, holder, f'{branch_name}/{descriptor}')
                elif type_code == 'string':
                    tree.Branch(branch_name, holder)
                else:
                    # Vector types
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
        
        # Number of particles of this type (scalar per event)
        n_branch_name = f'{singular_name}_n{singular_name.capitalize()}'
        n_holder = _create_branch_holder('int')
        branch_holders[n_branch_name] = (n_holder, 'int')
        _, descriptor = SCALAR_TYPES['int']
        tree.Branch(n_branch_name, n_holder, f'{n_branch_name}/{descriptor}')
        
        # Get branch structure from collected particles
        branch_structure = _get_particle_branch_structure(all_particles)
        
        # Create branches for each attribute
        for attr_name, type_code in branch_structure.items():
            branch_name = f'{singular_name}_{attr_name}'
            holder = _create_branch_holder(type_code)
            if holder is not None:
                branch_holders[branch_name] = (holder, type_code)
                tree.Branch(branch_name, holder)
    
    return branch_holders


def _fill_scalar_attribute(holder: Any, type_code: str, value: Any) -> None:
    """
    Fill scalar value into branch holder.
    
    :param holder: Branch holder (array.array or std::string)
    :param type_code: Type code
    :param value: Scalar value to fill
    """
    if type_code == 'string':
        holder.assign(str(value))
    elif type_code in SCALAR_TYPES:
        holder[0] = value


def _fill_vector_attribute(holder: Any, values: List[Any]) -> None:
    """
    Fill vector attribute into branch holder.
    
    :param holder: Vector branch holder
    :param values: List of values to fill
    """
    holder.clear()
    for val in values:
        holder.push_back(val)


def _fill_vector_vector_attribute(holder: Any, list_of_lists: List[List[Any]]) -> None:
    """
    Fill vector<vector> attribute into branch holder.
    
    :param holder: Vector<vector> branch holder
    :param list_of_lists: List of lists to fill
    """
    holder.clear()
    for sublist in list_of_lists:
        vec = _create_vector_for_values(sublist)
        holder.push_back(vec)


def _fill_particle_branches(branch_holders: Dict[str, Tuple[Any, str]], ptype: str, particles: List[Particle]) -> None:
    """
    Fill all branches for particles of given type.
    
    :param branch_holders: Dictionary of branch holders and type codes
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
            
            # Handle particle references
            if isinstance(attr_value, Particle):
                # Single particle reference
                if index_key in branch_holders:
                    holder, _ = branch_holders[index_key]
                    holder.push_back(attr_value.index)
                if type_key in branch_holders:
                    holder, _ = branch_holders[type_key]
                    holder.push_back(attr_value.name)
            
            elif isinstance(attr_value, list) and len(attr_value) > 0 and isinstance(attr_value[0], Particle):
                # List of particle references
                if index_key in branch_holders:
                    holder, _ = branch_holders[index_key]
                    vec_indices = r.std.vector('int')()
                    for p in attr_value:
                        vec_indices.push_back(p.index)
                    holder.push_back(vec_indices)
                if type_key in branch_holders:
                    holder, type_code = branch_holders[type_key]
                    particle_type_name = attr_value[0].name if attr_value else ""
                    holder.push_back(particle_type_name)
            
            elif branch_name in branch_holders:
                holder, type_code = branch_holders[branch_name]
                
                # Dispatch based on type
                if type_code.startswith('vector_vector_'):
                    # Particle attribute that is a list (rare case where particle has list attribute)
                    # For now, treat as empty - this is edge case
                    pass
                elif type_code.startswith('vector_'):
                    # Regular particle scalar attribute
                    holder.push_back(attr_value)


def _fill_branches(branch_holders: Dict[str, Tuple[Any, str]], event: Event, particle_types: List[str]) -> None:
    """
    Fill branch holders with event data.
    
    :param branch_holders: Dictionary of branch holders and type codes
    :param event: Event to dump
    :param particle_types: List of particle type names
    """
    # Fill event-level branches
    for attr_name, attr_value in event.__dict__.items():
        if attr_name.startswith('_'):
            continue
        
        branch_name = EVENT_PREFIX + attr_name
        
        # Handle potential multi-branch attributes (particle references)
        index_key = branch_name + INDEX_SUFFIX
        type_key = branch_name + TYPE_SUFFIX
        
        if isinstance(attr_value, Particle):
            # Event has single particle reference
            if index_key in branch_holders:
                holder, type_code = branch_holders[index_key]
                _fill_scalar_attribute(holder, type_code, attr_value.index)
            if type_key in branch_holders:
                holder, type_code = branch_holders[type_key]
                _fill_scalar_attribute(holder, type_code, attr_value.name)
        
        elif isinstance(attr_value, list) and len(attr_value) > 0 and isinstance(attr_value[0], Particle):
            # Event has list of particle references
            if index_key in branch_holders:
                holder, type_code = branch_holders[index_key]
                holder.clear()
                for p in attr_value:
                    holder.push_back(p.index)
            if type_key in branch_holders:
                holder, type_code = branch_holders[type_key]
                _fill_scalar_attribute(holder, type_code, attr_value[0].name if attr_value else "")
        
        elif branch_name in branch_holders:
            holder, type_code = branch_holders[branch_name]
            
            if type_code in SCALAR_TYPES or type_code == 'string':
                # Scalar attribute
                _fill_scalar_attribute(holder, type_code, attr_value)
            elif type_code.startswith('vector_') and not type_code.startswith('vector_vector_'):
                # Event vector attribute
                _fill_vector_attribute(holder, attr_value)
            elif type_code.startswith('vector_vector_'):
                # Event vector-of-vector attribute (rare)
                _fill_vector_vector_attribute(holder, attr_value)
    
    # Fill particle-level branches
    for ptype in particle_types:
        particles = getattr(event, ptype, [])
        
        # Fill number of particles (scalar)
        singular_name = singularize_pname(ptype)
        n_key = f'{singular_name}_n{singular_name.capitalize()}'
        if n_key in branch_holders:
            holder, type_code = branch_holders[n_key]
            _fill_scalar_attribute(holder, type_code, len(particles))
        
        # Clear all particle attribute vectors for this type
        for branch_name, (holder, type_code) in branch_holders.items():
            if branch_name.startswith(f'{singular_name}_') and branch_name != n_key:
                if hasattr(holder, 'clear'):
                    holder.clear()
        
        # Fill particle attributes
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
    - A vector of indices: {ptype}_{attr_name}_Idx
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
    color_msg(f"Running event dumping (v2)...", "green")
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
        tag="_test_v2",
        maxfiles=1,
        maxevents=10,
        tree_name="EVENTS",
        particle_types=None,  # Dump all particle types
    )
