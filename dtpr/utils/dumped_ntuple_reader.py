"""
Utility functions to read dumped ntuples and reconstruct particle relationships.

This module provides helper functions to read ntuples created by dump_events.py
and reconstruct the Event and Particle objects with their relationships intact.
"""

import ROOT as r
from typing import Dict, List, Any, Optional
from dtpr.base import Event, Particle
from dtpr.utils.functions import color_msg


class DumpedEventReader:
    """
    Reader for dumped event ntuples that reconstructs Event and Particle objects.
    
    This class handles reading flat ROOT ntuples created by dump_events and
    reconstructs the hierarchical Event/Particle structure, including particle
    references (matched_something attributes).
    """
    
    def __init__(self, file_path: str, tree_name: str = "EVENTS"):
        """
        Initialize the reader.
        
        :param file_path: Path to the dumped ROOT file
        :param tree_name: Name of the TTree to read
        """
        self.file_path = file_path
        self.tree_name = tree_name
        self.file = r.TFile.Open(file_path, "READ")
        
        if not self.file or self.file.IsZombie():
            raise IOError(f"Cannot open file: {file_path}")
        
        self.tree = self.file.Get(tree_name)
        if not self.tree:
            raise ValueError(f"Cannot find tree '{tree_name}' in file {file_path}")
        
        # Discover structure from branch names
        self._discover_structure()
    
    def _discover_structure(self):
        """Discover the event and particle structure from branch names."""
        self.event_branches = []
        self.particle_types = set()
        self.particle_branches = {}  # {particle_type: {attr_name: branch_name}}
        
        branches = self.tree.GetListOfBranches()
        
        for i in range(branches.GetEntries()):
            branch_name = branches.At(i).GetName()
            
            # Check if it's a particle count branch (n_particles)
            if branch_name.startswith('n_'):
                ptype = branch_name[2:]
                self.particle_types.add(ptype)
                self.particle_branches[ptype] = {}
            # Check if it's a particle attribute branch
            else:
                has_ptype = False
                for ptype in list(self.particle_types):
                    if branch_name.startswith(f'{ptype}_'):
                        attr_name = branch_name[len(ptype)+1:]
                        self.particle_branches[ptype][attr_name] = branch_name
                        has_ptype = True
                        break
                
                if not has_ptype:
                    # Try to detect new particle types from branch names
                    parts = branch_name.split('_', 1)
                    if len(parts) == 2:
                        potential_ptype = parts[0]
                        # Check if we have n_{potential_ptype} branch
                        if any(b.GetName() == f'n_{potential_ptype}' 
                               for b in [branches.At(j) for j in range(branches.GetEntries())]):
                            self.particle_types.add(potential_ptype)
                            if potential_ptype not in self.particle_branches:
                                self.particle_branches[potential_ptype] = {}
                            self.particle_branches[potential_ptype][parts[1]] = branch_name
                        else:
                            # Event-level branch
                            self.event_branches.append(branch_name)
                    else:
                        # Event-level branch
                        self.event_branches.append(branch_name)
        
        color_msg(f"Discovered particle types: {', '.join(self.particle_types)}", "yellow")
    
    def read_event(self, entry: int) -> Optional[Event]:
        """
        Read a specific event entry and reconstruct the Event object.
        
        :param entry: Entry number in the tree
        :return: Reconstructed Event object or None if entry is invalid
        """
        if entry < 0 or entry >= self.tree.GetEntries():
            return None
        
        self.tree.GetEntry(entry)
        
        # Create empty event
        event = Event(index=entry, use_config=False)
        
        # Fill event-level attributes
        for branch_name in self.event_branches:
            branch = self.tree.GetBranch(branch_name)
            if branch:
                leaf = branch.GetLeaf(branch_name)
                if leaf:
                    value = leaf.GetValue()
                    setattr(event, branch_name, int(value) if leaf.GetTypeName() in ['Int_t', 'int'] else value)
        
        # Build particles for each type
        particle_objects = {}  # {ptype: [particles]}
        
        for ptype in self.particle_types:
            n_particles = int(getattr(self.tree, f'n_{ptype}'))
            particles = []
            
            for i in range(n_particles):
                particle = Particle(index=i)
                particle.name = ptype.capitalize()[:-1] if ptype.endswith('s') else ptype.capitalize()
                
                # Fill particle attributes
                for attr_name, branch_name in self.particle_branches[ptype].items():
                    if '_indices' in attr_name or '_type' in attr_name:
                        continue  # Handle these separately
                    
                    branch_value = getattr(self.tree, branch_name)
                    
                    # Handle different branch types
                    if hasattr(branch_value, 'size'):  # It's a vector
                        if hasattr(branch_value[i], '__iter__') and not isinstance(branch_value[i], str):
                            # Vector of vectors
                            value = list(branch_value[i])
                        else:
                            # Simple vector
                            value = branch_value[i]
                    else:
                        value = branch_value
                    
                    setattr(particle, attr_name, value)
                
                particles.append(particle)
            
            particle_objects[ptype] = particles
            setattr(event, ptype, particles)
        
        # Second pass: reconstruct particle references
        for ptype in self.particle_types:
            particles = particle_objects[ptype]
            
            for attr_name in self.particle_branches[ptype].keys():
                if attr_name.endswith('_indices'):
                    # This is a reference to other particles
                    base_attr = attr_name[:-8]  # Remove '_indices'
                    type_attr = f'{base_attr}_type'
                    
                    if type_attr not in self.particle_branches[ptype]:
                        continue
                    
                    indices_branch = f'{ptype}_{attr_name}'
                    type_branch = f'{ptype}_{type_attr}'
                    
                    indices_vec = getattr(self.tree, indices_branch)
                    type_vec = getattr(self.tree, type_branch)
                    
                    for i, particle in enumerate(particles):
                        if i < len(indices_vec) and i < len(type_vec):
                            ref_indices = list(indices_vec[i])
                            ref_type_str = str(type_vec[i])
                            
                            # Find the particle type that matches
                            ref_ptype = None
                            for pt in self.particle_types:
                                if particles_match_type(particle_objects.get(pt, []), ref_type_str):
                                    ref_ptype = pt
                                    break
                            
                            if ref_ptype:
                                ref_particles = []
                                for idx in ref_indices:
                                    if idx < len(particle_objects[ref_ptype]):
                                        ref_particles.append(particle_objects[ref_ptype][idx])
                                
                                setattr(particle, base_attr, ref_particles)
                
                elif attr_name.endswith('_index') and not attr_name.endswith('_indices'):
                    # Single particle reference
                    base_attr = attr_name[:-6]  # Remove '_index'
                    type_attr = f'{base_attr}_type'
                    
                    if type_attr not in self.particle_branches[ptype]:
                        continue
                    
                    index_branch = f'{ptype}_{attr_name}'
                    type_branch = f'{ptype}_{type_attr}'
                    
                    index_vec = getattr(self.tree, index_branch)
                    type_vec = getattr(self.tree, type_branch)
                    
                    for i, particle in enumerate(particles):
                        if i < len(index_vec) and i < len(type_vec):
                            ref_index = int(index_vec[i])
                            ref_type_str = str(type_vec[i])
                            
                            # Find matching particle type
                            ref_ptype = None
                            for pt in self.particle_types:
                                if particles_match_type(particle_objects.get(pt, []), ref_type_str):
                                    ref_ptype = pt
                                    break
                            
                            if ref_ptype and ref_index < len(particle_objects[ref_ptype]):
                                setattr(particle, base_attr, particle_objects[ref_ptype][ref_index])
        
        return event
    
    def __iter__(self):
        """Iterate over all events in the tree."""
        for i in range(self.tree.GetEntries()):
            yield self.read_event(i)
    
    def __len__(self):
        """Return the number of events."""
        return self.tree.GetEntries()
    
    def __getitem__(self, index):
        """Access event by index."""
        if isinstance(index, int):
            return self.read_event(index)
        elif isinstance(index, slice):
            start, stop, step = index.indices(len(self))
            return [self.read_event(i) for i in range(start, stop, step)]
        else:
            raise TypeError("Index must be an integer or slice")
    
    def close(self):
        """Close the ROOT file."""
        if self.file:
            self.file.Close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


def particles_match_type(particles: List[Particle], type_name: str) -> bool:
    """
    Check if particles match the given type name.
    
    :param particles: List of particles to check
    :param type_name: Type name to match
    :return: True if particles match the type
    """
    if not particles:
        return False
    return particles[0].name == type_name


if __name__ == "__main__":
    """Example usage of the DumpedEventReader."""
    import os
    
    # Example: Read dumped events
    dumped_file = "../../results/dumped_events_test.root"
    
    if os.path.exists(dumped_file):
        color_msg(f"Reading dumped events from: {dumped_file}", "green")
        
        with DumpedEventReader(dumped_file, tree_name="EVENTS") as reader:
            color_msg(f"Total events in file: {len(reader)}", "yellow")
            
            # Read first event
            event = reader[0]
            if event:
                print(event)
            
            # Iterate over first 3 events
            for i, event in enumerate(reader):
                if i >= 3:
                    break
                print(f"\n--- Event {i} ---")
                print(event)
    else:
        color_msg(f"Dumped file not found: {dumped_file}", "red")
        color_msg("Please run dump_events first to create a dumped ntuple", "yellow")
