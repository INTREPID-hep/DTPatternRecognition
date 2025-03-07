import os
import warnings
from importlib import import_module
from dtpr.utils.config import RUN_CONFIG
from dtpr.utils.functions import color_msg
from dtpr.particles._particle import Particle  # Import the base Particle class

class Event:
    """
    Represents an event entry from a ROOT TTree, providing dynamic particle building and access to
    particle data. Supports filtering, sorting, and manual addition of particles for flexible event
    analysis.

    Attributes
    ----------
    index : int
        The index of the event in the TTree iteration.
    number : int
        The event number, if TBranch does not exist, it will be the same as the index.
    _particles : dict
        Internal dictionary storing particles by their type. This attribute is not intended for
        direct user access. Instead, users should access particles by their type name
        (e.g., `event.genmuons`).
    """
    def __init__(self, ev=None, index=None):
        """
        Initialize an Event instance.

        :param ev: The ROOT TTree entry containing event data.
        :param index: The index of the event in the TTree iteration.
        :type index: int
        """
        self.index = index
        self.number = index  # Default to the index if the event number is not found
        self._particles = {}  # Initialize an empty dictionary for particles
        if ev is not None:
            self._init_from_ev(ev)

    def __getattr__(self, name):
        """
        Override `__getattr__` to return particles from the `particles` and other attributes
        normally.

        :param name: The name of the attribute.
        :return: The list of particles if the name matches a particle type, or the attribute value.
        :raises AttributeError: If the attribute is not found and not a particle type.
        """
        if name in self._particles:
            return self._particles.get(name)
        raise AttributeError(f"'Event' object has no attribute '{name}'")

    def __setattr__(self, name, value):
        """
        Override `__setattr__` to store particle types in `_particles` and other attributes
        normally.

        :param name: The name of the attribute.
        :param value: The value to set for the attribute.
        """
        if isinstance(value, Particle):
            # If value is a single Particle instance, store it as a single-element list
            self._particles[name] = [value]
        elif isinstance(value, list) and all(isinstance(v, Particle) for v in value):
            # If value is a list of Particle instances, store it directly
            self._particles[name] = value
        else:
            # Otherwise, set the attribute normally
            super().__setattr__(name, value)

    def __str__(self, indentLevel=0):
        """
        Generate a summary string representation of the event.

        :param indentLevel: The indentation level for the summary.
        :return: The event summary.
        """
        def format_attribute(key, value, indent):
            return (
                color_msg(f"{key.capitalize()}:", color="green", indentLevel=indent, return_str=True)
                + color_msg(f"{value}", color="none", indentLevel=-1, return_str=True)
            )

        def format_particles(ptype, particles, indent):
            summary = [
                color_msg(f"{ptype.capitalize()}", color="green", indentLevel=indent, return_str=True),
                color_msg(
                    f"Number of {ptype}: {len(particles)}", color="purple", indentLevel=indent + 1,
                    return_str=True
                ),
            ]
            if ptype == "genmuons":
                summary.extend(
                    gm.__str__(indentLevel=indent + 1, color="green", exclude=["matches"])
                    for gm in particles
                )
            elif ptype == "segments":
                matches_segments = [seg for seg in particles if seg.matches]
                if matches_segments:
                    summary.append(
                        color_msg(
                            "AM-Seg matches:", color="purple", indentLevel=indent + 1, return_str=True
                        )
                    )
                    summary.extend(
                        seg.__str__(
                            indentLevel=indent + 2, color="purple", include=["wh", "sc", "st", "phi", "eta"]
                        )
                        for seg in matches_segments[:5]
                    )
            return summary

        summary = [
            color_msg(
                f"------ Event {self.number} info ------", color="yellow", indentLevel=indentLevel,
                return_str=True
            )
        ]
        for attrkey, attrval in {
            key: val for key, val in self.__dict__.items() if key not in ["_particles", "number"]
        }.items():
            summary.append(format_attribute(attrkey, attrval, indentLevel + 1))

        for ptype, particles in self._particles.items():
            summary.extend(format_particles(ptype, particles, indentLevel + 1))

        return "\n".join(summary)

    def to_dict(self):
        """
        Generate a dictionary representation of the event. Useful to serialize it to, for example,
        awkward arrays.
        """
        dict_out = {key: val for key, val in self.__dict__.items() if key != "_particles"}
        for ptype, particles in self._particles.items():
            dict_out[ptype] = [p.__dict__ for p in particles]
        return dict_out

    def _init_from_ev(self, ev):
        self.number = getattr(ev, "event_eventNumber", self.number)
        if hasattr(RUN_CONFIG, 'particle_types'):
            for ptype, pinfo in getattr(RUN_CONFIG, "particle_types", {}).items():
                self._build_particles(ev, ptype, pinfo)
        else:
            warnings.warn(
                "No particle types defined in the configuration file. Initializing an empty Event instance."
            )

    def _build_particles(self, ev, ptype, pinfo):
        """
        Build particles of a specific type based on the information of the TTree event entry.

        :param ev: The ROOT TTree entry containing event data.
        :param ptype: The type of particles to build. It will be the name of the attribute in the 
            Event instance.
        :param pinfo: The information dictionary for the particle type builder. It should contain 
            the class builder path, the name of the branch to infer the number of particles, and 
            optional conditions and sorting parameters.
        """
        try:
            module_name, class_name = pinfo["class"].rsplit(".", 1)
            module = import_module(module_name)
            ParticleClass = getattr(module, class_name)
        except AttributeError as e:
            raise AttributeError(f"{ptype} class not found: {e}")
        except ImportError as e:
            raise ImportError(f"Error importing {pinfo['class']}: {e}")

        try:
            num_particles = (
            n
            if isinstance(n := eval(f"ev.{pinfo['n_branch_name']}"), int)
            else len(n)
            )
        except AttributeError as e:
            raise AttributeError(f"Branch {pinfo['n_branch_name']} not found in the event entry or not included in the config file: {e}")
        except Exception as e:
            raise RuntimeError(f"Error evaluating branch {pinfo['n_branch_name']}: {e}")

        if "conditioner" in pinfo:
            conditioner = pinfo.get("conditioner", {})
            if "property" not in conditioner or "condition" not in conditioner:
                raise ValueError("Conditioner must have 'property' and 'condition'")
            particles = [
                ParticleClass(i, ev)
                for i in range(num_particles)
                if eval(
                    f"abs(ev.{conditioner['property']}[{i}]){conditioner['condition']}"
                )  # abs should not be hardcoded
            ]
        else:
            particles = [ParticleClass(i, ev) for i in range(num_particles)]

        if "sorter" in pinfo:
            sorter = pinfo.get("sorter", {})
            if "by" not in sorter:
                raise ValueError("Sorter must have 'by' attribute")
            particles = sorted(
                particles,
                key=lambda p: getattr(p, sorter["by"]),
                reverse=sorter["reverse"] if "reverse" in sorter else False,
            )

        setattr(self, ptype, particles)  # Add the particles to the Event instance

    def filter_particles(self, particle_type, **kwargs):
        """
        Filter all particles of a specific type that satisfy given attributes.

        :param particle_type: The type of particles to filter (e.g., 'digis', 'segments', 'tps').
        :param kwargs: Key-value pairs of attributes to filter by (e.g., wh=1, sc=2, st=3).
        :return: A list of filtered particles. If no particles are found, an empty list is returned.
        :raises ValueError: If invalid keys are provided for filtering.

        Example
        *******
        .. code-block:: python

            print("Amount of digis in the event:", len(event.digis))
            print("Amount of digis in wheel 1:", len(event.filter_particles("digis", wh=1)))

        Output
        ######
        .. code-block:: text

            Amount of digis in the event: 134
            Amount of digis in wheel 1: 1
        """
        if particle_type not in self._particles:
            warnings.warn(
                f"Invalid particle type {particle_type} to apply filter. Valid types are: {list(self._particles.keys())}"
            )
            return []

        particles = self._particles.get(particle_type, [])

        if not particles:
            return []  # Return an empty list if there are no particles

        valid_keys = set()
        for particle in particles:
            valid_keys.update(list(particle.__dict__.keys()))

        if not all(key in valid_keys for key in kwargs):
            raise ValueError(f"Invalid keys to filter. Valid keys are: {valid_keys}")

        def match(particle, kwargs):
            return all(getattr(particle, key) == value for key, value in kwargs.items())

        return [particle for particle in particles if match(particle, kwargs)]


if __name__ == "__main__":
    """
    Example of how to use the Event class to build particles and analyze them.
    """
    from ROOT import TFile
    from dtpr.particles import Shower

    # _maxevents = 1
    # with TFile(
    #     os.path.abspath(
    #         os.path.join(
    #             os.path.dirname(__file__),
    #             "../../test/ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root",
    #         )
    #     ),
    #     "read",
    # ) as ntuple:
    #     tree = ntuple["dtNtupleProducer/DTTREE;1"]

    #     for iev, ev in enumerate(tree):
    #         if iev >= _maxevents:
    #             break
    #         event = Event(ev=ev)
    #         # Print the event summary
    #         print(event)

    event = Event(index=0)
    print(event)

    showers = [Shower(index=i, wh=1, sc=1, st=1) for i in range(5)]
    event.showers = showers

    print(event)
    print(event.showers[-1])
