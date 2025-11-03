import os
import warnings
from .config import RUN_CONFIG
from .particle import Particle  # Import the base Particle class
from ..utils.functions import (
    color_msg,
    get_callable_from_src,
    format_event_attribute_str,
    format_event_particles_str,
)


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

    def __init__(self, ev=None, index=None, use_config=False, CONFIG=None):
        """
        Initialize an Event instance.

        :param ev: The ROOT TTree entry containing event data.
        :param index: The index of the event in the TTree iteration.
        :type index: int
        :param use_config: Flag to indicate whether to use the configuration file to build particles.
        :type use_config: bool
        """
        self.index = index
        self.number = index
        self._particles = {}  # Initialize an empty dictionary for particles
        CONFIG_ = CONFIG if CONFIG is not None else RUN_CONFIG
        if ev is not None:
            # Default to the index if the event number is not found
            self.number = getattr(ev, "event_eventNumber", self.number)

        if use_config and hasattr(CONFIG_, "particle_types"):
            for ptype, pinfo in getattr(CONFIG_, "particle_types", {}).items():
                self._build_particles(ev, ptype, pinfo)
        else:
            warnings.warn(
                "No particle types defined in the configuration file. Initializing an empty Event instance."
            )

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
        summary = [
            color_msg(
                f"------ Event {self.number} info ------",
                color="yellow",
                indentLevel=indentLevel,
                return_str=True,
            )
        ]
        summary.extend(
            format_event_attribute_str(key, val, indentLevel + 1)
            for key, val in self.__dict__.items()
            if key not in ["_particles", "number"]
        )
        for ptype, particles in self._particles.items():
            summary.extend(format_event_particles_str(ptype, particles, indentLevel + 1))
        return "\n".join(summary)

    def _build_particles(self, ev, ptype, pinfo):
        """
        Build particles of a specific type based on the information indicated in the config file.

        :param ev: The ROOT TTree entry containing event data.
        :param ptype: The type (name) of particles to build. It will be the attribute name in the
            Event instance.
        :param pinfo: The information dictionary for the particle type builder. It should contain
            the class builder path, the name of the branch to infer the number of particles, and
            optional conditions and sorting parameters.
        """
        # determine the number of particles to build
        _amount_attr = pinfo.get("amount", None)
        if _amount_attr is None:
            raise ValueError(
                f"Particle type {pinfo} does not specify an amount of instances to build."
            )
        else:
            if isinstance(_amount_attr, int):
                num_particles = _amount_attr
            elif isinstance(_amount_attr, str):
                _n = getattr(ev, _amount_attr, None)
                if _n is None:
                    raise ValueError(f"Branch {_amount_attr} not found in the event entry.")
                if isinstance(_n, int):
                    num_particles = _n
                else:
                    try:
                        num_particles = len(_n)
                    except Exception as e:
                        raise RuntimeError(
                            f"Amount of particles can be determined from {_amount_attr} branch: {e}"
                        )

        # determine the class to be used to build the particles
        if "class" in pinfo:
            ParticleClass = get_callable_from_src(pinfo["class"])
            if ParticleClass is None:
                raise ValueError(f"Particle class {pinfo['class']} wrongly defined.")
        else:
            ParticleClass = Particle  # Default to the base Particle class

        # Build the particles
        _particles = []
        for i in range(num_particles):
            _particle = ParticleClass(
                index=i,
                ev=ev,
                **pinfo.get("attributes", {}),
            )
            if _particle.name == "Particle":
                # If the name is not set, set it to the particle type
                _particle.name = ptype.capitalize()[:-1]

            if "filter" in pinfo:  # Only keep the particles that pass the filter, if defined
                filter_expr = pinfo["filter"]
                if not isinstance(filter_expr, str):
                    raise ValueError(
                        f"The 'filter' must be a string, got {type(filter_expr)} instead."
                    )
                try:
                    # Validate the filter expression by compiling it
                    compile(filter_expr, "<string>", "eval")
                except SyntaxError as e:
                    raise ValueError(f"Invalid filter expression: {filter_expr}. Error: {e}")

                if eval(filter_expr, {}, {"p": _particle, "ev": ev}):
                    _particles.append(_particle)
            else:
                _particles.append(_particle)

        if "sorter" in pinfo:  # Sort the particles if a sorter is defined
            sorter_info = pinfo["sorter"]
            if "by" not in sorter_info:
                raise ValueError(
                    f"Sorter information must contain 'by' key, got {sorter_info.keys()} instead."
                )
            key_expr = sorter_info["by"]
            if not isinstance(key_expr, str):
                raise ValueError(f"The sorter 'by' must be a string, got {type(key_expr)} instead.")
            try:
                # Validate the sorter expression by compiling it
                compile(key_expr, "<string>", "eval")
            except SyntaxError as e:
                raise ValueError(f"Invalid sorter expression: {key_expr}. Error: {e}")

            _particles = sorted(
                _particles,
                key=lambda p, ev=ev: eval(key_expr),
                reverse=sorter_info.get("reverse", False),
            )

        setattr(self, ptype, _particles)  # Add the particles to the Event instance

    def to_dict(self):
        """
        Generate a dictionary representation of the event. Useful to serialize it to, for example,
        awkward arrays.
        """
        dict_out = {key: val for key, val in self.__dict__.items() if key != "_particles"}
        for ptype, particles in self._particles.items():
            dict_out[ptype] = [p.__dict__ for p in particles]
        return dict_out

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
    # [start-example-1]
    event = Event(index=1)  # initialize an empty event with index 1
    print(event)

    # It is possible to manually add particles or other attributes
    from dtpr.base import Particle

    showers = [
        Particle(index=i, wh=1, sc=1, st=1, name="Shower") for i in range(5)
    ]  # create 5 showers
    event.showers = showers  # add them to the event

    print(event)
    print(event.showers[-1])
    # [end-example-1]
    # The event can be built from a TTree entry
    input_file = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "../../tests/ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root",
        )
    )
    cf_path = RUN_CONFIG.path
    # [start-example-2]
    from ROOT import TFile
    from dtpr.base.config import RUN_CONFIG

    # update the configuration file -> if it is not set, it will use the default one 'run_config.yaml'
    RUN_CONFIG.change_config_file(config_path=cf_path)

    # first, create a TFile object to read the dt ntuple (this is not necessary by using NTuple Class)
    with TFile(input_file, "read") as ntuple:
        tree = ntuple["dtNtupleProducer/DTTREE;1"]

        for iev, ev in enumerate(tree):
            # use_config=True to use the configuration file to build the particles
            event = Event(index=iev, ev=ev, use_config=True)

            # Print the event summary
            print(event)

            break  # break after the first event
