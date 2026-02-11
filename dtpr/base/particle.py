from ..utils.functions import color_msg, build_attribute_from_config
from copy import deepcopy


class Particle:
    """
    Base class to represent a particle.

    Attributes
    ----------
    index : int
        The index of the particle.
    name : str
        The name of the particle class. By default, it is the class name.
    """

    def __init__(self, index=None, ev=None, **kwargs):
        """
        Initialize a Particle instance.

        :param index: The index of the particle.
        :type index: int
        :param ev: The TTree event entry containing event data.
        :param branches: The branches to be used to define attributes.
        :type branches: dict
        :param kwargs: Additional attributes to set explicitly.
        """
        self.index = index
        self.name = self.__class__.__name__  # By default, it is the class name

        for key, value in kwargs.items():
            if isinstance(value, dict):
                value = build_attribute_from_config(key, value, instance=self, event=ev)
            else:
                value = deepcopy(value)
            setattr(self, key, value)

    def __str__(self, indentLevel=0, include=None, exclude=None, **kwargs):
        """
        Generate a string representation of the Particle instance.

        :param indentLevel: The indentation level for the summary.
        :type indentLevel: int
        :param include: List of property names to include in the summary. If None, include all.
        :type include: list or None
        :param exclude: List of property names to exclude from the summary. If None, exclude none.
        :type exclude: list or None
        :param kwargs: Additional keyword arguments to pass to the ``color_msg`` function.
        :return: The particle summary.
        :rtype: str
        """
        summary = [
            color_msg(
                f"{self.name} {self.index} info -->",
                color=kwargs.pop("color", "yellow"),
                indentLevel=indentLevel,
                return_str=True,
                **kwargs,
            )
        ]

        properties = {
            key: value
            for key, value in self.__dict__.items()
            if not key in {"index", "name"}
            and (include is None or key in include)
            and (exclude is None or key not in exclude)
        }

        summary.append(
            color_msg(
                ", ".join([f"{key.capitalize()}: {value}" for key, value in properties.items()]),
                indentLevel=indentLevel + 1,
                return_str=True,
            )
        )
        return "\n".join(summary)

    def __eq__(self, other):
        """
        Compare two Particle instances for equality, excluding the index and name attribute.

        :param other: The other Particle instance to compare.
        :type other: Particle
        :return: True if the instances are equal (excluding index and name), False otherwise.
        :rtype: bool
        """
        if not isinstance(other, Particle):
            return NotImplemented

        # Compare all attributes except 'index' and 'name'
        return {
            key: value for key, value in self.__dict__.items() if key not in {"index", "name"}
        } == {key: value for key, value in other.__dict__.items() if key not in {"index", "name"}}

    def __hash__(self):
        """
        Generate a hash for the Particle instance based on its attributes, excluding index and name.

        :return: The hash value of the Particle instance.
        :rtype: int
        """
        return hash(
            frozenset(
                (key, value) for key, value in self.__dict__.items() if key not in {"index", "name"}
            )
        )


if __name__ == "__main__":
    # case 1 : Directly set attributes
    particle = Particle(
        index=0, wh=-2, sc=1, st=1, detector_side={"expr": "'+z' if wh > 0 else '-z'"}
    )
    print(particle)
    # case 2 : Set attributes from a TTree event entry. Input file is a dt ntuple here
    import os

    # Example usage
    input_file = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "../../tests/ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root",
        )
    )
    from ROOT import TFile

    # first, create a TFile object to read the dt ntuple (this is not necessary by using NTuple Class)
    attributes = {
        "pt": {"branch": "gen_pt"},
        "eta": {"branch": "gen_eta"},
        "phi": {"branch": "gen_phi"},
        "charge": {"branch": "gen_charge"},
    }
    with TFile(input_file, "read") as ntuple:
        tree = ntuple["dtNtupleProducer/DTTREE;1"]
        for iev, ev in enumerate(tree):
            # in events of this ntuple, each event contains more o less 2 genmuons,
            # so, set index 0 or 1 allows to take properties of the first or second genmuon
            particle = Particle(index=0, ev=ev, name="GenMuon", **attributes)
            print(particle)
            break  # just to test the first event
