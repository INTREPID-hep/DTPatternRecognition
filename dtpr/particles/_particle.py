from abc import ABCMeta, abstractmethod
from dtpr.utils.functions import color_msg

class Particle(metaclass=ABCMeta):
    """
    Abstract base class for particles. metaclass=abc.ABCMeta.

    Attributes
    ----------
    index : int
        The index of the particle.
    """
    def __init__(self, index=None, ev=None, **kwargs):
        """
        Initialize a Particle instance.

        :param index: The index of the particle.
        :type index: int
        :param ev: The TTree event entry containing event data.
        :param kwargs: Additional attributes to set explicitly.
        """
        self.index = index        
        if ev is not None:
            # Initialize from root event if provided
            self._init_from_ev(ev)

        # Store any additional attributes
        for key, value in kwargs.items():
            # set any additional attributes
            setattr(self, key, value)

    @abstractmethod
    def _init_from_ev(self, ev):
        """
        Initialize the particle from the TTree event entry. It must be implemented in derived classes.
        """
        pass

    def __str__(self, indentLevel=0, include=None, exclude=None, **kwargs):
        """
        Generate a string representation of the DTParticle instance.

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
                f"{self.__class__.__name__} {self.index} info -->",
                color= kwargs.pop("color", "yellow"),
                indentLevel=indentLevel,
                return_str=True,
                **kwargs,
            )
        ]

        properties = {
            key: value
            for key, value in self.__dict__.items()
            if key != "index"
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
        Compare two Particle instances for equality, excluding the index attribute.

        :param other: The other Particle instance to compare.
        :type other: Particle
        :return: True if the instances are equal (excluding index), False otherwise.
        :rtype: bool
        """
        if not isinstance(other, Particle):
            return NotImplemented

        # Compare all attributes except 'index'
        return {
            key: value
            for key, value in self.__dict__.items()
            if key != "index"
        } == {
            key: value
            for key, value in other.__dict__.items()
            if key != "index"
        }

