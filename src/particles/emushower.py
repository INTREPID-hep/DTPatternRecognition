from dtpr.utils.functions import color_msg


class EmuShower(object):
    __slots__ = [
        "index",
        "wh",
        "sc",
        "st",
        "BX",
        "nDigis",
        "avg_pos",
        "avg_time",
        "_others",
    ]

    def __init__(
        self,
        iShower,
        ev=None,
        wh=-2,
        sc=1,
        st=1,
        nDigis=0,
        BX=0,
        min_wire=0,
        max_wire=0,
        avg_pos=0,
        avg_time=0,
    ):
        self.index = iShower
        self._others = {}
        if ev is not None:  # constructor with root_event info
            self.wh = ev.ph2Shower_wheel[iShower]
            self.sc = ev.ph2Shower_sector[iShower]
            self.st = ev.ph2Shower_station[iShower]
            self.nDigis = ev.ph2Shower_ndigis[iShower]
            self.BX = ev.ph2Shower_BX[iShower]
            self.min_wire = ev.ph2Shower_min_wire[iShower]
            self.max_wire = ev.ph2Shower_max_wire[iShower]
            self.avg_pos = ev.ph2Shower_avg_pos[iShower]
            self.avg_time = ev.ph2Shower_avg_time[iShower]

        else:  # constructor with explicit info
            self.wh = wh
            self.sc = sc
            self.st = st
            self.nDigis = nDigis
            self.BX = BX
            self.min_wire = min_wire
            self.max_wire = max_wire
            self.avg_pos = avg_pos
            self.avg_time = avg_time


    def to_dict(self):
        """
        Convert the Shower instance to a dictionary.

        :return: A dictionary representation of the Shower instance.
        :rtype: dict
        """
        _dict = {attr: getattr(self, attr) for attr in self.__slots__} 
        return {**_dict, **self._others}

    def __str__(self, indentLevel=0):
        """
        Generate a string representation of the Shower instance.

        :param indentLevel: The indentation level for the summary.
        :type indentLevel: int
        :return: The shower summary.
        :rtype: str
        """
        summary = [
            color_msg(
                f"------ EmuShower {self.index} info ------",
                color="yellow",
                indentLevel=indentLevel,
                return_str=True,
            )
        ]

        summary.append(
            color_msg(
                ", ".join(
                    [
                        f"{attr.capitalize()}: {getattr(self, attr)}"
                        for attr in self.__slots__
                        if attr != "index"
                    ]
                ),
                indentLevel=indentLevel + 1,
                return_str=True,
            )
        )
        return "\n".join(summary)
    
    def __getattr__(self, name):
        """
        Override __getattr__ to return attributes from the _others dictionary.

        :param name: The name of the attribute.
        :return: The attribute value.
        :raises AttributeError: If the attribute is not found.
        """
        if name in self.__slots__:
            return object.__getattribute__(self, name)
        if name in self._others:
            return self._others[name]
        raise AttributeError(f"'Shower' object has no attribute '{name}'")

    def __setattr__(self, name, value):
        """
        Override __setattr__ to store aditional attributes in the _others dictionary.

        :param name: The name of the attribute.
        :param value: The value to set.
        """
        if name in self.__slots__:
            object.__setattr__(self, name, value)
        else:
            self._others[name] = value
