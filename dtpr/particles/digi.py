from dtpr.particles import Particle

class Digi(Particle):
    """
    A class representing a digi.
    
    Attributes
    ----------
    wh : int
        The wheel number of the digi.
    sc : int
        The sector number of the digi.
    st : int
        The station number of the digi.
    sl : int
        The superlayer number of the digi.
    l : int
        The layer number of the digi.
    w : int
        The wire number of the digi.
    time : int
        The time of the digi.
    BX : int
        The bunch crossing number of the digi.
    """

    def __init__(self, index, ev=None, **kwargs):
        """
        Initialize a Digi instance.

        A Digi can be initialized either by providing the TTree event entry (``ev``) or by passing each parameter individually.

        :param index: The index of the digi.
        :type index: int
        :param ev: The TTree event object containing event data.
        :param kwargs: Additional attributes to set explicitly.
        """
        # Digi will have at least the following attributes
        self.wh = kwargs.pop("wh", None)
        self.sc = kwargs.pop("sc", None)
        self.st = kwargs.pop("st", None)
        self.sl = kwargs.pop("sl", None)
        self.l = kwargs.pop("l", None)
        self.w = kwargs.pop("w", None)
        self.time = kwargs.pop("time", None)

        super().__init__(index, ev, **kwargs)

        # define BX based on digi time
        self.BX = self.time // 25 if self.time is not None else None  # each BX is at 25ns

    def _init_from_ev(self, ev):
        """
        Properties taken from TBranches: {digi_wheel, digi_sector, digi_station, digi_superLayer, digi_layer, digi_wire, digi_time}

        :param ev: The TTree event entry containing event data.
        """

        self.wh = ev.digi_wheel[self.index]
        self.sc = ev.digi_sector[self.index]
        self.st = ev.digi_station[self.index]
        self.sl = int(ev.digi_superLayer[self.index])
        self.w = int(ev.digi_wire[self.index])
        self.l = int(ev.digi_layer[self.index])
        self.time = int(ev.digi_time[self.index]) 

if __name__ == "__main__":
    digi = Digi(index=0, wh=1, sc=2, st=3, sl=4, l=5, w=6, time=7)
    print(digi)