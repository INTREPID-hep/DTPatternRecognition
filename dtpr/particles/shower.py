from dtpr.particles._particle import Particle

class Shower(Particle):
    """
    A class representing a DT Muon Shower.

    Attributes
    -----------
    wh : int
        The wheel number of the shower.
    sc : int
        The sector number of the shower.
    st : int
        The station number of the shower.
    sl : int
        The superlayer number of the shower.
    nDigis : int
        The number of digis in the shower.
    BX : int
        The bunch crossing number of the shower.
    min_wire : int
        The minimum wire number of the shower.
    max_wire : int
        The maximum wire number of the shower.
    avg_pos : float
        The average position of the shower.
    avg_time : float
        The average time of the shower.
    wires_profile : list
        The wires profile of the shower.
    """
    def __init__(self, index, ev=None, **kwargs):
        """
        Initialize a Shower instance.

        A Shower can be initialized either by providing the root event entry (``ev``) or by passing each parameter individually.

        :param index: The index of the shower.
        :type index: int
        :param ev: The TTree event entry containing event data.
        :param kwargs: Additional attributes to set explicitly.
        """
        self.wh = kwargs.pop("wh", None)
        self.sc = kwargs.pop("sc", None)
        self.st = kwargs.pop("st", None)
        self.sl = kwargs.pop("sl", None)
        self.nDigis = kwargs.pop("nDigis", None)
        self.BX = kwargs.pop("BX", None)
        self.min_wire = kwargs.pop("min_wire", None)
        self.max_wire = kwargs.pop("max_wire", None)
        self.avg_pos = kwargs.pop("avg_pos", None)
        self.avg_time = kwargs.pop("avg_time", None)
        self.wires_profile = kwargs.pop("wires_profile", [])

        super().__init__(index, ev, **kwargs)

    def _init_from_ev(self, ev):
        """
        Properties taken from TBranches: {ph2Shower_wheel, ph2Shower_sector, ph2Shower_station, 
        ph2Shower_superlayer, ph2Shower_ndigis, ph2Shower_BX, ph2Shower_min_wire, ph2Shower_max_wire,
        ph2Shower_avg_pos, ph2Shower_avg_time, ph2Shower_wires_profile}
        """
        self.wh = ev.ph2Shower_wheel[self.index]
        self.sc = ev.ph2Shower_sector[self.index]
        self.st = ev.ph2Shower_station[self.index]
        self.sl = ev.ph2Shower_superlayer[self.index]
        self.nDigis = ev.ph2Shower_ndigis[self.index]
        self.BX = ev.ph2Shower_BX[self.index]
        self.min_wire = ev.ph2Shower_min_wire[self.index]
        self.max_wire = ev.ph2Shower_max_wire[self.index]
        self.avg_pos = ev.ph2Shower_avg_pos[self.index]
        self.avg_time = ev.ph2Shower_avg_time[self.index]
        self.wires_profile = list(ev.ph2Shower_wires_profile[self.index])

if __name__ == "__main__":
    shower = Shower(index=0, wh=1, sc=2, st=3, sl=4, nDigis=5, BX=6, min_wire=7)
    print(shower)