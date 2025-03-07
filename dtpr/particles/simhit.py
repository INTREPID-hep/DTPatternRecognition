from dtpr.particles._particle import Particle


class SimHit(Particle):
    """
    A class representing a simhit.

    Attributes
    -----------
    wh : int
        The wheel number of the simhit.
    sc : int
        The sector number of the simhit.
    st : int
        The station number of the simhit.
    sl : int
        The superlayer number of the simhit.
    l : int
        The layer number of the simhit.
    w : int
        The wire number of the simhit.
    process_type : int
        The process type of the simhit.
    particle_type : int
        The particle type of the simhit.
    """

    def __init__(self, index, ev=None, **kwargs):
        """
        Initialize a SimHit instance.

        A SimHit can be initialized either by providing the root event entry (``ev``) or by passing each parameter individually.

        :param index: The index of the simhit.
        :type index: int
        :param ev: The TTree event entry containing event data.
        :param kwargs: Additional attributes to set explicitly.
        """
        self.wh = kwargs.pop("wh", None)
        self.sc = kwargs.pop("sc", None)
        self.st = kwargs.pop("st", None)
        self.sl = kwargs.pop("sl", None)
        self.l = kwargs.pop("l", None)
        self.w = kwargs.pop("w", None)
        self.process_type = kwargs.pop("process_type", None)
        self.particle_type = kwargs.pop("particle_type", None)

        super().__init__(index, ev, **kwargs)

    def _init_from_ev(self, ev):
        """
        Properties taken from TBranches: {simHit_wheel, simHit_sector, simHit_station, simHit_superLayer,
        simHit_layer, simHit_wire, simHit_processType, simHit_particleType}
        """
        self.wh = int(ev.simHit_wheel[self.index])
        self.sc = int(ev.simHit_sector[self.index])
        self.st = int(ev.simHit_station[self.index])
        self.sl = int(ev.simHit_superLayer[self.index])
        self.l = int(ev.simHit_layer[self.index])
        self.w = int(ev.simHit_wire[self.index])
        self.process_type = int(ev.simHit_processType[self.index])
        self.particle_type = int(ev.simHit_particleType[self.index])


if __name__ == "__main__":
    # Test the class here
    simhit = SimHit(1)

    print(simhit)
