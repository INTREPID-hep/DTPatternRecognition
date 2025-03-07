
import numpy as np
from dtpr.particles import Particle

np.random.seed(1935)

class G4Digi(Particle):
    """
    A class representing a G4 digi.

    Attributes
    ----------
    wh : int
        The wheel number of the digi. Default is -2.
    sc : int
        The sector number of the digi. Default is 1.
    st : int
        The station number of the digi. Default is 1.
    sl : int
        The superlayer number of the digi.
    l : int
        The layer number of the digi.
    w : int
        The wire number of the digi.
    time : int
        The time of the digi.
    particle_type : int
        The particle type of the digi.
    BX : int
        The bunch crossing number of the digi.
    """
    def __init__(self, index, ev=None, **kwargs):
        """
        Initialize a G4Digi instance.

        A G4Digi can be initialized either by providing the root event entry (``ev``) or by passing each parameter individually.

        :param index: The index of the digi.
        :type index: int
        :param ev: The TTree event entry containing event data.
        :param kwargs: Additional attributes to set explicitly.
        """
        self.wh = kwargs.pop("wh", -2)
        self.sc = kwargs.pop("sc", 1)
        self.st = kwargs.pop("st", 1)
        self.sl = kwargs.pop("sl", None)
        self.w = kwargs.pop("w", None)
        self.l = kwargs.pop("l", None)
        self.time = kwargs.pop("time", None)
        self.particle_type = kwargs.pop("particle_type", None)

        super().__init__(index, ev, **kwargs)

        if self.time is not None:
            self.BX = self.time // 25

    def _init_from_ev(self, ev):
        """
        Properties taken from TBranches: {SLHit_SL, SLHit_Cell, SLHit_Layer, SLHit_Time, SLHit_PDG}

        :param ev: The TTree event entry containing event data.
        """
        self.sl = int(ev.SLHit_SL[self.index])
        self.w = int(ev.SLHit_Cell[self.index])
        self.l = int(ev.SLHit_Layer[self.index])
        self.time = ev.SLHit_Time[self.index]
        self._correct_time()
        self.particle_type = int(ev.SLHit_PDG[self.index])

    def _correct_time(self):
        """
        Correct the time of the digi by simulating the drift time.
        """
        # ----- mimic the Javi's Code ----
        # simulate drift time
        mean, stddev = 175, 75
        time_offset = 400
        delay = np.random.normal(loc=mean, scale=stddev)
        self.time += abs(delay) + time_offset # why abs ? 


if __name__ == '__main__':
    # Test the class here
    digi = G4Digi(1)
    print(digi)