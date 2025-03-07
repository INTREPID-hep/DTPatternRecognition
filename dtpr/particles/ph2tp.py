from dtpr.particles import Particle

class Ph2TP(Particle):
    """
    A class representing a Phase2 Trigger Primitive (Ph2TP).

    Attributes
    ----------
    wh : int
        The wheel number of the Ph2TP.
    sc : int
        The sector number of the Ph2TP.
    st : int
        The station number of the Ph2TP.
    phi : int
        The phi of the Ph2TP.
    phiB : int
        The phiB of the Ph2TP.
    BX : int
        The bunch crossing number of the Ph2TP.
    quality : int
        The quality of the Ph2TP.
    rpcFlag : int
        The RPC flag of the Ph2TP.
    """
    def __init__(self, index, ev=None, **kwargs):
        """
        Initialize a Phase2 Trigger Primitive (Ph2TP) instance.

        A Phase2 Trigger Primitive can be initialized either by providing the root event entry (``ev``) or by passing each parameter individually.

        :param index: The index of the trigger primitive.
        :type index: int
        :param ev: The TTree event entry containing event data.
        :param kwargs: Additional attributes to set explicitly.
        """
        self.wh = kwargs.pop("wh", None)
        self.sc = kwargs.pop("sc", None)
        self.st = kwargs.pop("st", None)
        self.phi = kwargs.pop("phi", None)
        self.phiB = kwargs.pop("phiB", None)
        self.BX = kwargs.pop("BX", None)
        self.quality = kwargs.pop("quality", None)
        self.rpcFlag = kwargs.pop("rpcFlag", None)

        # Constants
        self.phires_conv = 65536.0 / 0.5
        self.matches = []
        self.matches_with_segment = False

        super().__init__(index, ev, **kwargs)

    def _init_from_ev(self, ev):
        """
        Properties taken from TBranches: {ph2TpgPhiEmuAm_wheel, ph2TpgPhiEmuAm_sector, 
        ph2TpgPhiEmuAm_station, ph2TpgPhiEmuAm_phi, ph2TpgPhiEmuAm_phiB, ph2TpgPhiEmuAm_BX,
        ph2TpgPhiEmuAm_quality, ph2TpgPhiEmuAm_rpcFlag}
        """
        self.wh = ev.ph2TpgPhiEmuAm_wheel[self.index]
        self.sc = ev.ph2TpgPhiEmuAm_sector[self.index]
        self.st = ev.ph2TpgPhiEmuAm_station[self.index]
        self.phi = ev.ph2TpgPhiEmuAm_phi[self.index]
        self.phiB = ev.ph2TpgPhiEmuAm_phiB[self.index]
        self.BX = ev.ph2TpgPhiEmuAm_BX[self.index] - 20  # Correct to center BX at 0
        self.quality = ev.ph2TpgPhiEmuAm_quality[self.index]
        self.rpcFlag = ev.ph2TpgPhiEmuAm_rpcFlag[self.index]


if __name__ == "__main__":
    ph2tpg = Ph2TP(0, wh=1, sc=1, st=1, phi=1, phiB=1, BX=1, quality=1, rpcFlag=1)
    print(ph2tpg)
