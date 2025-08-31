import os
import pytest
from dtpr.base.ntuple import NTuple

def test_ntuple_with_real_root_file():
    # Path to your test ROOT file
    test_file = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root"
        )
    )
    assert os.path.exists(test_file), f"Test ROOT file not found: {test_file}"

    # Create the NTuple instance
    ntuple = NTuple(test_file)
    # Check that events are loaded
    assert hasattr(ntuple, "events")
    # Try to access the first event (if the file is not empty)
    if len(ntuple.events) > 0:
        ev = ntuple.events[0]
        assert ev is not None
        assert hasattr(ev, "index")
        assert isinstance(ev.index, int)