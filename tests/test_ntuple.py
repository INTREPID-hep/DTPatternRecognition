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
    ntuple = NTuple(test_file, tree_name="dtNtupleProducer/DTTREE")
    # ntuple.events is a dask_awkward.Array — compute divisions to enable len()
    assert hasattr(ntuple, "events")
    ntuple.events.eager_compute_divisions()
    assert len(ntuple.events) > 0
    # Access first event: index a dask array returns a dask Record; compute it
    ev = ntuple.events[0].compute()
    assert ev is not None
    assert hasattr(ev, "fields")