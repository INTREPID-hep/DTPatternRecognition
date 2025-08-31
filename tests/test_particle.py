import os
import pytest
from dtpr.base.particle import Particle
from numpy import array

class DummyEvent:
    gen_pt = array([10.0, 20.0])
    gen_eta = array([1.1, -1.2])
    gen_phi = array([0.5, -0.5])
    gen_charge = array([1, -1])

def test_particle_direct_attributes():
    p = Particle(index=0, wh=-2, sc=1, st=1)
    assert p.index == 0
    assert p.wh == -2
    assert p.sc == 1
    assert p.st == 1
    assert p.name == "Particle"

def test_particle_expr_attribute():
    p = Particle(index=0, wh=2, detector_side={"expr": "'+z' if wh > 0 else '-z'"})
    assert hasattr(p, "detector_side")
    assert p.detector_side == "+z"

def test_particle_init_from_dict_branch():
    ev = DummyEvent()
    attributes = {
        'pt': {'branch': 'gen_pt'},
        'eta': {'branch': 'gen_eta'},
        'phi': {'branch': 'gen_phi'},
        'charge': {'branch': 'gen_charge'},
    }
    p = Particle(index=1, ev=ev, **attributes)
    print(p)
    assert p.pt == ev.gen_pt[1]
    assert p.eta == ev.gen_eta[1]
    assert p.phi == ev.gen_phi[1]
    assert p.charge == ev.gen_charge[1]

def test_particle_equality_and_hash():
    p1 = Particle(index=0, wh=1, sc=2)
    p2 = Particle(index=1, wh=1, sc=2)
    p3 = Particle(index=2, wh=2, sc=2)
    assert p1 == p2
    assert p1 != p3
    assert hash(p1) == hash(p2)
    assert hash(p1) != hash(p3)

def test_particle_str():
    p = Particle(index=0, wh=1, sc=2)
    s = p.__str__()
    assert "Particle" in s
    assert "Wh" in s
    assert "Sc" in s

def test_particle_from_real_root_file():
    try:
        import ROOT
    except ImportError:
        pytest.skip("ROOT is not installed")

    # Path to your test ROOT file
    test_file = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root"
        )
    )
    if not os.path.exists(test_file):
        pytest.skip(f"Test ROOT file not found: {test_file}")

    attributes = {
        'pt': {'branch': 'gen_pt'},
        'eta': {'branch': 'gen_eta'},
        'phi': {'branch': 'gen_phi'},
        'charge': {'branch': 'gen_charge'},
    }
    from ROOT import TFile
    with TFile(test_file, "read") as ntuple:
        tree = ntuple["dtNtupleProducer/DTTREE;1"]
        for iev, ev in enumerate(tree):
            # In this ntuple, each event contains ~2 genmuons
            for idx in range(len(ev.gen_pt)):
                particle = Particle(index=idx, ev=ev, name="GenMuon", **attributes)
                # Validate values match those in the ROOT event
                assert particle.pt == ev.gen_pt[idx]
                assert particle.eta == ev.gen_eta[idx]
                assert particle.phi == ev.gen_phi[idx]
                assert particle.charge == ev.gen_charge[idx]
            break  # Only test the first event for speed