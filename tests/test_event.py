import os
import pytest
from dtpr.base.event import Event
from dtpr.base.particle import Particle

# ---------- UNIT TESTS WITH SYNTHETIC DATA ----------

def test_event_empty():
    event = Event(index=5)
    assert event.index == 5
    assert event.number == 5
    assert isinstance(event._particles, dict)
    assert len(event._particles) == 0

def test_event_manual_particles():
    event = Event(index=1)
    showers = [Particle(index=i, wh=1, sc=1, st=1, name="Shower") for i in range(3)]
    event.showers = showers
    assert hasattr(event, "showers")
    assert event.showers == showers
    assert event._particles["showers"] == showers

def test_event_getattr_setattr():
    event = Event(index=2)
    muons = [Particle(index=0, name="Muon"), Particle(index=1, name="Muon")]
    event.muons = muons
    assert event.muons == muons
    assert event._particles["muons"] == muons

def test_event_str():
    event = Event(index=3)
    event.tracks = [Particle(index=0, name="Track")]
    s = event.__str__()
    assert "Event" in s
    assert "tracks" in s or "Tracks" in s

def test_event_to_dict():
    event = Event(index=4)
    event.electrons = [Particle(index=0, name="Electron")]
    d = event.to_dict()
    assert "electrons" in d
    assert isinstance(d["electrons"], list)
    assert isinstance(d["electrons"][0], dict)
    assert d["index"] == 4

def test_event_filter_particles():
    event = Event(index=6)
    event.digis = [
        Particle(index=0, wh=1, sc=2, st=3, name="Digi"),
        Particle(index=1, wh=1, sc=2, st=4, name="Digi"),
        Particle(index=2, wh=2, sc=2, st=3, name="Digi"),
    ]
    filtered = event.filter_particles("digis", wh=1, sc=2)
    assert len(filtered) == 2
    assert all(p.wh == 1 and p.sc == 2 for p in filtered)

def test_event_filter_particles_invalid_type():
    event = Event(index=7)
    result = event.filter_particles("notype", wh=1)
    assert result == []

def test_event_filter_particles_invalid_key():
    event = Event(index=8)
    event.digis = [Particle(index=0, wh=1, name="Digi")]
    with pytest.raises(ValueError):
        event.filter_particles("digis", notakey=123)

# ---------- INTEGRATION TEST WITH REAL ROOT FILE ----------

def test_event_from_real_root_file():
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
        pytest.skip("Test ROOT file not found: {}".format(test_file))

    from dtpr.base.config import RUN_CONFIG

    with ROOT.TFile(test_file, "read") as ntuple:
        # Adjust the tree path as needed
        tree = ntuple["dtNtupleProducer/DTTREE;1"]
        for iev, ev in enumerate(tree):
            if ev is None:
                continue
            event = Event(index=iev, ev=ev, use_config=True)
            # Basic checks
            assert isinstance(event, Event)
            assert hasattr(event, "index")
            assert isinstance(event.index, int)
            # Check that all configured particle types are present as attributes
            for ptype in RUN_CONFIG.particle_types:
                assert hasattr(event, ptype), f"Event missing particle type: {ptype}"
                particles = getattr(event, ptype)
                # Should be a list (possibly empty)
                assert isinstance(particles, list)
                # If not empty, check that each is a Particle or subclass
                if particles:
                    from dtpr.base.particle import Particle
                    assert all(isinstance(p, Particle) for p in particles)
            break  # Only test the first event for speed