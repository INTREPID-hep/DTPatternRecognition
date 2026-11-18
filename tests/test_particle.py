import os
import pytest
from dtpr.base.particle import Particle

class DummyEvent:
    gen_pt = [10.0, 20.0]
    gen_eta = [1.1, -1.2]
    gen_phi = [0.5, -0.5]
    gen_charge = [1, -1]


class DummyFlatEvent:
    tps_matched_segments_flat_ids = [10, 11, 12, 20, 21]
    tps_matched_segments_flat_counts = [2, 3]

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


def test_particle_init_from_flat_branch_pair():
    ev = DummyFlatEvent()
    particle = Particle(
        index=1,
        ev=ev,
        matched_segments={
            "branch": [
                "tps_matched_segments_flat_ids",
                "tps_matched_segments_flat_counts",
            ]
        },
    )

    assert particle.matched_segments == [12, 20, 21]

class FakeBranch:
    def GetEntry(self, entry):
        return 1


class FakeFlatTree:
    tps_matched_segments_flat_ids = [10, 11, 12, 20, 21, 30, 31, 32, 33]
    tps_matched_segments_flat_counts = [2, 3, 4]

    def LoadTree(self, entry):
        return entry

    def GetTreeNumber(self):
        return 0

    def GetBranch(self, name):
        return FakeBranch() if hasattr(self, name) else None


def test_flat_pair_uses_cumulative_offsets():
    from dtpr.base.entry_view import EntryView

    view = EntryView(FakeFlatTree())
    view.set_entry(0)

    counts_branch = "tps_matched_segments_flat_counts"
    assert view.cumulative_offsets(counts_branch) == [0, 2, 5, 9]
    assert view.cumulative_offsets(counts_branch) is view.cumulative_offsets(counts_branch)

    expected = {0: [10, 11], 1: [12, 20, 21], 2: [30, 31, 32, 33]}
    for index, values in expected.items():
        particle = Particle(
            index=index,
            ev=view,
            matched_segments={
                "branch": [
                    "tps_matched_segments_flat_ids",
                    "tps_matched_segments_flat_counts",
                ]
            },
        )
        assert particle.matched_segments == values


def test_materialize_keeps_plain_values():
    from dtpr.base.entry_view import materialize

    values = [10, 11, 12]
    assert materialize(values) is values, "a plain list must not be copied again"
    assert materialize("abc") == "abc", "a string branch must not be shredded"
    assert materialize(None) is None
    assert materialize(7) == 7


def test_literal_attributes_are_not_shared():
    p1 = Particle(index=0, matched_tps=[])
    p2 = Particle(index=1, matched_tps=[])
    assert p1.matched_tps is not p2.matched_tps
    p1.matched_tps.append(1)
    assert p2.matched_tps == []

    nested = Particle(index=2, profile=[[1, 2]])
    nested.profile[0].append(3)
    assert Particle(index=3, profile=[[1, 2]]).profile == [[1, 2]]


def test_expression_is_compiled_once():
    from dtpr.base.particle import compile_expression

    first = compile_expression("wh + 1")
    assert compile_expression("wh + 1") is first, "the code object must be reused"
    assert compile_expression("wh + 2") is not first


def test_invalid_expression_raises_value_error():
    with pytest.raises(ValueError):
        Particle(index=0, wh=1, broken={"expr": "wh +"})


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