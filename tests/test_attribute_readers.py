"""Unit tests for the attribute readers (dtpr/base/particle.py).

attribute_reader() turns one definition of the config into a small function and
Particle(index, ev, readers=...) fills a Particle with them. Both are checked
here against Particle(index, ev, **attributes), the general path, so a
difference names the form (branch, pair, expr, literal, cast) that is wrong.
"""

import os
import sys

import pytest

from dtpr.base.particle import Particle, attribute_reader

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)


class DummyEvent:
    gen_pt = [10.0, 20.0]
    gen_eta = [1.1, -1.2]
    gen_charge = [1, -1]
    digi_nDigis = 2


class DummyFlatEvent:
    tps_matched_segments_flat_ids = [10, 11, 12, 20, 21]
    tps_matched_segments_flat_counts = [2, 3]


def readers_of(attributes):
    return [(attr, attribute_reader(attr, spec)) for attr, spec in attributes.items()]


def build(attributes, index, ev):
    return Particle(index=index, ev=ev, readers=readers_of(attributes))


def test_branch_and_expression_match_particle():
    attributes = {
        "pt": {"branch": "gen_pt"},
        "eta": {"branch": "gen_eta"},
        "double_pt": {"expr": "pt * 2"},
    }
    ev = DummyEvent()
    for index in range(2):
        built = build(attributes, index, ev)
        expected = Particle(index=index, ev=ev, **attributes)
        assert built.pt == expected.pt
        assert built.eta == expected.eta
        assert built.double_pt == expected.double_pt
        assert built.index == index
        assert built.name == expected.name == "Particle"


def test_built_dictionary_has_the_same_keys_and_order():
    attributes = {"pt": {"branch": "gen_pt"}, "side": {"expr": "'+z' if pt > 15 else '-z'"}}
    ev = DummyEvent()

    built = build(attributes, 0, ev)
    expected = Particle(index=0, ev=ev, **attributes)
    assert list(built.__dict__) == list(expected.__dict__)
    assert built.side == expected.side


def test_expression_can_use_earlier_attributes():
    attributes = {"wh": {"branch": "gen_charge"}, "side": {"expr": "'+z' if wh > 0 else '-z'"}}
    ev = DummyEvent()

    assert build(attributes, 0, ev).side == "+z"
    assert build(attributes, 1, ev).side == "-z"


def test_flat_pair_matches_particle():
    attributes = {
        "matched_segments": {
            "branch": ["tps_matched_segments_flat_ids", "tps_matched_segments_flat_counts"]
        }
    }
    ev = DummyFlatEvent()

    for index in range(2):
        built = build(attributes, index, ev)
        expected = Particle(index=index, ev=ev, **attributes)
        assert built.matched_segments == expected.matched_segments


def test_flat_pair_out_of_bounds_raises_index_error():
    attributes = {
        "matched_segments": {
            "branch": ["tps_matched_segments_flat_ids", "tps_matched_segments_flat_counts"]
        }
    }
    with pytest.raises(IndexError):
        build(attributes, 2, DummyFlatEvent())


def test_flat_pair_with_target_registers_the_resolution():
    attributes = {
        "matched_segments": {
            "branch": ["tps_matched_segments_flat_ids", "tps_matched_segments_flat_counts"],
            "target": "segments",
            "identifier": "idx",
        }
    }
    built = build(attributes, 0, DummyFlatEvent())

    assert built._needs_resolution == {"matched_segments": ("segments", "idx")}
    expected = Particle(index=0, ev=DummyFlatEvent(), **attributes)
    assert built._needs_resolution == expected._needs_resolution


def test_pair_without_target_is_not_a_resolution():
    attributes = {
        "matched_segments": {
            "branch": ["tps_matched_segments_flat_ids", "tps_matched_segments_flat_counts"]
        }
    }
    assert build(attributes, 0, DummyFlatEvent())._needs_resolution == {}


def test_literals_are_not_shared_between_particles():
    attributes = {"matched_tps": [], "showered": False}
    readers = readers_of(attributes)

    first = Particle(index=0, ev=DummyEvent(), readers=readers)
    second = Particle(index=1, ev=DummyEvent(), readers=readers)
    assert first.matched_tps is not second.matched_tps
    first.matched_tps.append(1)
    assert second.matched_tps == []
    assert first.showered is False and second.showered is False


def test_missing_branch_raises_value_error():
    with pytest.raises(ValueError):
        build({"x": {"branch": "no_such"}}, 0, DummyEvent())


def test_type_cast_matches_particle():
    attributes = {"charge": {"branch": "gen_pt", "type": "int"}}
    built = build(attributes, 0, DummyEvent())

    assert built.charge == 10
    assert isinstance(built.charge, int)


def test_type_cast_of_a_list_stays_lazy():
    attributes = {
        "matched": {
            "branch": ["tps_matched_segments_flat_ids", "tps_matched_segments_flat_counts"],
            "type": "float",
        }
    }
    built = build(attributes, 0, DummyFlatEvent())

    assert not isinstance(built.matched, list), "the cast is applied lazily, as before"
    assert list(built.matched) == [10.0, 11.0]


def test_invalid_type_cast_raises_value_error():
    with pytest.raises(ValueError):
        attribute_reader("x", {"branch": "gen_pt", "type": "NotAType"})


def test_definition_must_name_exactly_one_source():
    with pytest.raises(ValueError):
        attribute_reader("x", {"branch": "gen_pt", "expr": "1"})
    with pytest.raises(ValueError):
        attribute_reader("x", {})
    with pytest.raises(ValueError):
        attribute_reader("x", {"branch": ["a", "b", "c"]})


def test_invalid_expression_raises_value_error():
    with pytest.raises(ValueError):
        attribute_reader("x", {"expr": "1 +"})


def test_src_is_only_supported_by_particle_init():
    with pytest.raises(ValueError):
        attribute_reader("x", {"src": "dtpr.utils.functions.color_msg"})
