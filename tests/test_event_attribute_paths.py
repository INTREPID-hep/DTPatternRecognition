"""Which path Event takes to build a particle type.

A particle type whose class is the base Particle and whose attributes are all
declarative is built with the readers compiled by attribute_reader(); a custom
class or an src (callable) attribute keeps Particle.__init__, which is where that
behaviour lives. Both fallbacks are exercised here, and a counter checks that the
readers are compiled once per event, not once per particle.
"""

import os
import sys

from dtpr.base.event import Event
from dtpr.base.particle import Particle

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)


class DummyEvent:
    gen_pt = [10.0, 20.0]
    gen_eta = [1.1, -1.2]
    digi_nDigis = 2


def double_pt(particle):
    """A callable attribute: only Particle.__init__ can call it."""
    return particle.pt * 2


class IdentityParticle(Particle):
    """A subclass with the default __init__: Event must take the general path."""


def test_readers_are_compiled_once_per_event(monkeypatch):
    from dtpr.base import event as event_module

    calls = []
    original = event_module.attribute_reader

    def counting_attribute_reader(attr, spec):
        calls.append(attr)
        return original(attr, spec)

    monkeypatch.setattr(event_module, "attribute_reader", counting_attribute_reader)

    class Config:
        particle_types = {
            "digis": {
                "amount": "digi_nDigis",
                "attributes": {"pt": {"branch": "gen_pt"}, "eta": {"expr": "pt * 2"}},
            }
        }

    event = Event(ev=DummyEvent(), index=0, use_config=True, CONFIG=Config)

    assert len(event.digis) == 2, "two particles come from the amount branch"
    assert calls == ["pt", "eta"], "one reader per attribute, not per particle"


def test_falls_back_for_a_callable_attribute():
    class Config:
        particle_types = {
            "digis": {
                "amount": 2,
                "attributes": {
                    "pt": {"branch": "gen_pt"},
                    "double": {"src": "test_event_attribute_paths.double_pt"},
                },
            }
        }

    event = Event(ev=DummyEvent(), index=0, use_config=True, CONFIG=Config)

    assert [particle.double for particle in event.digis] == [20.0, 40.0]


def test_falls_back_for_a_custom_class():
    class Config:
        particle_types = {
            "digis": {
                "amount": 2,
                "class": "test_event_attribute_paths.IdentityParticle",
                "attributes": {"pt": {"branch": "gen_pt"}},
            }
        }

    event = Event(ev=DummyEvent(), index=0, use_config=True, CONFIG=Config)

    assert len(event.digis) == 2
    assert event.digis[1].pt == 20.0
    assert event.digis[0].name == "IdentityParticle"
