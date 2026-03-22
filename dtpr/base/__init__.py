
__all__ = [
    "Config",
    "RUN_CONFIG",
    "Event",
    "EventList",
    "NTuple",
    "Particle"
]

def __getattr__(name: str) -> object:
    if name in ["Config", "RUN_CONFIG"]:
        from .config import Config, RUN_CONFIG

        return Config if name == "Config" else RUN_CONFIG

    if name == "Event":
        from .event import Event

        return Event

    if name == "NTuple":
        from .ntuple import NTuple

        return NTuple

    if name == "EventList":
        from .event_list import EventList

        return EventList

    if name == "Particle":
        from .particle import Particle

        return Particle

    raise AttributeError(f"module 'DTPatternRecognition' has no attribute {name!r}")