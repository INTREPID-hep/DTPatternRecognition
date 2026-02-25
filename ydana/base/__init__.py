"""Public base-layer API for YDANA."""

__all__ = [
    "Config",
    "get_run_config",
    "set_run_config",
    "NTuple",
    "EventRecord",
    "ParticleArray",
    "ParticleRecord",
    "Histogram",
]


def __getattr__(name: str) -> object:
    if name == "Config":
        from .config import Config

        return Config

    if name == "get_run_config":
        from .config import get_run_config

        return get_run_config

    if name == "set_run_config":
        from .config import set_run_config

        return set_run_config

    if name == "NTuple":
        from .ntuple import NTuple

        return NTuple

    if name == "EventRecord":
        from .event import EventRecord

        return EventRecord

    # Grouping things from the same file is fine, just return explicitly
    if name in {"ParticleArray", "ParticleRecord"}:
        from .particle import ParticleArray, ParticleRecord

        return ParticleArray if name == "ParticleArray" else ParticleRecord

    if name == "Histogram":
        from .histos import Histogram

        return Histogram

    raise AttributeError(f"module 'ydana.base' has no attribute {name!r}")
