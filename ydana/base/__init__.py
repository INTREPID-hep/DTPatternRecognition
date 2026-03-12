"""Public base-layer API for YDANA."""

__all__ = [
    "CLI_CONFIG",
    "Config",
    "Event",
    "EventRecord",
    "Histogram",
    "NTuple",
    "Particle",
    "ParticleArray",
    "ParticleRecord",
    "YAMLSchema",
    "RUN_CONFIG",
    "expand",
    "fill",
    "from_config",
    "histos",
    "to_root",
]


def __getattr__(name: str) -> object:
    if name in {"CLI_CONFIG", "Config", "RUN_CONFIG"}:
        from .config import CLI_CONFIG, RUN_CONFIG, Config

        exports = {
            "CLI_CONFIG": CLI_CONFIG,
            "Config": Config,
            "RUN_CONFIG": RUN_CONFIG,
        }
        return exports[name]

    if name in {"Event", "EventRecord"}:
        from .event import EventRecord

        exports = {
            "Event": EventRecord,
            "EventRecord": EventRecord,
        }
        return exports[name]

    if name in {"Particle", "ParticleArray", "ParticleRecord"}:
        from .particle import ParticleArray, ParticleRecord

        exports = {
            "Particle": ParticleRecord,
            "ParticleArray": ParticleArray,
            "ParticleRecord": ParticleRecord,
        }
        return exports[name]

    if name == "NTuple":
        from .ntuple import NTuple

        return NTuple

    if name == "YAMLSchema":
        from .schema import YAMLSchema

        return YAMLSchema

    if name in {"Histogram", "expand", "fill", "from_config", "to_root"}:
        from .histos import Histogram, expand, fill, from_config, to_root

        exports = {
            "Histogram": Histogram,
            "expand": expand,
            "fill": fill,
            "from_config": from_config,
            "to_root": to_root,
        }
        return exports[name]

    if name == "histos":
        from importlib import import_module

        return import_module(".histos", __name__)

    raise AttributeError(f"module 'ydana.base' has no attribute {name!r}")
