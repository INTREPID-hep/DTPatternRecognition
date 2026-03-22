from importlib.metadata import PackageNotFoundError, version

__all__ = [
    "Config",
    "RUN_CONFIG",
    "Event",
    "NTuple",
    "__version__",
]

try:
    __version__ = version("DTPatternRecognition")
except PackageNotFoundError:
    __version__ = "3.4.0-beta"

__doc__ = "Set of tools to implement pattern recognition algorithms on CMS DTs"


def __getattr__(name: str) -> object:
    if name in ["Config", "RUN_CONFIG"]:
        from .base.config import Config, RUN_CONFIG

        return Config if name == "Config" else RUN_CONFIG

    if name in ["Event", "NTuple"]:
        from .base.event import Event
        from .base.ntuple import NTuple

        return Event if name == "Event" else NTuple


    raise AttributeError(f"module 'DTPatternRecognition' has no attribute {name!r}")