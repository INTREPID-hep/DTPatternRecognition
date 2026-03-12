"""Public package interface for YDANA."""

from importlib.metadata import PackageNotFoundError, version

__all__ = ["Config", "Histogram", "NTuple", "__version__"]

try:
    __version__ = version("ydana")
except PackageNotFoundError:
    __version__ = "0.1.0"


def __getattr__(name: str) -> object:
    if name == "Config":
        from .base.config import Config

        return Config
    if name == "Histogram":
        from .base.histos import Histogram

        return Histogram
    if name == "NTuple":
        from .base.ntuple import NTuple

        return NTuple
    raise AttributeError(f"module 'ydana' has no attribute {name!r}")
