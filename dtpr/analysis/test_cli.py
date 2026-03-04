"""Minimal CLI smoke-test: prints every argument as received."""

from ..utils.functions import color_msg
from ..base.ntuple import NTuple

def test_cli(inputs, tree_name, maxfiles, **kwargs):
    """Print all CLI arguments as received — useful for checking wiring."""
    color_msg("CLI argument dump", "green")
    color_msg(f"inputs     : {inputs}", "blue", 1)
    color_msg(f"tree_name  : {tree_name}", "blue", 1)
    color_msg(f"maxfiles   : {maxfiles}", "blue", 1)
    for key, val in kwargs.items():
        color_msg(f"{key:<10} : {val}", "yellow", 1)