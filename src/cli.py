"""Main CLI interface"""
import os
import sys
import argparse
import warnings
import importlib
import inspect
from copy import deepcopy
from dtpr.utils.functions import color_msg, warning_handler, error_handler
from src.utils.config import RUN_CONFIG, CLI_CONFIG

# warnings.filterwarnings(action="once", category=UserWarning)
# # Set the custom warning handler
# warnings.showwarning = warning_handler
# # Set the custom error handler
# sys.excepthook = error_handler


def add_arguments(parser: argparse.ArgumentParser, args: list) -> None:
    """Add common arguments to the parser."""
    for arg in args:
        _items = deepcopy(CLI_CONFIG.opt_args[arg])
        parser.add_argument(
            *_items.pop("flags"),
            **_items,
        )

def import_function(func_path: str) -> callable:
    """Dynamically import a function from a given module path."""
    module_name, func_name = func_path.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, func_name)

def create_wrapper(func: callable) -> callable:
    """Create a wrapper function to map parsed arguments to the function's parameters."""
    sig = inspect.signature(func)
    def wrapper(args: argparse.Namespace) -> None:
        kwargs = {}
        for param in sig.parameters.values():
            kwargs[param.name] = getattr(args, param.name)
        return func(**kwargs)
    return wrapper

def add_subcommands(subparser: argparse._SubParsersAction, subcommands: list) -> None:
    """Add subcommands to the subparser."""
    for subcommand in subcommands:
        _subcommand_info = deepcopy(CLI_CONFIG.pos_args[subcommand])
        _subcommand_parser = subparser.add_parser(
            _subcommand_info["name"],
            help=_subcommand_info["help"],
        )
        add_arguments(_subcommand_parser, _subcommand_info["opt_args"])

        # Function to import
        func_path = _subcommand_info["func"]
        func = import_function(func_path)
        _subcommand_parser.set_defaults(func=create_wrapper(func))


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Command Line Interface for the Pattern Recognition - Analysis, providing some "
            "base analysis tools to study NTuple files."
        )
    )

    command_subparser = parser.add_subparsers(required=True, dest="command")

    add_subcommands(
        command_subparser,
        [
            # ---- analysis commands ----
            "fill-histos",
            "plot-dts",
            "plot-dt",
            "inspect-event",
            "event-visualizer",
            "digis-fpga-dumper",
            # ---- config commands ----
            "create-config",
            "create-analysis",
            "create-histogram",
        ],
    )

    # Parse the command line
    args = parser.parse_args()

    # update the run config file if it exists in the output folder
    if not "create" in args.command:
        cfg_changed = False
        for root, _, files in os.walk(args.outfolder):
            if "run_config.yaml" in files:
                cfg_changed = True
                config_file_path = os.path.join(root, "run_config.yaml")
                color_msg(f"Using configuration file: {config_file_path}", "yellow")
                RUN_CONFIG.change_config_file(config_path=config_file_path)
                break
        if not cfg_changed:
            color_msg(f"No configuration file found in the output path. Using default configuration file: {RUN_CONFIG.path}", "yellow")

    # Run the function
    args.func(args)


if __name__ == "__main__":
    main()
