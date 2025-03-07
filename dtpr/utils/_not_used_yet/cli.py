"""Main CLI interface"""

import sys
import argparse
import warnings
import importlib
import inspect

from dtpr.utils.functions import color_msg, warning_handler, error_handler
from dtpr.utils.config import create_workspace, RUN_CONFIG, CLI_CONFIG, validate_workspace

warnings.filterwarnings(action="once", category=UserWarning)
# Set the custom warning handler
warnings.showwarning = warning_handler
# Set the custom error handler
sys.excepthook = error_handler


def add_common_arguments(parser: argparse.ArgumentParser, args: list) -> None:
    """Add common arguments to the parser."""
    for arg in args:
        _items = CLI_CONFIG.common_opt_args[arg].copy()
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
            if param.name != "ntuple_type":
                kwargs[param.name] = getattr(args, param.name)
        kwargs["ntuple_type"] = args.command
        return func(**kwargs)
    return wrapper

def add_common_subcommands(parser, subcommands):
    """Add common subcommands to the parser."""
    subparser = parser.add_subparsers(required=True, dest="subcommand")
    for subcommand in subcommands:
        _subcommand_info = CLI_CONFIG.common_pos_args[subcommand].copy()
        _subcommand_parser = subparser.add_parser(
            _subcommand_info["name"],
            help=_subcommand_info["help"],
        )
        add_common_arguments(_subcommand_parser, _subcommand_info["opt_args"])

        # Determine the function to import
        func_path = _subcommand_info["func"]
        if isinstance(func_path, dict):
            command_parent = parser.prog.split(" ")[-1]
            func_path = func_path[command_parent]

        func = import_function(func_path)
        _subcommand_parser.set_defaults(func=create_wrapper(func))


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Command Line Interface for the Pattern Recognition - Analysis, providing some "
            "base analysis tools to study NTuple files."
        )
    )
    subparsers = parser.add_subparsers(required=True, dest="command")

    # -------------------------------------- "dtntuple" command ------------------------------------
    dtntuple_parser = subparsers.add_parser(
        "dtntuple",
        help="Command for processing CMS DT NTuple files."
    )

    add_common_subcommands(
        dtntuple_parser,
        [
            "fill-histos",
            "plot-dts",
            "plot-dt",
            "inspect-event",
            "event-visualizer",
        ],
    )

    # -------------------------------------- "g4dtntuple" command ----------------------------------
    G4dtntuple_parser = subparsers.add_parser(
        "g4dtntuple",
        help="Command for processing CMS G4 DT NTuple files."
    )

    add_common_subcommands(
        G4dtntuple_parser,
        [
            "fill-histos",
            "inspect-event",
            "event-visualizer",
        ],
    )


    # -------------------------------------- "config" command --------------------------------------
    config_parser = subparsers.add_parser("config",help="Command for managing needed workspaces.")

    config_subparsers = config_parser.add_subparsers(required=True, dest="subcommand")

    # -----------> "create" subcommand
    config_setup_parser = config_subparsers.add_parser(
        "create",
        help="Create a worksapce with necessaty config. files for running analysis.",
    )

    config_setup_parser.add_argument(
        "workspace",
        type=str,
        help="Path to the folder where the workspace will be created.",
    )

    config_setup_parser.set_defaults(
        func=lambda args: create_workspace(args.workspace)
    )

    # Parse the command line
    args = parser.parse_args()

    # update the run config file only if the command is not 'config create'
    if args.command != "config":

        # if args.command == "dtntuple":
        validation = validate_workspace(args.outfolder)
        if not all(validation):
            color_msg(f"{args.outfolder} is not a valid workspace", "red")
            return 0

        RUN_CONFIG.change_config_file(outfolder=args.outfolder, config_path=".workspace/run_config.yaml")

    if hasattr(args, 'func'):
        color_msg(f"Executing command: {args.command} {args.subcommand}", "green")
        args.func(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
