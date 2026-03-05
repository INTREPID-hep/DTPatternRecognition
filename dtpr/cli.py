"""Main CLI interface"""

import os
import sys
import argparse
import warnings
import inspect
from copy import deepcopy
from .base.config import RUN_CONFIG, CLI_CONFIG
from .utils.functions import (
    color_msg,
    warning_handler,
    error_handler,
    get_callable_from_src,
    create_outfolder,
)
from .utils.paths import ensure_config_on_syspath

warnings.filterwarnings(action="once", category=UserWarning)
# Set the custom warning handler
warnings.showwarning = warning_handler
# Set the custom error handler
sys.excepthook = error_handler


def add_arguments(parser: argparse.ArgumentParser, args: dict) -> None:
    """Add common arguments to the parser."""
    for arg_name, args_items in args.items():
        try:
            _items = deepcopy(args_items)
            parser.add_argument(
                *_items.pop("flags"),
                **_items,
            )
        except:
            raise ValueError(f"Failed to add argument: {arg_name} with items: {args_items}")


def create_wrapper(func: callable) -> callable:
    """Create a wrapper function to map parsed arguments to the function's parameters."""
    sig = inspect.signature(func)
    has_var_keyword = any(
        p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
    )

    def wrapper(args: argparse.Namespace) -> None:
        kwargs = {}
        named_params: set[str] = set()
        for param in sig.parameters.values():
            if param.kind == inspect.Parameter.VAR_KEYWORD:
                continue
            named_params.add(param.name)
            kwargs[param.name] = getattr(args, param.name)
        if has_var_keyword:
            for key, val in vars(args).items():
                if key not in named_params and key != "func":
                    kwargs[key] = val
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
        func = get_callable_from_src(func_path)
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
            "dump-events",
            "merge-histos",
            "plot-dts",
            "plot-dt",
            "events-visualizer",
            "test-cli",
            # ---- config commands ----
            "create-particle",
            "create-config",
            "create-analysis",
            "create-histogram",
        ],
    )

    # Parse the command line
    args = parser.parse_args()

    if "create" not in args.command:
        change_cfg = False
        if args.config_file:
            change_cfg = True
        else:
            if getattr(args, "outfolder", None):
                create_outfolder(args.outfolder)
                with os.scandir(args.outfolder) as entries:
                    for entry in entries:
                        if entry.is_file() and entry.name.endswith(".yaml"):
                            change_cfg = True
                            args.config_file = entry.path
                        break
        if change_cfg:
            color_msg(f"Using configuration file: {args.config_file}", "yellow")
            RUN_CONFIG.change_config_file(config_path=args.config_file)
        else:
            color_msg(
                f"No configuration file provided or found in the output path. Using default configuration file: {RUN_CONFIG.path}",
                "yellow",
            )

    # Ensure config dir and (optionally) outfolder are importable
    ensure_config_on_syspath(RUN_CONFIG)
    if hasattr(args, "outfolder") and args.outfolder:
        sys.path.append(args.outfolder)

    # Run the function — wrap in a Dask distributed client if requested
    scheduler_address = getattr(args, "scheduler_address", None)
    if scheduler_address:
        try:
            from dask.distributed import Client
        except ImportError:
            color_msg(
                "dask.distributed is not installed. "
                "Install it with: pip install dask[distributed]",
                "red",
            )
            sys.exit(1)
        color_msg(f"Connecting to Dask scheduler: {scheduler_address}", "yellow")
        with Client(scheduler_address) as _client:
            color_msg(
                f"Dask dashboard: {_client.dashboard_link}", "yellow"
            )
            args.func(args)
    else:
        args.func(args)


if __name__ == "__main__":
    main()
