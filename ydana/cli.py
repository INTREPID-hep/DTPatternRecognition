"""Main CLI interface"""

import argparse
import inspect
import os
import sys
import warnings
from collections.abc import Callable
from copy import deepcopy

from .base.config import CLI_CONFIG, RUN_CONFIG
from .utils.functions import (
    color_msg,
    create_outfolder,
    ensure_on_syspaths,
    error_handler,
    get_callable_from_src,
    warning_handler,
)

warnings.filterwarnings(action="once", category=UserWarning)
# Set the custom warning handler
warnings.showwarning = warning_handler
# Set the custom error handler
sys.excepthook = error_handler


def add_arguments(
    parser: argparse.ArgumentParser, args: dict[str, dict[str, object]]
) -> None:
    """Add common arguments to the parser."""
    for arg_name, args_items in args.items():
        try:
            _items = deepcopy(args_items)
            parser.add_argument(
                *_items.pop("flags"),
                **_items,
            )
        except Exception:
            raise ValueError(
                f"Failed to add argument: {arg_name} with items: {args_items}"
            )


def create_wrapper(
    func: Callable[..., object],
) -> Callable[[argparse.Namespace], object]:
    """Create a wrapper function to map parsed arguments to the function's parameters."""
    sig = inspect.signature(func)
    has_var_keyword = any(
        p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
    )

    def wrapper(args: argparse.Namespace) -> None:
        kwargs: dict[str, object] = {}
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


def add_subcommands(
    subparser: argparse._SubParsersAction, subcommands: list[str]
) -> None:
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


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Command line interface for YDANA, providing analysis utilities for "
            "YAML-driven columnar ntuple workflows."
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
            "merge-roots",
            "test-cli",
        ],
    )

    # Parse the command line
    args = parser.parse_args()

    verbose = getattr(args, "verbose", True)

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
            if verbose:
                color_msg(f"Using configuration file: {args.config_file}", "yellow")
            RUN_CONFIG.change_config_file(config_path=args.config_file)
        else:
            if verbose:
                color_msg(
                    f"No configuration file provided or found in the output path. Using default configuration file: {RUN_CONFIG.path}",
                    "yellow",
                )

    # Ensure config dir and (optionally) outfolder are importable
    ensure_on_syspaths(RUN_CONFIG)
    if hasattr(args, "outfolder") and args.outfolder:
        sys.path.append(args.outfolder)

    # Run the function — wrap in a Dask distributed client if requested
    scheduler_address = getattr(args, "scheduler_address", None)
    if scheduler_address:
        try:
            from dask.distributed import Client
        except ImportError:
            raise ImportError(
                "dask.distributed is not installed. "
                "Install it with: pip install dask[distributed]",
            )
        if verbose:
            color_msg(f"Connecting to Dask scheduler: {scheduler_address}", "yellow")

        with Client(scheduler_address) as _client:
            if verbose:
                color_msg(f"Dask dashboard: {_client.dashboard_link}", "yellow")
            args.func(args)
    else:
        args.func(args)


if __name__ == "__main__":
    main()
