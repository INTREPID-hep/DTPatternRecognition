import os
import warnings
import yaml


def _merge(base, extra):
    """
    Merge two YAML documents.

    Mappings are merged key by key (recursively); anything else is replaced by
    ``extra``, so a list of included files behaves like a sequence of overrides.

    :param base: The document merged so far, or None.
    :param extra: The document to merge into it.
    :return: The merged document.
    """
    if isinstance(base, dict) and isinstance(extra, dict):
        merged = dict(base)
        for key, value in extra.items():
            merged[key] = _merge(merged[key], value) if key in merged else value
        return merged
    return extra


class IncludeLoader(yaml.SafeLoader):
    """
    YAML loader that resolves ``!include`` directives.

    ``!include`` takes a path or a list of paths, resolved relative to the file
    that names them::

        included_map: !include maps.yaml
        merged_map: !include [maps_a.yaml, maps_b.yaml]
        included_list: !include list.yaml

    A list is merged in order: mappings key by key, anything else replaced by
    the last file. Circular includes raise a ``ValueError``.
    """

    def __init__(self, stream, base_dir, visited=()):
        super().__init__(stream)
        self.base_dir = base_dir
        self.visited = visited


def _construct_include(loader, node):
    """Build the value of an ``!include`` node."""
    if isinstance(node, yaml.ScalarNode):
        paths = [loader.construct_scalar(node)]
    elif isinstance(node, yaml.SequenceNode):
        paths = [loader.construct_scalar(item) for item in node.value]
    else:
        raise yaml.constructor.ConstructorError(
            None,
            None,
            "!include expects a path or a list of paths",
            node.start_mark,
        )

    merged = None
    for path in paths:
        merged = _merge(merged, _load_file(os.path.join(loader.base_dir, path), loader.visited))
    return merged


IncludeLoader.add_constructor("!include", _construct_include)


def _load_file(path, visited):
    """
    Load one YAML file, resolving its own includes relative to its directory.

    :param path: Path to the file.
    :type path: str
    :param visited: Files already being loaded, used to detect circular includes.
    :type visited: tuple of str
    :return: The loaded document.
    :raises FileNotFoundError: If the file cannot be found.
    :raises IOError: If the file cannot be read.
    :raises ValueError: If an include is circular.
    :raises yaml.YAMLError: If the YAML is invalid.
    """
    resolved = os.path.abspath(path)
    if resolved in visited:
        raise ValueError(f"Circular include detected: {resolved}")

    try:
        handle = open(resolved, "r")
    except FileNotFoundError as error:
        raise FileNotFoundError(
            f"Cannot find included file '{path}' (resolved to: {resolved})"
        ) from error
    except IOError as error:
        raise IOError(f"Cannot read included file '{path}' (resolved to: {resolved})") from error

    with handle:
        loader = IncludeLoader(
            handle, base_dir=os.path.dirname(resolved), visited=visited + (resolved,)
        )
        try:
            return loader.get_single_data()
        finally:
            loader.dispose()


class Config:
    """
    Configuration class to handle loading and setting up configurations from a YAML file.
    """

    def __init__(self, config_path):
        """
        Initializes the Config object.

        :param config_path: The path to the configuration file.
        :type config_path: str
        """
        self.path = config_path
        self._setup()

    def _setup(self):
        """
        Sets up the configuration by loading the config file and setting attributes.
        """
        config_dict = self._load_config(self.path)
        if not config_dict:
            config_dict = {}
        for key in config_dict.keys():
            setattr(self, key, config_dict[key])
            # Evaluate types of opt_args
            if key == "opt_args":
                for subkey in self.opt_args.keys():
                    try:
                        self.opt_args[subkey]["type"] = eval(self.opt_args[subkey]["type"])
                    except KeyError:
                        continue

    def _delete_existing_attributes(self):
        """
        Deletes existing attributes of the Config object.
        """
        for attr in list(self.__dict__.keys()):
            if attr not in ["path"]:
                delattr(self, attr)

    def change_config_file(self, config_path="./run_config.yaml"):
        """
        Changes the configuration file to a new path.

        :param config_path: The relative path to the new config file. Default is "./run_config.yaml".
        :type config_path: str
        """
        try:
            self._delete_existing_attributes()
            self.path = config_path
            self._setup()
        except Exception as e:
            warnings.warn(
                f"Error changing configuration file: {e}. Using the default configuration file."
            )

    @staticmethod
    def _load_config(config_path):
        """
        Loads the configuration from a YAML file supporting !include directives.

        The directives are resolved by IncludeLoader while the YAML is parsed,
        so an include can appear anywhere a value can and a list of includes is
        merged in order. Paths are relative to the file that names them.

        :param config_path: The path to the configuration file.
        :type config_path: str
        :return: The loaded configuration dictionary.
        :rtype: dict
        :raises FileNotFoundError: If the config file or any included file is not found.
        :raises yaml.YAMLError: If there's an error parsing the YAML.
        :raises ValueError: If circular includes are detected.
        """
        path = os.path.abspath(config_path)
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        try:
            config = _load_file(path, ())
        except (FileNotFoundError, ValueError, IOError) as e:
            raise type(e)(f"Error processing includes in {config_path}: {e}") from e
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Error parsing YAML configuration from {config_path}: {e}") from e

        return config if config else {}


# ------- create CLI_CONFIG -------
cli_config_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../utils/yamls/config_cli.yaml")
)
CLI_CONFIG = Config(cli_config_path)

# ------- create RUN_CONFIG and customize its method -------
run_config_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../utils/yamls/run_config.yaml")
)
RUN_CONFIG = Config(run_config_path)
