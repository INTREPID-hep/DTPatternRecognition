import os
import re
import warnings
import yaml

DIRECTIVE = re.compile(r"^(\s*)(?:([\w.-]+)\s*:\s*)?!include\s+(.+?)\s*$")


def _include_paths(spec):
    """
    Return the paths named by an ``!include`` directive.

    :param spec: Everything written after ``!include``.
    :type spec: str
    :return: One path, or several when the directive uses a list.
    :rtype: list of str
    """
    spec = spec.split(" #")[0].strip()
    if spec.startswith("[") and spec.endswith("]"):
        return [path.strip().strip("\"'") for path in spec[1:-1].split(",") if path.strip()]
    return [spec.strip("\"'")]


def _read_include(resolved, path):
    """
    Read an included file, reporting both the path and how it was resolved.

    :param resolved: Absolute path of the included file.
    :type resolved: str
    :param path: Path as it was written in the directive.
    :type path: str
    :return: The content of the file.
    :rtype: str
    """
    try:
        with open(resolved) as handle:
            return handle.read()
    except FileNotFoundError as error:
        raise FileNotFoundError(
            f"Cannot find included file '{path}' (resolved to: {resolved})"
        ) from error
    except IOError as error:
        raise IOError(f"Cannot read included file '{path}' (resolved to: {resolved})") from error


def _resolve_includes(text, base_dir, visited=()):
    """
    Splice every ``!include`` directive into the text.

    ``key: !include file`` becomes ``key:`` followed by the content of the file
    indented two more spaces, so the content is the value of that key. A bare
    ``!include file`` is spliced at its own indentation, so its keys merge with
    its siblings, which is how a configuration is split into several files.

    Paths are relative to the file that names them and a circular include raises
    ``ValueError``. When a directive names several files they are spliced in
    order, so the last one wins for a key that appears in more than one.

    :param text: YAML text to process.
    :type text: str
    :param base_dir: Directory of the file the text belongs to.
    :type base_dir: str
    :param visited: Absolute paths already being expanded.
    :type visited: tuple of str
    :return: The text with every include resolved.
    :rtype: str
    :raises FileNotFoundError: If an included file is missing.
    :raises IOError: If an included file cannot be read.
    :raises ValueError: If an include is circular.
    """
    lines = []
    for line in text.splitlines():
        match = DIRECTIVE.match(line)
        if not match:
            lines.append(line)
            continue

        indent, key, spec = match.groups()
        if key:
            lines.append(f"{indent}{key}:")
        shift = indent + ("  " if key else "")

        for path in _include_paths(spec):
            resolved = os.path.abspath(os.path.join(base_dir, path))
            if resolved in visited:
                raise ValueError(f"Circular include detected: {resolved}")

            content = _read_include(resolved, path)
            expanded = _resolve_includes(content, os.path.dirname(resolved), visited + (resolved,))
            lines.extend(shift + line if line.strip() else line for line in expanded.splitlines())

    return "\n".join(lines)


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
        Loads the configuration from a YAML file, resolving ``!include`` directives.

        Includes are spliced into the text before parsing, so a directive can be
        the value of a key or a stand-alone line, and an included file can itself
        contain directives.

        :param config_path: The path to the config file.
        :type config_path: str
        :return: The loaded configuration dictionary.
        :rtype: dict
        :raises FileNotFoundError: If the config file or an included file is not found.
        :raises yaml.YAMLError: If there's an error parsing the YAML.
        :raises ValueError: If circular includes are detected.
        """
        path = os.path.abspath(config_path)
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        try:
            with open(path) as handle:
                text = handle.read()
        except IOError as error:
            raise IOError(f"Cannot read configuration file: {config_path}") from error

        try:
            text = _resolve_includes(text, os.path.dirname(path))
        except (FileNotFoundError, ValueError, IOError) as error:
            raise type(error)(f"Error processing includes in {config_path}: {error}") from error

        try:
            config = yaml.safe_load(text)
        except yaml.YAMLError as error:
            raise yaml.YAMLError(
                f"Error parsing YAML configuration from {config_path}: {error}"
            ) from error

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
