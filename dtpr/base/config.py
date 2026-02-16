import os
import warnings
import yaml
import re


def _resolve_includes(content, base_dir, visited=None):
    """
    Recursively resolves !include directives in YAML content.
    
    The !include directive inlines the content from the referenced file at the same
    indentation level, allowing merging with sibling keys.
    
    Examples:
      parent:
        !include file.yaml    # Content of file.yaml is inlined here
        extra_key: value      # This merges with included content
    
    :param content: The YAML content as a string.
    :param base_dir: The base directory for resolving relative paths.
    :param visited: Set of already visited files to prevent circular includes.
    :return: The content with all includes resolved.
    :raises ValueError: If circular includes are detected.
    :raises FileNotFoundError: If an included file cannot be found.
    :raises IOError: If an included file cannot be read.
    """
    if visited is None:
        visited = set()
    
    # Pattern to match !include directives (standalone or after key:)
    pattern = r'^(\s*)(?:(\w+):\s*)?!include\s+(.+)$'
    
    lines = content.split('\n')
    processed_lines = []
    
    for line in lines:
        match = re.match(pattern, line)
        if match:
            indent = match.group(1)
            key_part = match.group(2)  # Just the key name (no colon)
            include_path = match.group(3).split('#')[0].strip()  # Remove comments and strip whitespace
            
            full_path = os.path.abspath(os.path.join(base_dir, include_path))
            
            # Check for circular includes
            if full_path in visited:
                raise ValueError(f"Circular include detected: {full_path}")
            
            visited.add(full_path)
            
            try:
                with open(full_path, 'r') as inc_file:
                    included_content = inc_file.read()
            except FileNotFoundError as e:
                raise FileNotFoundError(
                    f"Cannot find included file '{include_path}' (resolved to: {full_path})"
                ) from e
            except IOError as e:
                raise IOError(
                    f"Cannot read included file '{include_path}' (resolved to: {full_path})"
                ) from e
            
            included_dir = os.path.dirname(full_path)
            resolved_content = _resolve_includes(included_content, included_dir, visited)
            
            if key_part:
                # Case: key: !include file.yaml
                # Add the key line, then indent the content
                processed_lines.append(f"{indent}{key_part}:")
                nested_indent = indent + "  "
                indented_lines = [nested_indent + line if line.strip() else line 
                                 for line in resolved_content.split('\n')]
                processed_lines.extend(indented_lines)
            else:
                # Case: !include file.yaml (standalone)
                # Inline the content at the same indentation level
                indented_lines = [indent + line if line.strip() else line 
                                 for line in resolved_content.split('\n')]
                processed_lines.extend(indented_lines)
            
            visited.remove(full_path)
        else:
            processed_lines.append(line)
    
    return '\n'.join(processed_lines)


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
        
        All !include directives (both top-level and nested) are resolved before
        parsing the YAML, with paths relative to the file containing the directive.

        :param config_path: The path to the configuration file.
        :type config_path: str
        :return: The loaded configuration dictionary.
        :rtype: dict
        :raises FileNotFoundError: If the config file or any included file is not found.
        :raises yaml.YAMLError: If there's an error parsing the YAML.
        :raises ValueError: If circular includes are detected.
        """
        config_dir = os.path.dirname(os.path.abspath(config_path))
        
        try:
            with open(config_path, "r") as file:
                content = file.read()
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Configuration file not found: {config_path}") from e
        except IOError as e:
            raise IOError(f"Cannot read configuration file: {config_path}") from e
        
        # Recursively resolve all !include directives
        try:
            resolved_content = _resolve_includes(content, config_dir)
        except (FileNotFoundError, ValueError, IOError) as e:
            raise type(e)(f"Error processing includes in {config_path}: {e}") from e
        
        # Parse the final YAML with standard loader
        try:
            config = yaml.safe_load(resolved_content)
        except yaml.YAMLError as e:
            raise yaml.YAMLError(
                f"Error parsing YAML configuration from {config_path}: {e}"
            ) from e
        
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
