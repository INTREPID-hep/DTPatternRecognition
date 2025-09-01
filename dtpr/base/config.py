import os
import warnings
import yaml


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
        Loads the configuration from a YAML file.

        :param config_path: The path to the configuration file.
        :type config_path: str
        :return: The loaded configuration dictionary.
        :rtype: dict
        """
        with open(config_path, "r") as file:
            try:
                config = yaml.safe_load(file)
                return config
            except yaml.YAMLError as exc:
                raise exc


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
