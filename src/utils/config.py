import os
import shutil
import warnings
import types
from dtpr.utils.config import Config
from dtpr.utils.functions import color_msg
from dtpr.utils.config import EVENT_CONFIG

# ------- create CLI_CONFIG -------
cli_config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "./yamls/config_cli.yaml"))
CLI_CONFIG = Config(cli_config_path)

# ------- create RUN_CONFIG and customize its method -------
run_config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "./yamls/run_config.yaml"))
RUN_CONFIG = Config(run_config_path)

def custom_change_config_file(self, config_path=run_config_path):
    """
    Custom method to change the configuration file to a new path for RUN_CONFIG and at the same time
    change the EVENT_CONFIG to the same path. This is because in this framework, the event config is
    contained in the run config file, so we can just set the event config to point to the run config file.
    """
    try:
        self.path = config_path
        self._setup()
        EVENT_CONFIG.change_config_file(config_path=config_path)
    except Exception as e:
        warnings.warn(
            f"Error changing configuration file: {e}. Using the default configuration file."
        )

# Bind the new method to the RUN_CONFIG instance
RUN_CONFIG.change_config_file = types.MethodType(custom_change_config_file, RUN_CONFIG)
# Now every time that change the configuration file for RUN_CONFIG, the EVENT_CONFIG will be updated too
RUN_CONFIG.change_config_file(run_config_path)

def create_workspace(outfolder):
    """
    Creates a workspace directory with necessary subdirectories and configuration files.

    Args:
        outfolder (str): The path to the output folder.
    """
    folder, workspace, run_config_file = validate_workspace(outfolder)

    if not folder:
        # Create the output folder
        color_msg(f"Creating output folder {outfolder}...", color="purple", indentLevel=0)
        os.system(f"mkdir -p {outfolder}")

    if not workspace:
        # Create the workspace folder
        color_msg(f"Setting up workspace folder...", color="purple", indentLevel=0)
        os.system(f"mkdir -p {os.path.join(outfolder, '.workspace')}")

    if not run_config_file:
        # put a copy of the run_config.yaml file in the output folder
        run_config_path = os.path.join(outfolder, ".workspace/run_config.yaml")
        color_msg(f"Copying run_config.yaml file into the workspace folder...", color="yellow", indentLevel=1)
        shutil.copy(RUN_CONFIG.path, run_config_path)

    print(
        color_msg(f"Workspace ready in {outfolder}:", color="green", return_str=True) +
        color_msg(f"Use for your analysis the argument '-o {outfolder}'", color="yellow", return_str=True)
    )

    launch_config_cli(outfolder)

def validate_workspace(outfolder):
    """
    Validates the workspace directory structure.

    Args:
        outfolder (str): The path to the output folder.

    Returns:
        tuple: A tuple indicating the presence of the folder, workspace, and config file.
    """
    validation = [1, 1, 1]
    if not os.path.exists(outfolder):
        validation[0] = 0 # no one of the required elements exists

    if not os.path.exists(os.path.join(outfolder, ".workspace/")):
        validation[1] = 0 # Output folder does not contain a .workspace folder

    if not os.path.exists(os.path.join(outfolder, ".workspace/run_config.yaml")):
        validation[2] = 0 # Output folder does not contain a run_config.yaml file

    return validation # all required elements exist

def launch_config_cli(outfolder):
    """
    Launches a CLI to edit the configuration file.

    Args:
        outfolder (str): The path to the output folder.
    """
    # color_msg("Do you want to edit the configuration file?", color="purple", indentLevel=1)
    # anws = input(color_msg("y/n: ", color="purple", indentLevel=1, return_str=True))
    # if anws == "y":
    #     os.system(f"vim {outfolder}/.workspace/run_config.yaml")
    #     color_msg("Configuration file saved.", color="green", indentLevel=1)
    pass