import os
import shutil
from datetime import datetime
from dtpr.utils.functions import color_msg


def create_run_config_template(outfolder):
    source_path = os.path.join(
        os.path.dirname(__file__), "yamls/run_config.yaml"
    )
    dest_path = os.path.join(outfolder, "run_config.yaml")

    shutil.copy(source_path, dest_path)
    print(
        color_msg(
            f"run_config.yaml created in ", "green", return_str=True
        )
        + color_msg(f"{dest_path}", "yellow", return_str=True)
    )


def create_analysis_template(name, outfolder):
    template_source_path = os.path.join(
        os.path.dirname(__file__), "templates/analysis_template.txt"
    )
    final_name = name.lower()
    template_dest_path = os.path.join(outfolder, f"{final_name}.py")

    replace_placeholders(template_source_path, template_dest_path, name)
    print(
        color_msg(f"{name} analysis created from template in:", "green", return_str=True)
        + color_msg(f"{template_dest_path}", "yellow", return_str=True)
    )

def create_histogram_template(name, outfolder):
    template_source_path = os.path.join(
        os.path.dirname(__file__), "templates/histogram_template.txt"
    )
    final_name = name.lower()
    template_dest_path = os.path.join(outfolder, f"{final_name}_histos.py")

    replace_placeholders(template_source_path, template_dest_path, name)
    print(
        color_msg(f"{name} histogram created from template in:", "green", return_str=True)
        + color_msg(f"{template_dest_path}", "yellow", return_str=True)
    )

def replace_placeholders(file_source_path, file_dest_path, name):
    with open(file_source_path, "r") as file:
        content = file.read()
    content = content.replace("{name}", name)
    content = content.replace("{date}", datetime.now().strftime("%a %b %d %H:%M:%S %Y"))
    with open(file_dest_path, "w") as file:
        file.write(content)
