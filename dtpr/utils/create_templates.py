import os
import shutil
from datetime import datetime
from .functions import color_msg


def create_particle_class_template(name, outfolder):
    template_source_path = os.path.join(
        os.path.dirname(__file__), "templates/particle_class_template.txt"
    )
    if "ntuple" in name.lower():
        final_name = name.lower().replace("ntuple", "_ntuple")
    else:
        final_name = name.lower()
    template_dest_path = os.path.join(outfolder, f"{final_name}.py")

    replace_placeholders(template_source_path, template_dest_path, name)
    print(
        color_msg(f"{name} class created from template in:", "green", return_str=True)
        + color_msg(f"{template_dest_path}", "yellow", return_str=True)
    )


def create_run_config_template(outfolder):
    yamls_dir = os.path.join(os.path.dirname(__file__), "../yamls")
    _skip = {"config_cli.yaml"}
    copied = []
    for fname in os.listdir(yamls_dir):
        if fname.endswith(".yaml") and fname not in _skip:
            shutil.copy(os.path.join(yamls_dir, fname), os.path.join(outfolder, fname))
            copied.append(fname)
    print(
        color_msg(f"Config templates copied to ", "green", return_str=True)
        + color_msg(f"{outfolder}", "yellow", return_str=True)
        + color_msg(f" ({', '.join(sorted(copied))})", "blue", return_str=True)
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
