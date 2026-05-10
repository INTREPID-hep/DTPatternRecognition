import os
from typing import Union, Sequence

from dtpr.utils.functions import color_msg, create_outfolder


def _build_root_canvas_name(root_object: str, tag: str = "") -> str:
    object_name = root_object.strip().replace("/", "_")
    if not object_name:
        object_name = "root_object"
    return f"{object_name}_{tag}" if tag else object_name


def _pick_input_root_file(inpath: Union[str, Sequence[str]]) -> str:
    if isinstance(inpath, str):
        return inpath
    if not inpath:
        raise ValueError("No input ROOT file was provided.")
    if len(inpath) > 1:
        color_msg(
            "draw-root currently uses the first input file when multiple files are provided.",
            "yellow",
        )
    return inpath[0]


def draw_root_object(
    inpath: Union[str, Sequence[str]],
    outfolder: str,
    tag: str,
    root_object: str,
    save: bool,
) -> None:
    """Draw a ROOT object from the selected ROOT file."""
    input_file = os.path.abspath(_pick_input_root_file(inpath))
    if not os.path.isfile(input_file):
        raise FileNotFoundError(f"Input ROOT file not found: {input_file}")
    if not root_object:
        raise ValueError("Argument '--root-object' is required for draw-root.")

    import ROOT as r

    tfile = r.TFile.Open(input_file, "READ")
    if not tfile or tfile.IsZombie():
        raise OSError(f"Could not open ROOT file: {input_file}")

    try:
        obj = tfile.Get(root_object)
        if obj is None:
            raise ValueError(f"ROOT object '{root_object}' was not found in '{input_file}'.")

        canvas_name = _build_root_canvas_name(root_object, tag)
        canvas = r.TCanvas(f"canvas_{canvas_name}", f"canvas_{canvas_name}", 900, 700)
        obj.Draw()
        canvas.Update()
        if save:
            create_outfolder(outfolder)
            output_file = os.path.join(outfolder, f"{canvas_name}.png")
            canvas.SaveAs(output_file)
            color_msg(f"Saved ROOT canvas in: {output_file}", "green")
        else:
            color_msg("ROOT object drawn in memory (use --save to export).", "green")
    finally:
        tfile.Close()

    color_msg("Done", "green")
