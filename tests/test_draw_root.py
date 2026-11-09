from dtpr.analysis.draw_root import _build_root_canvas_name, _pick_input_root_file
from dtpr.base.config import CLI_CONFIG


def test_build_root_canvas_name():
    assert _build_root_canvas_name("dir/my_histo", "v1") == "dir_my_histo_v1"
    assert _build_root_canvas_name("my_histo", "") == "my_histo"
    assert _build_root_canvas_name(" ", "") == "root_object"


def test_pick_input_root_file_prefers_first_item():
    assert _pick_input_root_file("file.root") == "file.root"
    assert _pick_input_root_file(["a.root", "b.root"]) == "a.root"


def test_cli_config_contains_draw_root_command():
    assert "draw-root" in CLI_CONFIG.pos_args
    assert CLI_CONFIG.pos_args["draw-root"]["func"] == "dtpr.analysis.draw_root.draw_root_object"
