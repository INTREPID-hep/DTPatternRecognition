from pathlib import Path

import pytest

from dtpr.base.config import Config


def test_config_include_loader():
    fixtures_dir = Path(__file__).parent / "fixtures" / "config_include"
    cfg = Config(str(fixtures_dir / "include_main.yaml"))

    assert cfg.simple_key == "simple"
    assert cfg.included_map == {"alpha": 1, "beta": 2}
    assert cfg.merged_map == {"alpha": 1, "beta": 2, "gamma": 3, "delta": 4}
    assert cfg.included_list == ["x", "y", "z"]


def test_include_is_relative_to_the_including_file():
    fixtures_dir = Path(__file__).parent / "fixtures" / "config_include"
    cfg = Config(str(fixtures_dir / "nested_main.yaml"))
    assert cfg.nested_map == {"outer": 1, "inner_map": {"inner": 2}}


def test_circular_include_raises():
    fixtures_dir = Path(__file__).parent / "fixtures" / "config_include"
    with pytest.raises(ValueError):
        Config(str(fixtures_dir / "circular_a.yaml"))


def test_missing_include_raises():
    fixtures_dir = Path(__file__).parent / "fixtures" / "config_include"
    with pytest.raises(FileNotFoundError):
        Config(str(fixtures_dir / "missing_include.yaml"))
