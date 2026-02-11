from pathlib import Path

from dtpr.base.config import Config


def test_config_include_loader():
    fixtures_dir = Path(__file__).parent / "fixtures" / "config_include"
    cfg = Config(str(fixtures_dir / "include_main.yaml"))

    assert cfg.simple_key == "simple"
    assert cfg.included_map == {"alpha": 1, "beta": 2}
    assert cfg.merged_map == {"alpha": 1, "beta": 2, "gamma": 3, "delta": 4}
    assert cfg.included_list == ["x", "y", "z"]
