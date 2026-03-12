from __future__ import annotations

from pathlib import Path

import pytest

from ydana.analysis.dumper import dump_events


def test_dump_events_requires_exactly_one_output_format(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="exactly one output format"):
        dump_events(inputs=[], outfolder=str(tmp_path), to_root=False, to_parquet=False)

    with pytest.raises(ValueError, match="exactly one output format"):
        dump_events(inputs=[], outfolder=str(tmp_path), to_root=True, to_parquet=True)


def test_dump_events_to_root_with_test_ntuple(sample_root_file: str, tmp_path: Path) -> None:
    outdir = tmp_path / "dump-root"

    dump_events(
        inputs=[sample_root_file],
        tree_name="dtNtupleProducer/DTTREE",
        maxfiles=1,
        in_root=True,
        in_parquet=False,
        to_root=True,
        to_parquet=False,
        ncores=1,
        outfolder=str(outdir),
        tag="_unit",
        verbose=False,
    )

    root_files = sorted((outdir / "roots").glob("dumpedEvents*_unit_*.root"))
    assert root_files, "Expected at least one dumped ROOT partition file"


def test_dump_events_to_parquet_with_test_ntuple(sample_root_file: str, tmp_path: Path) -> None:
    outdir = tmp_path / "dump-parquet"

    dump_events(
        inputs=[sample_root_file],
        tree_name="dtNtupleProducer/DTTREE",
        maxfiles=1,
        in_root=True,
        in_parquet=False,
        to_root=False,
        to_parquet=True,
        ncores=1,
        outfolder=str(outdir),
        tag="_unit",
        verbose=False,
    )

    parquet_dirs = sorted(outdir.glob("parquet*_unit"))
    assert parquet_dirs, "Expected dataset parquet output directory"
    assert list(parquet_dirs[0].glob("*.parquet"))
