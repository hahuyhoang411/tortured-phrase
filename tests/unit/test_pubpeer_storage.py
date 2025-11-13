from pathlib import Path

import pytest

from src.pipelines.processing.pubpeer.storage import read_json_list, write_json_list


def test_read_json_list_handles_missing(tmp_path: Path) -> None:
    path = tmp_path / "data.json"
    assert read_json_list(path) == []


def test_write_json_list_roundtrip(tmp_path: Path) -> None:
    path = tmp_path / "data.json"
    payload = [{"value": 1}]
    write_json_list(path, payload)
    assert read_json_list(path) == payload


def test_write_json_list_rejects_non_list(tmp_path: Path) -> None:
    path = tmp_path / "data.json"
    with pytest.raises(ValueError):
        write_json_list(path, {"value": 1})
