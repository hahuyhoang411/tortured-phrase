from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def read_json_list(path: Path) -> list[Any]:
    if not path.exists():
        return []
    raw = path.read_text(encoding="utf-8")
    if not raw.strip():
        return []
    data = json.loads(raw)
    if isinstance(data, list):
        return data
    raise ValueError("Expected JSON array")


def write_json_list(path: Path, data: list[Any]) -> None:
    if not isinstance(data, list):
        raise ValueError("data must be a list")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
