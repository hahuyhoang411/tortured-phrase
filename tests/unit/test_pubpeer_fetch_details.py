from pathlib import Path
from typing import Any, Dict, List

import pytest

from scripts.pubpeer_fetch_details import (
    clone_publication,
    enrich_details,
    load_existing_details,
    load_phrase_links,
    update_failures,
)
from src.pipelines.processing.pubpeer.storage import write_json_list


class FakeClient:
    def __init__(self) -> None:
        self.detail_calls: List[str] = []

    def get_publication_id(self, reference: str) -> str:
        if reference == "invalid":
            raise ValueError("invalid reference")
        identifier = reference.rsplit("/", 1)[-1]
        if "#" in identifier:
            identifier = identifier.split("#", 1)[0]
        return identifier

    def fetch_publication_detail(self, reference: str) -> Dict[str, Any]:
        identifier = self.get_publication_id(reference)
        self.detail_calls.append(reference)
        return {
            "pubpeer_id": identifier,
            "pubpeer_url": reference,
            "source_reference": reference,
            "title": "Title",
            "authors": [],
            "comments": [],
        }


def test_load_phrase_links_filters_invalid(tmp_path: Path) -> None:
    source = tmp_path / "results.json"
    write_json_list(
        source,
        [
            {"tortured_phrase": "valid", "pubpeer_links": ["link1", 5, ""]},
            {"tortured_phrase": None, "pubpeer_links": ["link2"]},
            "not-a-dict",
        ],
    )
    entries = load_phrase_links(source)
    assert entries == [{"phrase": "valid", "links": ["link1"]}]


def test_load_existing_details_returns_cache(tmp_path: Path) -> None:
    source = tmp_path / "details.json"
    write_json_list(
        source,
        [
            {
                "tortured_phrase": "phrase",
                "publications": [{"pubpeer_id": "ID1", "value": 1}],
                "failed_links": [{"reference": "bad", "error": "err"}],
            }
        ],
    )
    results, phrase_index, cache = load_existing_details(source)
    assert results[0]["publications"][0]["value"] == 1
    assert phrase_index["phrase"] is results[0]
    assert cache["ID1"]["value"] == 1
    assert results[0]["failed_links"][0]["reference"] == "bad"


def test_update_failures_adds_and_removes() -> None:
    record: Dict[str, Any] = {"tortured_phrase": "phrase", "publications": []}
    update_failures(record, "link", "error")
    assert record["failed_links"] == [{"reference": "link", "error": "error"}]
    update_failures(record, "link", None)
    assert "failed_links" not in record


def test_clone_publication_updates_reference() -> None:
    base = {"pubpeer_id": "ID", "source_reference": "old"}
    cloned = clone_publication(base, "new")
    assert cloned is not base
    assert cloned["source_reference"] == "new"
    assert base["source_reference"] == "old"


def test_enrich_details_fetches_and_caches(tmp_path: Path) -> None:
    phrases = [
        {"phrase": "one", "links": ["https://pubpeer.com/publications/ID1#0", "invalid"]},
        {"phrase": "two", "links": ["https://pubpeer.com/publications/ID1"]},
    ]
    results: List[Dict[str, Any]] = []
    phrase_index: Dict[str, Dict[str, Any]] = {}
    cache: Dict[str, Dict[str, Any]] = {}
    client = FakeClient()
    output_path = tmp_path / "details.json"
    enrich_details(phrases, results, phrase_index, cache, client, checkpoint_size=10, output_path=output_path)
    assert len(results) == 2
    first_publications = results[0]["publications"]
    assert len(first_publications) == 1
    assert first_publications[0]["pubpeer_id"] == "ID1"
    assert results[0]["failed_links"][0]["reference"] == "invalid"
    second_publications = results[1]["publications"]
    assert len(second_publications) == 1
    assert second_publications[0]["source_reference"] == "https://pubpeer.com/publications/ID1"
    assert client.detail_calls == ["https://pubpeer.com/publications/ID1#0"]
    assert output_path.exists()


def test_enrich_details_respects_existing_data(tmp_path: Path) -> None:
    phrases = [{"phrase": "one", "links": ["https://pubpeer.com/publications/ID1"]}]
    results: List[Dict[str, Any]] = [{"tortured_phrase": "one", "publications": [{"pubpeer_id": "ID1"}]}]
    phrase_index = {"one": results[0]}
    cache: Dict[str, Dict[str, Any]] = {"ID1": {"pubpeer_id": "ID1", "source_reference": "cached", "title": "Title", "authors": [], "comments": []}}
    client = FakeClient()
    output_path = tmp_path / "details.json"
    enrich_details(phrases, results, phrase_index, cache, client, checkpoint_size=5, output_path=output_path)
    assert client.detail_calls == []
    assert len(results[0]["publications"]) == 1
