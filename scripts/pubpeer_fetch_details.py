from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

from requests import RequestException
from tqdm import tqdm

ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from pipelines.processing.pubpeer.client import PubPeerClient, PubPeerClientConfig
from pipelines.processing.pubpeer.storage import read_json_list, write_json_list


def load_phrase_links(path: Path) -> List[Dict[str, Any]]:
    data = read_json_list(path)
    entries: List[Dict[str, Any]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        phrase = item.get("tortured_phrase")
        links = item.get("pubpeer_links")
        if not isinstance(phrase, str) or not isinstance(links, list):
            continue
        cleaned_links = [value for value in links if isinstance(value, str) and value.strip()]
        entries.append({"phrase": phrase, "links": cleaned_links})
    return entries


def load_existing_details(path: Path) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    data = read_json_list(path)
    results: List[Dict[str, Any]] = []
    phrase_index: Dict[str, Dict[str, Any]] = {}
    cache: Dict[str, Dict[str, Any]] = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        phrase = item.get("tortured_phrase")
        publications = item.get("publications")
        if not isinstance(phrase, str):
            continue
        record: Dict[str, Any] = {
            "tortured_phrase": phrase,
            "publications": [],
        }
        if isinstance(publications, list):
            valid_publications = []
            for publication in publications:
                if not isinstance(publication, dict):
                    continue
                identifier = publication.get("pubpeer_id")
                if isinstance(identifier, str) and identifier not in cache:
                    cache[identifier] = publication
                valid_publications.append(publication)
            record["publications"] = valid_publications
        failed_links = item.get("failed_links")
        if isinstance(failed_links, list):
            valid_failures = [failure for failure in failed_links if isinstance(failure, dict) and isinstance(failure.get("reference"), str)]
            if valid_failures:
                record["failed_links"] = valid_failures
        phrase_index[phrase] = record
        results.append(record)
    return results, phrase_index, cache


def ensure_record_structure(record: Dict[str, Any]) -> None:
    if not isinstance(record.get("publications"), list):
        record["publications"] = []
    failed = record.get("failed_links")
    if failed is not None and not isinstance(failed, list):
        record.pop("failed_links", None)


def clone_publication(publication: Dict[str, Any], reference: str) -> Dict[str, Any]:
    cloned = dict(publication)
    cloned["source_reference"] = reference
    return cloned


def update_failures(record: Dict[str, Any], reference: str, error_message: str | None) -> None:
    failures = [failure for failure in record.get("failed_links", []) if isinstance(failure, dict) and isinstance(failure.get("reference"), str)]
    failure_map = {failure["reference"]: failure for failure in failures}
    if error_message is None:
        if reference in failure_map:
            failures = [failure for failure in failures if failure["reference"] != reference]
    else:
        if reference in failure_map:
            failure_map[reference]["error"] = error_message
        else:
            entry = {"reference": reference, "error": error_message}
            failures.append(entry)
    if failures:
        record["failed_links"] = failures
    else:
        record.pop("failed_links", None)


def enrich_details(
    phrases: List[Dict[str, Any]],
    results: List[Dict[str, Any]],
    phrase_index: Dict[str, Dict[str, Any]],
    cache: Dict[str, Dict[str, Any]],
    client: PubPeerClient,
    checkpoint_size: int,
    output_path: Path,
) -> None:
    processed = 0
    progress = tqdm(total=len(phrases), desc="Fetching PubPeer details", unit="phrase")
    for entry in phrases:
        phrase = entry["phrase"]
        record = phrase_index.get(phrase)
        if record is None:
            record = {"tortured_phrase": phrase, "publications": []}
            phrase_index[phrase] = record
            results.append(record)
        ensure_record_structure(record)
        existing_ids = {pub.get("pubpeer_id") for pub in record["publications"] if isinstance(pub, dict) and isinstance(pub.get("pubpeer_id"), str)}
        for link in entry["links"]:
            try:
                identifier = client.get_publication_id(link)
            except ValueError as error:
                update_failures(record, link, str(error))
                continue
            if identifier in existing_ids:
                update_failures(record, link, None)
                continue
            if identifier in cache:
                publication_detail = clone_publication(cache[identifier], link)
                record["publications"].append(publication_detail)
                existing_ids.add(identifier)
                update_failures(record, link, None)
                continue
            try:
                detail = client.fetch_publication_detail(link)
            except RequestException as error:
                update_failures(record, link, str(error))
                continue
            except Exception as error:  # noqa: BLE001
                update_failures(record, link, str(error))
                continue
            cache[identifier] = detail
            record["publications"].append(detail)
            existing_ids.add(identifier)
            update_failures(record, link, None)
        processed += 1
        progress.update(1)
        if processed % checkpoint_size == 0:
            write_json_list(output_path, results)
    progress.close()
    write_json_list(output_path, results)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=Path("results/pubpeer_results.json"))
    parser.add_argument("--output", type=Path, default=Path("results/pubpeer_publications.json"))
    parser.add_argument("--checkpoint-size", type=int, default=25)
    parser.add_argument("--delay-seconds", type=float, default=1.0)
    parser.add_argument("--retry-backoff", type=float, default=2.0)
    parser.add_argument("--max-retries", type=int, default=5)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.checkpoint_size <= 0:
        raise ValueError("checkpoint_size must be positive")
    phrases = load_phrase_links(args.input)
    existing_results, phrase_index, cache = load_existing_details(args.output)
    results: List[Dict[str, Any]] = []
    seen_phrases = set()
    for entry in phrases:
        phrase = entry["phrase"]
        if phrase in phrase_index:
            results.append(phrase_index[phrase])
        else:
            record = {"tortured_phrase": phrase, "publications": []}
            phrase_index[phrase] = record
            results.append(record)
        seen_phrases.add(phrase)
    for record in existing_results:
        phrase = record["tortured_phrase"]
        if phrase not in seen_phrases:
            results.append(record)
    client_config = PubPeerClientConfig(
        delay_seconds=args.delay_seconds,
        retry_backoff_seconds=args.retry_backoff,
        max_retries=args.max_retries,
    )
    client = PubPeerClient(client_config)
    enrich_details(phrases, results, phrase_index, cache, client, args.checkpoint_size, args.output)


if __name__ == "__main__":
    main()
