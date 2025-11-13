from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd
from requests import RequestException
from tqdm import tqdm

ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from pipelines.processing.pubpeer.client import PubPeerClient, PubPeerClientConfig
from pipelines.processing.pubpeer.storage import read_json_list, write_json_list


def load_phrases(csv_path: Path, column: str, limit: int | None) -> List[str]:
    frame = pd.read_csv(csv_path)
    if column not in frame.columns:
        raise ValueError(f"Column '{column}' not found in {csv_path}")
    phrases = frame[column].dropna().astype(str).map(str.strip)
    phrases = phrases[phrases != ""]
    unique_phrases = phrases.drop_duplicates().tolist()
    if limit is None:
        return unique_phrases
    return unique_phrases[:limit]


def write_results(output_path: Path, results: List[Dict[str, object]]) -> None:
    write_json_list(output_path, results)


def load_existing(output_path: Path) -> tuple[list[Dict[str, object]], set[str]]:
    if not output_path.exists():
        return [], set()
    data = read_json_list(output_path)
    processed = set()
    cleaned: List[Dict[str, object]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        phrase = item.get("tortured_phrase")
        if isinstance(phrase, str):
            processed.add(phrase)
        cleaned.append(item)
    return cleaned, processed


def scrape(
    query_terms: List[str],
    output_path: Path,
    client_config: PubPeerClientConfig,
    checkpoint_size: int,
) -> None:
    if checkpoint_size <= 0:
        raise ValueError("checkpoint_size must be positive")
    existing_results, processed_phrases = load_existing(output_path)
    client = PubPeerClient(client_config)
    results = list(existing_results)
    new_items = 0
    remaining = [phrase for phrase in query_terms if phrase not in processed_phrases]
    total = len(query_terms)
    progress = tqdm(total=total, desc="Scraping PubPeer", unit="phrase")
    progress.update(len(processed_phrases))
    for phrase in remaining:
        try:
            links = list(client.fetch_publication_links(phrase))
            results.append({"tortured_phrase": phrase, "pubpeer_links": links})
        except RequestException as error:
            tqdm.write(f"Error for phrase '{phrase}': {error}")
            results.append({"tortured_phrase": phrase, "pubpeer_links": [], "error": str(error)})
        except Exception as error:
            tqdm.write(f"Unexpected error for phrase '{phrase}': {error}")
            results.append({"tortured_phrase": phrase, "pubpeer_links": [], "error": str(error)})
        processed_phrases.add(phrase)
        new_items += 1
        progress.update(1)
        if new_items % checkpoint_size == 0:
            write_results(output_path, results)
    if new_items or not output_path.exists():
        write_results(output_path, results)
    progress.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=Path("data/tortured.csv"))
    parser.add_argument("--column", default="tortured_phrase")
    parser.add_argument("--output", type=Path, default=Path("results/pubpeer_results.json"))
    parser.add_argument("--max-results", type=int, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--checkpoint-size", type=int, default=50)
    parser.add_argument("--delay-seconds", type=float, default=1.0)
    parser.add_argument("--retry-backoff", type=float, default=2.0)
    parser.add_argument("--max-retries", type=int, default=5)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    phrases = load_phrases(args.input, args.column, args.limit)
    client_config = PubPeerClientConfig(
        max_results=args.max_results,
        delay_seconds=args.delay_seconds,
        retry_backoff_seconds=args.retry_backoff,
        max_retries=args.max_retries,
    )
    scrape(phrases, args.output, client_config, args.checkpoint_size)


if __name__ == "__main__":
    main()
