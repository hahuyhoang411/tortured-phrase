from __future__ import annotations

import html
import json
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple
from urllib.parse import urlparse

import requests

META_CSRF_PATTERN = re.compile(r'<meta[^>]+name="csrf-token"[^>]+content="([^"]+)"', re.IGNORECASE)


@dataclass
class PubPeerClientConfig:
    base_url: str = "https://pubpeer.com"
    timeout_seconds: float = 30.0
    delay_seconds: float = 1.0
    max_per_page: int = 40
    max_results: Optional[int] = None
    max_retries: int = 5
    retry_backoff_seconds: float = 2.0
    user_agent: str = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    )


class PubPeerClient:
    def __init__(
        self,
        config: Optional[PubPeerClientConfig] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.config = config or PubPeerClientConfig()
        self._external_session = session is not None
        self._session = session or self._create_session()
        if session is not None:
            session.headers.setdefault("User-Agent", self.config.user_agent)
        self._csrf_token: Optional[str] = None

    def fetch_publications(self, query: str) -> Iterator[Dict]:
        token = self._get_token()
        collected = 0
        offset = 0
        total: Optional[int] = None
        while True:
            payload, token = self._request_with_retry(query, token, offset)
            publications = payload.get("publications", [])
            if total is None:
                total = payload.get("meta", {}).get("total")
            if not publications:
                break
            for item in publications:
                yield item
                collected += 1
                if self.config.max_results is not None and collected >= self.config.max_results:
                    return
            offset += len(publications)
            if total is not None and offset >= total:
                break
            if self.config.max_results is not None and offset >= self.config.max_results:
                break
            time.sleep(self.config.delay_seconds)

    def fetch_publication_links(self, query: str) -> Iterator[str]:
        for item in self.fetch_publications(query):
            link = item.get("link_with_hash")
            if link:
                if link.startswith("http"):
                    yield link
                else:
                    yield f"{self._base_url}{link}"
            else:
                identifier = item.get("pubpeer_id")
                if identifier:
                    yield f"{self._base_url}/publications/{identifier}"

    def fetch_publication_records(self, query: str) -> Iterable[Dict]:
        return list(self.fetch_publications(query))

    def fetch_publication_detail(self, reference: str) -> Dict[str, Any]:
        pubpeer_id = self._extract_pubpeer_id(reference)
        page = self._fetch_publication_page(pubpeer_id)
        normalized_link = f"{self._base_url}/publications/{pubpeer_id}"
        detail = self._parse_publication_page(page, pubpeer_id, normalized_link, reference)
        time.sleep(self.config.delay_seconds)
        return detail

    def get_publication_id(self, reference: str) -> str:
        return self._extract_pubpeer_id(reference)

    def _get_token(self) -> str:
        if self._csrf_token is None:
            return self._refresh_token()
        return self._csrf_token

    def _refresh_token(self, force: bool = False) -> str:
        if force:
            self._csrf_token = None
        response = self._session.get(self._base_url, timeout=self.config.timeout_seconds)
        response.raise_for_status()
        match = META_CSRF_PATTERN.search(response.text)
        if not match:
            raise RuntimeError("Unable to locate CSRF token in PubPeer landing page")
        self._csrf_token = match.group(1)
        return self._csrf_token

    @property
    def _base_url(self) -> str:
        return self.config.base_url.rstrip("/")

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({"User-Agent": self.config.user_agent})
        return session

    def _reset_session(self) -> None:
        if self._external_session:
            return
        self._session.close()
        self._session = self._create_session()

    def _extract_pubpeer_id(self, reference: str) -> str:
        if reference is None:
            raise ValueError("reference must not be None")
        candidate = reference.strip()
        if not candidate:
            raise ValueError("reference must not be empty")
        if candidate.startswith("http://") or candidate.startswith("https://"):
            parsed = urlparse(candidate)
            path = parsed.path.strip("/")
            segments = path.split("/") if path else []
            if "publications" in segments:
                index = segments.index("publications")
                if index + 1 >= len(segments):
                    raise ValueError(f"Unable to extract pubpeer id from {reference}")
                candidate = segments[index + 1]
            elif segments:
                candidate = segments[-1]
        if "#" in candidate:
            candidate = candidate.split("#", 1)[0]
        if not candidate:
            raise ValueError(f"Unable to extract pubpeer id from {reference}")
        return candidate

    def _fetch_publication_page(self, pubpeer_id: str) -> str:
        attempts = 0
        last_exception: Optional[BaseException] = None
        while attempts < self.config.max_retries:
            try:
                response = self._session.get(
                    f"{self._base_url}/publications/{pubpeer_id}",
                    timeout=self.config.timeout_seconds,
                )
            except requests.RequestException as error:
                last_exception = error
                self._reset_session()
                attempts += 1
                time.sleep(self.config.retry_backoff_seconds * attempts)
                continue
            if response.status_code in {403, 429, 500, 502, 503, 504}:
                self._reset_session()
                attempts += 1
                time.sleep(self.config.retry_backoff_seconds * attempts)
                continue
            try:
                response.raise_for_status()
            except requests.HTTPError as error:
                last_exception = error
                self._reset_session()
                attempts += 1
                time.sleep(self.config.retry_backoff_seconds * attempts)
                continue
            return response.text
        if last_exception is not None:
            raise last_exception
        raise RuntimeError("Exceeded maximum retries for PubPeer request")

    def _parse_publication_page(
        self,
        page: str,
        pubpeer_id: str,
        normalized_link: str,
        reference: str,
    ) -> Dict[str, Any]:
        publication_payload = self._extract_embedded_json(page, "data-publication")
        if publication_payload is None:
            raise RuntimeError("Unable to locate publication payload")
        comments_payload = self._extract_embedded_json(page, "data-comments")
        comments = comments_payload if isinstance(comments_payload, list) else []
        article_url = self._normalize_url(publication_payload.get("url"))
        doi = self._extract_doi(article_url)
        journal = self._extract_journal(publication_payload.get("journals"))
        authors = self._extract_authors(publication_payload.get("authors"))
        return {
            "pubpeer_id": pubpeer_id,
            "pubpeer_url": normalized_link,
            "source_reference": reference,
            "title": publication_payload.get("title"),
            "abstract": publication_payload.get("abstract") or None,
            "published_at": publication_payload.get("published_at"),
            "doi": doi,
            "article_url": article_url,
            "comments_total": publication_payload.get("comments_total"),
            "journal": journal,
            "authors": authors,
            "comments": comments,
        }

    def _extract_embedded_json(self, page: str, attribute: str) -> Optional[Any]:
        pattern = re.compile(rf":{attribute}=\"([^\"]*)\"", re.DOTALL)
        match = pattern.search(page)
        if not match:
            return None
        raw = html.unescape(match.group(1))
        if not raw:
            return None
        return json.loads(raw)

    def _extract_doi(self, article_url: Optional[str]) -> Optional[str]:
        if article_url is None:
            return None
        lowered = article_url.lower()
        prefixes = [
            "https://doi.org/",
            "http://doi.org/",
            "https://dx.doi.org/",
            "http://dx.doi.org/",
        ]
        for prefix in prefixes:
            if lowered.startswith(prefix):
                return article_url[len(prefix) :]
        return None

    def _extract_journal(self, journals: Any) -> Optional[str]:
        if not isinstance(journals, list) or not journals:
            return None
        first = journals[0]
        if not isinstance(first, dict):
            return None
        title = first.get("title")
        return title if isinstance(title, str) else None

    def _extract_authors(self, authors: Any) -> List[Dict[str, Any]]:
        if not isinstance(authors, list):
            return []
        results: List[Dict[str, Any]] = []
        for author in authors:
            if isinstance(author, dict):
                results.append(
                    {
                        "first_name": author.get("first_name"),
                        "last_name": author.get("last_name"),
                        "display_name": author.get("display_name"),
                        "orcid": author.get("orcid"),
                        "affiliations": author.get("affiliations"),
                    }
                )
        return results

    def _normalize_url(self, value: Optional[str]) -> Optional[str]:
        if not isinstance(value, str):
            return None
        trimmed = value.strip()
        return trimmed if trimmed else None

    def _request_with_retry(self, query: str, token: str, offset: int) -> Tuple[Dict[str, Any], str]:
        attempts = 0
        last_exception: Optional[BaseException] = None
        while attempts < self.config.max_retries:
            params: Dict[str, Any] = {"q": query, "token": token, "type": "publications"}
            if offset:
                params["from"] = offset
            try:
                response = self._session.get(
                    f"{self._base_url}/api/search",
                    params=params,
                    timeout=self.config.timeout_seconds,
                )
            except requests.RequestException as error:
                last_exception = error
                self._reset_session()
                token = self._refresh_token(force=True)
                attempts += 1
                time.sleep(self.config.retry_backoff_seconds * attempts)
                continue
            if response.status_code == 403:
                token = self._refresh_token(force=True)
                attempts += 1
                time.sleep(self.config.retry_backoff_seconds * attempts)
                continue
            if response.status_code in {429, 500, 502, 503, 504}:
                self._reset_session()
                token = self._refresh_token(force=True)
                attempts += 1
                time.sleep(self.config.retry_backoff_seconds * attempts)
                continue
            try:
                response.raise_for_status()
            except requests.HTTPError as error:
                last_exception = error
                self._reset_session()
                token = self._refresh_token(force=True)
                attempts += 1
                time.sleep(self.config.retry_backoff_seconds * attempts)
                continue
            return response.json(), token
        if last_exception is not None:
            raise last_exception
        raise RuntimeError("Exceeded maximum retries for PubPeer request")
