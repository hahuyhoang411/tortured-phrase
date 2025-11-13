from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, Optional, Tuple

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
