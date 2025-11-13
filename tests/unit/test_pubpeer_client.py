from typing import Any, Dict, List, Optional, Tuple

import requests

from src.pipelines.processing.pubpeer.client import PubPeerClient, PubPeerClientConfig


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        text: str = "",
        json_data: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.status_code = status_code
        self._text = text
        self._json = json_data or {}

    def json(self) -> Dict[str, Any]:
        return self._json

    @property
    def text(self) -> str:
        return self._text

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"status code {self.status_code}")


class FakeSession(requests.Session):
    def __init__(self, responses: List[FakeResponse]) -> None:
        super().__init__()
        self._responses = responses
        self.calls: List[Tuple[str, Optional[Dict[str, Any]]]] = []

    def get(self, url: str, params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> FakeResponse:
        self.calls.append((url, params))
        if not self._responses:
            raise AssertionError("No more responses available")
        return self._responses.pop(0)


def test_fetch_publication_links_handles_relative_urls() -> None:
    html = '<meta name="csrf-token" content="token-abc">'
    payload = {
        "meta": {"total": 2},
        "publications": [
            {"link_with_hash": "/publications/ID1#0"},
            {"pubpeer_id": "ID2"},
        ],
    }
    responses = [
        FakeResponse(200, text=html),
        FakeResponse(200, json_data=payload),
    ]
    session = FakeSession(responses)
    client = PubPeerClient(PubPeerClientConfig(delay_seconds=0), session=session)
    links = list(client.fetch_publication_links("query"))
    assert links == [
        "https://pubpeer.com/publications/ID1#0",
        "https://pubpeer.com/publications/ID2",
    ]
    assert session.calls[0][0] == "https://pubpeer.com"
    assert session.calls[1][0] == "https://pubpeer.com/api/search"
    assert session.calls[1][1]["token"] == "token-abc"
