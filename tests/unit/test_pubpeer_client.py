import html
import json
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


_PUBLICATION_PAYLOAD = {
    "id": 1,
    "title": "Random Forest",
    "abstract": "",
    "pubpeer_id": "AAA111",
    "url": "https://doi.org/10.1000/xyz123",
    "published_at": "2025",
    "comments_total": 1,
    "authors": [
        {
            "first_name": "Ada",
            "last_name": "Lovelace",
            "display_name": "Ada Lovelace",
            "orcid": "",
            "affiliations": ["Analytical Engine Lab"],
        }
    ],
    "journals": [
        {
            "id": 7,
            "title": "Mathematics and Computer Science for Real-World Applications",
        }
    ],
}

_COMMENTS_PAYLOAD = [
    {
        "id": 2,
        "inner_id": 1,
        "html": "<p>Comment</p>",
        "markdown": "Comment",
        "visible": 1,
        "user_alias": "Reviewer",
        "type": "pubpeer",
        "link": None,
        "updatable": False,
        "editable": False,
        "is_from_author": False,
        "important": 0,
        "is_accepted": True,
        "is_disabled": False,
        "accepted_at": "2025-10-07T04:40:50.000000Z",
        "selected_at": None,
        "tweets": [],
        "updates": [],
        "user": {
            "first_name": "reviewer",
            "last_name": "one",
            "display_name": "Reviewer One",
        },
    }
]

DETAIL_HTML = "".join(
    [
        "<html><body>",
        "<publication-page :data-publication=\"",
        html.escape(json.dumps(_PUBLICATION_PAYLOAD)),
        "\">",
        "<comment-timeline :data-comments=\"",
        html.escape(json.dumps(_COMMENTS_PAYLOAD)),
        "\"></comment-timeline>",
        "</publication-page></body></html>",
    ]
)


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


def test_fetch_publication_detail_parses_page() -> None:
    responses = [FakeResponse(200, text=DETAIL_HTML)]
    session = FakeSession(responses)
    client = PubPeerClient(PubPeerClientConfig(delay_seconds=0), session=session)
    detail = client.fetch_publication_detail("https://pubpeer.com/publications/AAA111#0")
    assert detail["pubpeer_id"] == "AAA111"
    assert detail["pubpeer_url"] == "https://pubpeer.com/publications/AAA111"
    assert detail["doi"] == "10.1000/xyz123"
    assert detail["article_url"] == "https://doi.org/10.1000/xyz123"
    assert detail["journal"] == "Mathematics and Computer Science for Real-World Applications"
    assert detail["authors"] == [
        {
            "first_name": "Ada",
            "last_name": "Lovelace",
            "display_name": "Ada Lovelace",
            "orcid": "",
            "affiliations": ["Analytical Engine Lab"],
        }
    ]
    assert detail["comments"][0]["user_alias"] == "Reviewer"


def test_extract_pubpeer_id_handles_variants() -> None:
    client = PubPeerClient(PubPeerClientConfig(delay_seconds=0))
    assert client._extract_pubpeer_id("https://pubpeer.com/publications/XYZ123#0") == "XYZ123"
    assert client._extract_pubpeer_id("https://pubpeer.com/publications/XYZ123") == "XYZ123"
    assert client._extract_pubpeer_id("XYZ123") == "XYZ123"
    assert client._extract_pubpeer_id("https://pubpeer.com/publications/XYZ123/comments") == "XYZ123"
