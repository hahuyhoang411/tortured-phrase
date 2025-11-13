from urllib.parse import quote

BASE_SEARCH_URL = "https://pubpeer.com/search"


def build_search_url(phrase: str) -> str:
    if phrase is None:
        raise ValueError("phrase must not be None")
    encoded = quote(phrase, safe="")
    return f"{BASE_SEARCH_URL}?q={encoded}"
