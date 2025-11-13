from src.pipelines.processing.pubpeer.url_builder import build_search_url


def test_build_search_url_encodes_phrase():
    phrase = '"surface region" AND "surface area"'
    url = build_search_url(phrase)
    expected = "https://pubpeer.com/search?q=%22surface%20region%22%20AND%20%22surface%20area%22"
    assert url == expected


def test_build_search_url_rejects_none():
    try:
        build_search_url(None)  # type: ignore[arg-type]
    except ValueError as error:
        assert str(error) == "phrase must not be None"
    else:
        raise AssertionError("Expected ValueError for None phrase")
