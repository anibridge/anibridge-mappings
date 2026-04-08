import asyncio
from datetime import UTC, datetime, timedelta

import aiohttp
import pytest

from anibridge_mappings.core.meta import SourceMeta
from anibridge_mappings.sources.tvdb import (
    BaseTvdbSource,
    TvdbMovieSource,
    TvdbShowSource,
)


class _FakeResponse:
    def __init__(self, *, status: int, payload: dict, headers: dict | None = None):
        self.status = status
        self._payload = payload
        self.headers = headers or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def raise_for_status(self) -> None:
        if self.status >= 400 and self.status != 429 and self.status != 404:
            from aiohttp import RequestInfo
            from yarl import URL

            info = RequestInfo(
                url=URL("http://test"),
                method="GET",
                headers={},  # type: ignore
            )
            raise aiohttp.ClientResponseError(info, (), status=self.status)

    async def json(self) -> dict:
        return self._payload


class _FakeSession:
    def __init__(self, get_responses=None, post_responses=None):
        self._get_responses = get_responses or []
        self._post_responses = post_responses or []

    def get(self, url):
        del url
        return self._get_responses.pop(0)

    def post(self, url, json):
        del url, json
        return self._post_responses.pop(0)


class DummyTvdbSource(BaseTvdbSource):
    provider_key = "tvdb_show"
    cache_filename = "dummy_tvdb.json"

    async def _fetch_entry(self, session, entry_id, scope):
        raise AssertionError


def test_tvdb_parse_helpers() -> None:
    assert BaseTvdbSource._parse_runtime(24) == 24
    assert BaseTvdbSource._parse_runtime(-1) is None
    assert BaseTvdbSource._parse_year("2020-01-01") == 2020
    assert BaseTvdbSource._scope_from_season(3) == "s3"

    dt = BaseTvdbSource._extract_air_date({"aired": "2020-05-01"})
    assert dt is not None and dt.tzinfo == UTC

    assert BaseTvdbSource._extract_finale_type({"finaleType": "season"}) == "season"
    assert BaseTvdbSource._extract_season_number({"seasonNumber": 2}) == 2


def test_tvdb_build_show_scope_meta_marks_recent_incomplete() -> None:
    source = DummyTvdbSource()
    recent = (datetime.now(UTC) - timedelta(days=10)).date().isoformat()

    episodes = [
        {"seasonNumber": 1, "aired": "2010-01-01", "finaleType": "season"},
        {"seasonNumber": 1, "aired": "2010-01-08"},
        {"seasonNumber": 2, "aired": recent},
    ]

    scope_meta = source._build_show_scope_meta(episodes, 24, ["Show Title"])
    assert scope_meta["s1"].episodes == 2
    assert scope_meta["s2"].episodes is None
    assert scope_meta["s1"].titles == ("Show Title",)


def test_tvdb_movie_and_show_response_parsing() -> None:
    movie_source = TvdbMovieSource()
    movie = movie_source._build_movie_meta(110, "2022-01-01", ["Movie Title"])
    assert movie.episodes == 1
    assert movie.start_year == 2022
    assert movie.titles == ("Movie Title",)


def test_tvdb_extract_helpers_cover_edge_cases() -> None:
    assert BaseTvdbSource._extract_season_number({"seasonNumber": 4}) == 4
    assert BaseTvdbSource._extract_season_number({}) is None

    assert BaseTvdbSource._extract_air_year({"aired": "2021-02-03"}) == 2021
    assert BaseTvdbSource._extract_air_year({"aired": "bad"}) is None

    assert (
        BaseTvdbSource._extract_air_date({"aired": "2022-01-01T00:00:00+00:00"})
        is not None
    )
    assert BaseTvdbSource._extract_air_date({"aired": "not-a-date"}) is None

    assert BaseTvdbSource._extract_finale_type({"finaleType": "series"}) == "series"
    assert BaseTvdbSource._extract_finale_type({"finaleType": "other"}) is None


def test_tvdb_request_json_and_token_branches(monkeypatch) -> None:
    source = DummyTvdbSource()

    async def _fake_sleep(delay: int) -> None:
        del delay

    monkeypatch.setattr("anibridge_mappings.sources.tvdb.asyncio.sleep", _fake_sleep)

    # request_json: 429 retry then success
    session = _FakeSession(
        get_responses=[
            _FakeResponse(status=429, payload={}, headers={"Retry-After": "0"}),
            _FakeResponse(status=200, payload={"ok": True}),
        ]
    )
    payload, cacheable = asyncio.run(source._request_json(session, "http://x"))  # type: ignore
    assert payload == {"ok": True}
    assert cacheable is True

    # request_json: 404 treated as cacheable miss
    session_404 = _FakeSession(get_responses=[_FakeResponse(status=404, payload={})])
    payload_404, cacheable_404 = asyncio.run(
        source._request_json(session_404, "http://x")  # type: ignore
    )
    assert payload_404 is None and cacheable_404 is True

    # token fetch: 429 then success
    monkeypatch.setenv("TVDB_API_KEY", "k")
    token_session = _FakeSession(
        post_responses=[
            _FakeResponse(status=429, payload={}, headers={"Retry-After": "0"}),
            _FakeResponse(status=200, payload={"data": {"token": "abc"}}),
        ]
    )
    token = asyncio.run(source._get_or_fetch_token(token_session))  # type: ignore
    assert token == "abc"


def test_tvdb_get_or_fetch_token_requires_api_key(monkeypatch) -> None:
    source = DummyTvdbSource()
    monkeypatch.setattr(source, "_get_api_key", lambda: None)

    with pytest.raises(RuntimeError, match="TVDB_API_KEY is required"):
        asyncio.run(source._get_or_fetch_token(_FakeSession()))  # type: ignore


def test_tvdb_prepare_raises_when_token_fetch_fails(monkeypatch) -> None:
    monkeypatch.setenv("TVDB_API_KEY", "k")
    source = DummyTvdbSource()

    class _PrepareSession:
        async def __aenter__(self):
            return _FakeSession(post_responses=[_FakeResponse(status=200, payload={})])

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(
        "anibridge_mappings.sources.tvdb.aiohttp.ClientSession", _PrepareSession
    )

    with pytest.raises(RuntimeError, match="TVDB login response missing token"):
        asyncio.run(source.prepare())


def test_tvdb_show_and_movie_fetch_entry_parsing(monkeypatch) -> None:
    show_source = TvdbShowSource()

    async def _fake_show_meta(session, base_id):
        del session, base_id
        return ({"s1": SourceMeta(episodes=12), "s2": SourceMeta(episodes=6)}, True)

    monkeypatch.setattr(show_source, "_get_or_fetch_show_meta", _fake_show_meta)

    class _FakeSess1:
        pass

    entry, scoped, cacheable = asyncio.run(
        show_source._fetch_entry(_FakeSess1(), "77", "s1")  # type: ignore
    )
    assert entry == "77"
    assert cacheable is True
    assert scoped == {"s1": SourceMeta(episodes=12), "s2": SourceMeta(episodes=6)}

    movie_source = TvdbMovieSource()

    async def _fake_movie_payload(session, base_id):
        del session, base_id
        return (
            {
                "data": {
                    "runtime": 95,
                    "year": "2020",
                    "name": "Movie Name",
                }
            },
            True,
        )

    monkeypatch.setattr(movie_source, "_request_movie_payload", _fake_movie_payload)

    class _FakeSess2:
        pass

    entry_id, scope_meta, cacheable = asyncio.run(
        movie_source._fetch_entry(_FakeSess2(), "88", None)  # type: ignore
    )
    assert entry_id == "88"
    assert cacheable is True
    assert scope_meta is not None
    assert scope_meta[None].episodes == 1
    assert scope_meta[None].duration == 95
    assert scope_meta[None].titles == ("Movie Name",)
