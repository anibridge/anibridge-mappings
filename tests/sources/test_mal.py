import asyncio

import pytest

from anibridge_mappings.core.graph import IdMappingGraph
from anibridge_mappings.core.meta import SourceMeta, SourceType
from anibridge_mappings.sources.base import CachedMetadataSource
from anibridge_mappings.sources.mal import MalSource


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
        if self.status >= 400 and self.status != 429:
            raise RuntimeError("http error")

    async def json(self) -> dict:
        return self._payload


class _FakeSession:
    def __init__(self, *, get_responses=None, post_responses=None, **kwargs):
        del kwargs
        self._get_responses = get_responses or []
        self._post_responses = post_responses or []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def get(self, url: str, params: dict | None = None, headers: dict | None = None):
        del url, params, headers
        return self._get_responses.pop(0)

    def post(self, url: str, data: dict | None = None):
        del url, data
        return self._post_responses.pop(0)


def test_mal_prepare_fetches_rankings_and_parses_metadata(
    monkeypatch,
    tmp_path,
) -> None:
    CachedMetadataSource.DATA_DIR = tmp_path
    source = MalSource()

    responses = _FakeSession(
        post_responses=[
            _FakeResponse(status=429, payload={}, headers={"Retry-After": "0"}),
            _FakeResponse(status=200, payload={"access_token": "token-123"}),
        ],
        get_responses=[
            _FakeResponse(status=429, payload={}, headers={"Retry-After": "0"}),
            _FakeResponse(
                status=200,
                payload={
                    "data": [
                        {
                            "node": {
                                "id": 1,
                                "title": "Series One",
                                "alternative_titles": {
                                    "en": "Series One EN",
                                    "ja": "シリーズ1",
                                    "synonyms": ["Series 1"],
                                },
                                "start_date": "2020-01-05",
                                "media_type": "tv",
                                "num_episodes": 12,
                                "average_episode_duration": 1440,
                            }
                        },
                        {
                            "node": {
                                "id": 2,
                                "title": "Movie Two",
                                "alternative_titles": {
                                    "synonyms": ["Movie Alt"],
                                },
                                "start_date": "2021-03-01",
                                "media_type": "movie",
                                "num_episodes": 1,
                                "average_episode_duration": 5400,
                            }
                        },
                    ],
                    "paging": {
                        "next": (
                            "https://api.myanimelist.net/v2/anime/ranking?"
                            "ranking_type=all&limit=500&offset=500"
                        )
                    },
                },
            ),
            _FakeResponse(
                status=200,
                payload={
                    "data": [
                        {
                            "node": {
                                "id": 3,
                                "title": "ONA Three",
                                "alternative_titles": {},
                                "start_date": "2022",
                                "media_type": "ona",
                                "num_episodes": 0,
                                "average_episode_duration": 1200,
                            }
                        }
                    ],
                    "paging": {},
                },
            ),
        ],
    )

    monkeypatch.setenv("MAL_CLIENT_ID", "client-id")
    monkeypatch.setenv("MAL_API_KEY", "refresh-token")
    monkeypatch.setattr(
        "anibridge_mappings.sources.mal.aiohttp.ClientSession",
        lambda **kwargs: responses,
    )

    async def _fake_sleep(delay: int) -> None:
        del delay

    monkeypatch.setattr("anibridge_mappings.sources.mal.asyncio.sleep", _fake_sleep)

    asyncio.run(source.prepare())

    meta_1 = source._cache["1"]
    assert meta_1 is not None
    assert meta_1[None].episodes == 12
    assert meta_1[None].type is SourceType.TV
    assert meta_1[None].start_year == 2020
    assert meta_1[None].duration == 24
    assert meta_1[None].titles == (
        "Series One",
        "Series One EN",
        "シリーズ1",
    )

    meta_2 = source._cache["2"]
    assert meta_2 is not None
    assert meta_2[None].episodes == 1
    assert meta_2[None].type is SourceType.MOVIE

    meta_3 = source._cache["3"]
    assert meta_3 is not None
    assert meta_3[None].episodes is None
    assert meta_3[None].type is SourceType.TV


def test_mal_prepare_requires_client_id_for_raw_refresh_token(
    monkeypatch,
    tmp_path,
) -> None:
    CachedMetadataSource.DATA_DIR = tmp_path
    source = MalSource()
    monkeypatch.setenv("MAL_API_KEY", "refresh-token-only")
    monkeypatch.setenv("MAL_CLIENT_ID", "")

    with pytest.raises(RuntimeError, match="MAL metadata fetches require"):
        asyncio.run(source.prepare())


def test_mal_collect_metadata_uses_cached_entries_and_marks_misses(tmp_path) -> None:
    CachedMetadataSource.DATA_DIR = tmp_path
    source = MalSource()
    source._cache = {
        "11": {None: SourceMeta(type=SourceType.TV, episodes=13)},
    }
    source._prepared = True

    graph = IdMappingGraph()
    graph.add_edge(("mal", "11", None), ("anilist", "99", None))
    graph.add_edge(("mal", "12", None), ("tvdb_show", "1", "s1"))

    store = asyncio.run(source.collect_metadata(graph))

    mal_meta = store.peek("mal", "11", None)
    assert mal_meta is not None and mal_meta.episodes == 13
    assert store.peek("mal", "12", None) is None
    assert source._cache["12"] is None
