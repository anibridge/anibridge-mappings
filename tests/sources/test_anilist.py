import asyncio

from anibridge_mappings.core.graph import IdMappingGraph
from anibridge_mappings.sources.anilist import AnilistSource


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
    def __init__(self, responses: list[_FakeResponse], **kwargs):
        del kwargs
        self._responses = responses

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def post(self, url: str, json: dict):
        del url, json
        return self._responses.pop(0)


def test_anilist_session_kwargs_and_batch_size() -> None:
    source = AnilistSource(batch_size=0)
    kwargs = source._session_kwargs()

    assert source._batch_size == 1
    assert kwargs["headers"]["Content-Type"] == "application/json"


def test_anilist_fetch_missing_parses_results_and_handles_rate_limit(
    monkeypatch,
) -> None:
    source = AnilistSource(batch_size=2)

    responses = [
        _FakeResponse(status=429, payload={}, headers={"Retry-After": "0"}),
        _FakeResponse(
            status=200,
            payload={
                "data": {
                    "batch1": {
                        "media": [
                            {
                                "id": 1,
                                "episodes": 12,
                                "format": "TV",
                                "seasonYear": 2020,
                                "duration": 24,
                                "title": {
                                    "romaji": "Title One",
                                    "english": "Title One EN",
                                    "native": None,
                                    "userPreferred": "Title One",
                                },
                                "synonyms": ["Title One Alt"],
                            },
                            {
                                "id": 2,
                                "episodes": 1,
                                "format": "MOVIE",
                                "seasonYear": 2021,
                                "duration": 100,
                                "title": {
                                    "romaji": "Movie Two",
                                    "english": None,
                                    "native": None,
                                    "userPreferred": "Movie Two",
                                },
                                "synonyms": [],
                            },
                        ]
                    }
                }
            },
        ),
    ]

    monkeypatch.setattr(
        "anibridge_mappings.sources.anilist.aiohttp.ClientSession",
        lambda **kwargs: _FakeSession(responses, **kwargs),
    )

    async def _fake_sleep(delay: int) -> None:
        del delay

    monkeypatch.setattr("anibridge_mappings.sources.anilist.asyncio.sleep", _fake_sleep)

    result = asyncio.run(source._fetch_missing([("1", None), ("2", None), ("3", None)]))

    by_id = {entry_id: scope for entry_id, scope, _cacheable in result}
    scope_1 = by_id["1"]
    scope_2 = by_id["2"]
    assert scope_1 is not None and scope_1[None].episodes == 12
    assert scope_2 is not None and scope_2[None].episodes == 1
    assert scope_1[None].titles == ("Title One", "Title One EN")
    meta_2 = scope_2[None]
    assert meta_2 is not None
    assert meta_2.type is not None and meta_2.type.value == "movie"
    assert meta_2.titles == ("Movie Two",)
    assert by_id["3"] is None


def test_anilist_collect_metadata_uses_eligible_ids(monkeypatch) -> None:
    source = AnilistSource()
    asyncio.run(source.prepare())

    async def _fake_fetch_missing(entry_ids):
        return [
            (entry_id, {None: source_meta}, True)
            for entry_id, _scope in entry_ids
            for source_meta in []
        ]

    # one valid and one invalid provider to exercise _eligible_ids filtering
    graph = IdMappingGraph()
    graph.add_edge(("anilist", "11", None), ("mal", "9", None))

    async def _fixed_fetch(entry_ids):
        assert entry_ids == [("11", None)]
        from anibridge_mappings.core.meta import SourceMeta, SourceType

        return [("11", {None: SourceMeta(type=SourceType.TV, episodes=13)}, True)]

    monkeypatch.setattr(source, "_fetch_missing", _fixed_fetch)

    store = asyncio.run(source.collect_metadata(graph))
    anilist_meta = store.peek("anilist", "11", None)
    assert anilist_meta is not None and anilist_meta.episodes == 13
