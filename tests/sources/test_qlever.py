import asyncio

import pytest

from anibridge_mappings.core.graph import IdMappingGraph
from anibridge_mappings.sources.qlever import (
    QleverImdbMovieSource,
    QleverImdbShowSource,
    QleverWikidataSource,
)


def test_qlever_build_query_and_extractors() -> None:
    source = QleverImdbMovieSource(batch_size=2)
    query, normalized = source._build_query(["tt123", "456", "bad"])

    assert "VALUES ?id" in query
    assert "tt0000123" in normalized
    assert "tt0000456" in normalized

    binding = {"id": {"value": "tt0000123"}, "runtimeMinutes": {"value": "120"}}
    assert source._extract_str(binding, "id") == "tt0000123"
    assert source._extract_int(binding, "runtimeMinutes") == 120


def test_qlever_parse_bindings_for_movie_and_show() -> None:
    movie = QleverImdbMovieSource()
    show = QleverImdbShowSource()

    bindings = [
        {
            "id": {"value": "tt0000123"},
            "type": {"value": "movie"},
            "startYear": {"value": "2020"},
            "runtimeMinutes": {"value": "100"},
            "primaryTitle": {"value": "Movie Title"},
            "originalTitle": {"value": "Original Movie Title"},
        },
        {
            "id": {"value": "tt0000456"},
            "type": {"value": "tvSeries"},
            "episodeCount": {"value": "12"},
            "runtimeMinutes": {"value": "24"},
            "primaryTitle": {"value": "Show Title"},
        },
    ]

    movie_meta = movie._parse_bindings(bindings, {"tt0000123": ["tt123"]})
    show_meta = show._parse_bindings(bindings, {"tt0000456": ["tt456"]})

    assert movie_meta["tt123"].episodes == 1
    assert movie_meta["tt123"].titles == ("Movie Title", "Original Movie Title")
    assert show_meta["tt456"].episodes == 12
    assert show_meta["tt456"].titles == ("Show Title",)


def test_qlever_wikidata_build_id_graph_and_prop_extraction() -> None:
    source = QleverWikidataSource()
    source._prepared = True
    source._bindings = [
        {
            "item": {"value": "q1"},
            "prop": {"value": "https://www.wikidata.org/prop/direct/P5646"},
            "id": {"value": "12"},
        },
        {
            "item": {"value": "q1"},
            "prop": {"value": "https://www.wikidata.org/prop/direct/P8729"},
            "id": {"value": "99"},
        },
    ]

    graph = source.build_id_graph()
    assert graph.has_edge(("anidb", "12", None), ("anilist", "99", None))
    assert source._extract_prop_code(source._bindings[0]) == "P5646"


def test_qlever_collect_metadata_requires_prepare_and_uses_fetch(monkeypatch) -> None:
    source = QleverImdbShowSource()

    with pytest.raises(RuntimeError):
        asyncio.run(source.collect_metadata(IdMappingGraph()))

    asyncio.run(source.prepare())

    async def _fake_fetch(entry_ids):
        assert entry_ids == [("tt0000123", None)]
        from anibridge_mappings.core.meta import SourceMeta, SourceType

        return [("tt0000123", {None: SourceMeta(type=SourceType.TV, episodes=7)})]

    monkeypatch.setattr(source, "_fetch_entries", _fake_fetch)

    graph = IdMappingGraph()
    graph.add_edge(("imdb_show", "tt0000123", None), ("mal", "1", None))
    store = asyncio.run(source.collect_metadata(graph))

    imdb_meta = store.peek("imdb_show", "tt0000123", None)
    assert imdb_meta is not None and imdb_meta.episodes == 7
