from anibridge_mappings.sources.anime_offline_database import (
    AnimeOfflineDatabaseEntry,
    AnimeOfflineDatabaseSource,
)


def test_anime_offline_parse_source_and_type() -> None:
    assert AnimeOfflineDatabaseSource._parse_source_string(
        "https://anidb.net/anime/12"
    ) == (
        "anidb",
        "12",
        "R",
    )
    assert AnimeOfflineDatabaseSource._parse_source_string(
        "https://anilist.co/anime/55"
    ) == (
        "anilist",
        "55",
        None,
    )
    assert AnimeOfflineDatabaseSource._parse_source_string("https://invalid") is None

    parsed_movie = AnimeOfflineDatabaseSource._parse_type_string("movie")
    assert parsed_movie is not None and parsed_movie.value == "movie"
    parsed_ova = AnimeOfflineDatabaseSource._parse_type_string("ova")
    assert parsed_ova is not None and parsed_ova.value == "tv"
    assert AnimeOfflineDatabaseSource._parse_type_string("unknown") is None


def test_anime_offline_build_graph_and_metadata() -> None:
    source = AnimeOfflineDatabaseSource()
    source._prepared = True
    source._entries = [
        AnimeOfflineDatabaseEntry.model_validate(
            {
                "sources": [
                    "https://anidb.net/anime/12",
                    "https://anilist.co/anime/55",
                    "https://myanimelist.net/anime/33",
                ],
                "title": "x",
                "type": "TV",
                "episodes": 24,
                "animeSeason": {"year": 2021, "season": "SPRING"},
            }
        )
    ]

    graph = source.build_id_graph()
    assert graph.has_edge(("anidb", "12", "R"), ("anilist", "55", None))

    import asyncio

    store = asyncio.run(source.collect_metadata(graph))
    meta = store.peek("anidb", "12", "R")
    assert meta is not None
    assert meta.episodes == 24
    assert meta.start_year == 2021
    assert meta.titles == ("x",)
