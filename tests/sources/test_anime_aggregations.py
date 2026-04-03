from anibridge_mappings.sources.anime_aggregations import AnimeAggregationsSource


def test_anime_aggregations_collect_helpers() -> None:
    resources = {
        "IMDB": ["tt123", "bad"],
        "MAL": ["1", "x", 2],
        "TMDB": ["tv/10", "movie/20", "movie/x"],
    }

    assert AnimeAggregationsSource._collect_imdb(resources) == ["tt0000123"]
    assert AnimeAggregationsSource._collect_mal(resources) == ["1", "2"]
    assert AnimeAggregationsSource._collect_tmdb(resources) == (["10"], ["20"])


def test_anime_aggregations_build_id_graph_and_metadata() -> None:
    source = AnimeAggregationsSource()
    source._entries = [
        {
            "anime_id": 1,
            "type": "SERIES",
            "resources": {
                "MAL": ["2"],
                "TMDB": ["movie/10"],
                "IMDB": ["tt1234567"],
            },
            "episodes": {
                "REGULAR": [{"length": 24, "air_date": "2020-01-01"}] * 2,
                "SPECIAL": [{"length": 20}],
            },
            "start_date": "2019-05-01",
        }
    ]

    graph = source.build_id_graph()
    assert graph.has_edge(("anidb", "1", "R"), ("mal", "2", None))
    component = graph.get_component(("anidb", "1", "R"))
    assert ("imdb_movie", "tt1234567", None) in component
    assert ("tmdb_movie", "10", None) in component

    import asyncio

    store = asyncio.run(source.collect_metadata(graph))
    regular = store.peek("anidb", "1", "R")
    specials = store.peek("anidb", "1", "S")
    assert regular is not None and regular.episodes == 2 and regular.start_year == 2019
    assert specials is not None and specials.episodes == 1


def test_anime_aggregations_normalizes_movie_metadata_for_inference() -> None:
    source = AnimeAggregationsSource()
    source._entries = [
        {
            "anime_id": 7,
            "type": "MOVIE",
            "resources": {},
            "episodes": {
                "REGULAR": [
                    {"length": 45, "air_date": "1997-07-12"},
                    {"length": 45, "air_date": "1997-07-12"},
                    {"length": 44, "air_date": "1997-07-12"},
                ]
            },
            "start_date": "1997-07-12",
        }
    ]

    import asyncio

    store = asyncio.run(source.collect_metadata(None))  # type: ignore[arg-type]
    regular = store.peek("anidb", "7", "R")

    assert regular is not None
    assert regular.type == "movie" or regular.type.value == "movie"
    assert regular.episodes == 1
    assert regular.duration is None
    assert regular.start_year == 1997
