from anibridge_mappings.core.meta import MetaStore, SourceMeta, SourceType
from anibridge_mappings.sources.shinkro import (
    BaseShinkroMappingSource,
    ShinkroTmdbMappingSource,
    ShinkroTvdbMappingSource,
)


def test_base_shinkro_normalizers() -> None:
    assert BaseShinkroMappingSource._normalize_id("12") == "12"
    assert BaseShinkroMappingSource._normalize_id("0") is None


def test_shinkro_tvdb_helpers_and_graphs() -> None:
    source = ShinkroTvdbMappingSource()
    source._prepared = True
    source._entries = [
        {
            "malid": "100",
            "tvdbid": 200,
            "tvdbseason": 1,
            "start": 1,
            "useMapping": False,
        },
        {
            "malid": "101",
            "tvdbid": 201,
            "useMapping": True,
            "animeMapping": [
                {
                    "tvdbseason": 2,
                    "mappingType": "explicit",
                    "explicitEpisodes": {"1": "3", "2": "4"},
                }
            ],
        },
    ]

    id_graph = source.build_id_graph()
    assert id_graph.has_edge(("mal", "100", None), ("tvdb_show", "200", "s1"))
    assert id_graph.has_edge(("mal", "101", None), ("tvdb_show", "201", "s2"))

    store = MetaStore()
    store.set("tvdb_show", "200", SourceMeta(type=SourceType.TV, episodes=2), "s1")
    store.set("tvdb_show", "201", SourceMeta(type=SourceType.TV, episodes=10), "s2")

    ep_graph = source.build_episode_graph(store, id_graph)
    assert ep_graph.has_edge(
        ("tvdb_show", "200", "s1", "1-2"), ("mal", "100", None, "1-2")
    )
    assert ep_graph.has_edge(
        ("tvdb_show", "201", "s2", "1-2"), ("mal", "101", None, "3-4")
    )


def test_shinkro_tmdb_graphs() -> None:
    source = ShinkroTmdbMappingSource()
    source._prepared = True
    source._entries = [{"malid": "9", "tmdbid": 77}]

    id_graph = source.build_id_graph()
    assert id_graph.has_edge(("tmdb_movie", "77", None), ("mal", "9", None))

    ep_graph = source.build_episode_graph(MetaStore(), id_graph)
    assert ep_graph.has_edge(("tmdb_movie", "77", None, "1"), ("mal", "9", None, "1"))
