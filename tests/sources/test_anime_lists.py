from lxml import etree

from anibridge_mappings.core.meta import MetaStore, SourceMeta, SourceType
from anibridge_mappings.sources.anime_lists import AnimeListsSource

XML = """
<root>
  <anime anidbid="1" tvdbid="100" defaulttvdbseason="1" tmdbtv="200" tmdbseason="1" tmdbid="300" imdbid="tt1234567">
    <mapping-list>
      <mapping anidbseason="1" tvdbseason="1">1-1;2-2</mapping>
        <mapping anidbseason="1" tvdbseason="2" tmdbseason="2">3-1;4-2</mapping>
    </mapping-list>
  </anime>
</root>
"""  # noqa: E501


def _prepared_source() -> AnimeListsSource:
    source = AnimeListsSource()
    source._data = etree.fromstring(XML)
    return source


def test_anime_lists_static_helpers() -> None:
    assert AnimeListsSource._scope_from_attr("A") == "s1"
    assert AnimeListsSource._anidb_scope_from_attr("special") == "S"
    assert AnimeListsSource._episode_key(" movie ") == "1"
    assert AnimeListsSource._safe_int("x") == 0
    assert AnimeListsSource._split_ids("1, 2") == ["1", "2"]
    assert AnimeListsSource._split_imdb_ids("tt1234567,bad") == ["tt1234567"]

    mapping_el = etree.Element("mapping", start="2", end="3", offset="1")
    assert AnimeListsSource._parse_offset_pairs(mapping_el) == [
        ("2", ["3"]),
        ("3", ["4"]),
    ]


def test_anime_lists_build_id_graph_and_episode_graph() -> None:
    source = _prepared_source()
    id_graph = source.build_id_graph()

    assert id_graph.has_edge(("anidb", "1", "R"), ("tvdb_show", "100", "s1"))
    assert id_graph.has_edge(("anidb", "1", "R"), ("tvdb_show", "100", "s2"))
    assert id_graph.has_edge(("anidb", "1", "R"), ("tmdb_show", "200", "s1"))
    assert id_graph.has_edge(("anidb", "1", "R"), ("tmdb_show", "200", "s2"))
    assert id_graph.has_edge(("anidb", "1", "R"), ("imdb_movie", "tt1234567", None))

    store = MetaStore()
    store.set("anidb", "1", SourceMeta(type=SourceType.TV, episodes=2), "R")

    ep_graph = source.build_episode_graph(store, id_graph)
    assert ep_graph.has_edge(("anidb", "1", "R", "1"), ("tvdb_show", "100", "s1", "1"))
    assert ep_graph.has_edge(("anidb", "1", "R", "2"), ("tvdb_show", "100", "s1", "2"))


def test_anime_lists_offset_segments_and_movie_targets() -> None:
    source = _prepared_source()

    assert source._build_offset_segments(3, 1) == [(1, 3, 2, 4)]
    assert source._apply_episode_offset("1", -1) is None

    anime_movie = etree.Element(
        "anime",
        anidbid="9",
        tmdbid="333,444",
        imdbid="tt0000123",
        tvdbid="movie",
    )
    assert source._collect_movie_targets(anime_movie) == [
        ("tmdb_movie", "333"),
        ("tmdb_movie", "444"),
        ("imdb_movie", "tt0000123"),
    ]
