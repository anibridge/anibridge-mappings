from anibridge_mappings.core.graph import IdMappingGraph
from anibridge_mappings.core.inference import (
    _episode_range,
    _normalize_title,
    _relative_delta,
    _title_score,
    infer_episode_mappings,
)
from anibridge_mappings.core.meta import MetaStore, SourceMeta, SourceType


def test_inference_skips_when_provider_has_many_same_length_candidates() -> None:
    id_graph = IdMappingGraph()
    special = ("anidb", "2036", "S")
    main = ("anidb", "69", "R")
    a = ("anilist", "1238", None)
    b = ("anilist", "2490", None)
    id_graph.add_equivalence_class([special, main, a, b])

    store = MetaStore()
    store.set(
        special[0],
        special[1],
        SourceMeta(type=SourceType.TV, episodes=1),
        special[2],
    )
    store.set(
        main[0],
        main[1],
        SourceMeta(
            type=SourceType.TV,
            episodes=100,
            start_year=1999,
            titles=("One Piece",),
        ),
        main[2],
    )
    store.set(
        a[0],
        a[1],
        SourceMeta(
            type=SourceType.TV,
            episodes=1,
            duration=45,
            start_year=2003,
            titles=("One Piece Special 3",),
        ),
        a[2],
    )
    store.set(
        b[0],
        b[1],
        SourceMeta(
            type=SourceType.TV,
            episodes=1,
            duration=5,
            start_year=2004,
            titles=("One Piece: Pirate Baseball King",),
        ),
        b[2],
    )

    episode_graph = infer_episode_mappings(store, id_graph)

    assert episode_graph.node_count() == 0


def test_infer_episode_mappings_links_matching_component_nodes() -> None:
    id_graph = IdMappingGraph()
    a = ("anidb", "1", "R")
    b = ("anilist", "2", None)
    c = ("mal", "3", None)
    id_graph.add_equivalence_class([a, b, c])

    store = MetaStore()
    shared = SourceMeta(
        type=SourceType.TV,
        episodes=12,
        duration=24,
        start_year=2020,
        titles=("Orbital Children",),
    )
    store.set(a[0], a[1], shared, a[2])
    store.set(b[0], b[1], shared, b[2])
    store.set(
        c[0],
        c[1],
        SourceMeta(
            type=SourceType.TV,
            episodes=24,
            duration=24,
            start_year=2020,
            titles=("Different Show",),
        ),
        c[2],
    )

    episode_graph = infer_episode_mappings(store, id_graph)

    assert episode_graph.has_edge(
        ("anidb", "1", "R", "1-12"), ("anilist", "2", None, "1-12")
    )
    assert not episode_graph.has_edge(
        ("anidb", "1", "R", "1-12"), ("mal", "3", None, "1-24")
    )


def test_inference_matching_helpers() -> None:
    tv_a = SourceMeta(
        type=SourceType.TV,
        episodes=10,
        duration=24,
        start_year=2020,
        titles=("Example Show",),
    )
    tv_b = SourceMeta(
        type=SourceType.TV,
        episodes=10,
        duration=25,
        start_year=2020,
        titles=("example show", "Example Show Season 1"),
    )
    assert _title_score(tv_a, tv_b) >= 0.9
    assert _normalize_title("千と千尋の神隠し") == "千と千尋の神隠し"

    assert _relative_delta(10, 12) == 2 / 12
    assert _episode_range(SourceMeta(episodes=1)) == "1"
    assert _episode_range(SourceMeta(episodes=5)) == "1-5"
    assert _episode_range(SourceMeta(episodes=0)) is None


def test_inference_requires_titles_and_exact_year_alignment() -> None:
    id_graph = IdMappingGraph()
    a = ("anilist", "1", None)
    b = ("mal", "2", None)
    id_graph.add_equivalence_class([a, b])

    store = MetaStore()
    store.set(
        a[0],
        a[1],
        SourceMeta(type=SourceType.TV, episodes=12, start_year=2020),
        a[2],
    )
    store.set(
        b[0],
        b[1],
        SourceMeta(
            type=SourceType.TV,
            episodes=12,
            start_year=2021,
            titles=("Matching Title",),
        ),
        b[2],
    )

    assert infer_episode_mappings(store, id_graph).node_count() == 0


def test_inference_skips_ambiguous_provider_pair_matches() -> None:
    id_graph = IdMappingGraph()
    a = ("anilist", "1", None)
    b = ("mal", "2", None)
    c = ("mal", "3", None)
    id_graph.add_equivalence_class([a, b, c])

    store = MetaStore()
    shared = SourceMeta(
        type=SourceType.TV,
        episodes=12,
        duration=24,
        start_year=2020,
        titles=("Ambiguous Match",),
    )
    store.set(a[0], a[1], shared, a[2])
    store.set(b[0], b[1], shared, b[2])
    store.set(c[0], c[1], shared, c[2])

    assert infer_episode_mappings(store, id_graph).node_count() == 0


def test_inference_uses_sibling_series_metadata_to_break_special_ties() -> None:
    id_graph = IdMappingGraph()
    special_2002 = ("anidb", "33", "S")
    main_2002 = ("anidb", "33", "R")
    special_2003 = ("anidb", "835", "S")
    main_2003 = ("anidb", "835", "R")
    anilist = ("anilist", "7579", None)
    id_graph.add_equivalence_class(
        [special_2002, main_2002, special_2003, main_2003, anilist]
    )

    store = MetaStore()
    store.set(
        special_2002[0],
        special_2002[1],
        SourceMeta(type=SourceType.TV, episodes=1),
        special_2002[2],
    )
    store.set(
        special_2003[0],
        special_2003[1],
        SourceMeta(type=SourceType.TV, episodes=1),
        special_2003[2],
    )
    store.set(
        main_2002[0],
        main_2002[1],
        SourceMeta(
            type=SourceType.TV,
            episodes=13,
            start_year=2002,
            titles=("Happy Lesson (TV)",),
        ),
        main_2002[2],
    )
    store.set(
        main_2003[0],
        main_2003[1],
        SourceMeta(
            type=SourceType.TV,
            episodes=13,
            start_year=2003,
            titles=("Happy Lesson Advance",),
        ),
        main_2003[2],
    )
    store.set(
        anilist[0],
        anilist[1],
        SourceMeta(
            type=SourceType.TV,
            episodes=1,
            duration=25,
            start_year=2002,
            titles=("Happy Lesson: Hokahoka Kanna to Futari Kiri",),
        ),
        anilist[2],
    )

    episode_graph = infer_episode_mappings(store, id_graph)

    assert episode_graph.has_edge(
        ("anidb", "33", "S", "1"), ("anilist", "7579", None, "1")
    )
    assert not episode_graph.has_edge(
        ("anidb", "835", "S", "1"), ("anilist", "7579", None, "1")
    )
