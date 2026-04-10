from anibridge_mappings.core.graph import (
    EpisodeMappingGraph,
    IdMappingGraph,
    ProvenanceContext,
)


def test_base_graph_node_and_edge_operations() -> None:
    graph = IdMappingGraph()
    a = ("anidb", "1", "R")
    b = ("mal", "2", None)
    c = ("anilist", "3", None)

    graph.add_edge(a, b)
    graph.add_equivalence_class([b, c])

    assert graph.has_edge(a, b)
    assert graph.has_edge(b, c)
    assert graph.get_component(a) == {a, b, c}
    assert graph.node_count() == 3

    grouped = graph.get_component_by_provider(a)
    assert grouped["anidb"] == {("1", "R")}

    graph.remove_edge(a, b)
    assert not graph.has_edge(a, b)

    graph.remove_node(c)
    assert not graph.has_node(c)


def test_episode_graph_provenance_and_edit_edge_state() -> None:
    graph = EpisodeMappingGraph()
    left = ("anidb", "1", "R", "1-3")
    right = ("mal", "2", None, "1-3")

    with graph.provenance_context(
        ProvenanceContext(stage="stage", actor="actor", details={"a": 1})
    ):
        graph.add_edge(left, right, details={"edit": True})

    assert graph.is_edit_edge(left, right)

    graph.remove_edge(left, right)
    assert not graph.has_edge(left, right)
    assert not graph.is_edit_edge(left, right)

    items = graph.provenance_items()
    assert len(items) == 1
    assert [event.action for event in items[0][2]] == ["add", "remove"]


def test_episode_graph_transitive_edges_and_component_grouping() -> None:
    graph = EpisodeMappingGraph()

    n1 = ("anidb", "1", "R", "1")
    n2 = ("mal", "2", None, "1")
    n3 = ("anilist", "3", None, "1")

    graph.add_edge(n1, n2)
    graph.add_edge(n2, n3)

    added = graph.add_transitive_edges(
        provenance=ProvenanceContext(stage="transitive", actor="engine")
    )
    assert added == 1
    assert graph.has_edge(n1, n3)

    grouped = graph.get_component_by_provider(n1)
    assert set(grouped) == {"anidb", "mal", "anilist"}


def test_episode_graph_transitive_edges_skip_same_provider_links() -> None:
    graph = EpisodeMappingGraph()

    n1 = ("anidb", "90", "S", "4")
    n2 = ("tvdb_show", "70863", "s3", "33")
    n3 = ("anidb", "73", "S", "1")

    graph.add_edge(n1, n2)
    graph.add_edge(n2, n3)

    added = graph.add_transitive_edges(
        provenance=ProvenanceContext(stage="transitive", actor="engine")
    )

    assert added == 0
    assert not graph.has_edge(n1, n3)


def test_episode_graph_add_graph_merges_edges() -> None:
    left = ("anidb", "1", "R", "1")
    right = ("mal", "2", None, "1")

    g1 = EpisodeMappingGraph()
    g2 = EpisodeMappingGraph()
    g2.add_edge(left, right)

    g1.add_graph(g2, provenance=ProvenanceContext(stage="merge"))

    assert g1.has_edge(left, right)
    assert len(g1.provenance_items()) == 1


def test_episode_graph_transitive_edges_allow_multi_entry_scopes() -> None:
    """Multi-entry scopes (e.g. a TVDB season containing many anime) should not
    have their transitive edges blocked by the single-entry conflict check."""
    graph = EpisodeMappingGraph()

    # Shinkro-like: MAL entries -> different episodes in the same TVDB season
    mal_a = ("mal", "100", None, "1")
    tvdb_ep1 = ("tvdb_show", "72775", "s0", "1")
    mal_b = ("mal", "200", None, "1")
    tvdb_ep2 = ("tvdb_show", "72775", "s0", "2")

    graph.add_edge(mal_a, tvdb_ep1)
    graph.add_edge(mal_b, tvdb_ep2)

    # AniList = MAL edges (from inference/ID graph)
    al_a = ("anilist", "100", None, "1")
    al_b = ("anilist", "200", None, "1")
    graph.add_edge(al_a, mal_a)
    graph.add_edge(al_b, mal_b)

    # Inference adds one anilist -> tvdb edge directly
    graph.add_edge(al_a, tvdb_ep1)

    ctx = ProvenanceContext(stage="transitive", actor="engine")
    added = graph.add_transitive_edges(provenance=ctx)

    # al_b -> tvdb_ep2 should be added transitively despite tvdb_show:72775:s0
    # already mapping to anilist:100. The scope is multi-entry.
    assert graph.has_edge(al_b, tvdb_ep2), (
        "Multi-entry scope tvdb_show:72775:s0 should allow transitive edges "
        "to different anilist entries at different episodes"
    )
    assert added >= 1
