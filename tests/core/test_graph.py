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


def test_episode_graph_provenance_and_forced_edge_state() -> None:
    graph = EpisodeMappingGraph()
    left = ("anidb", "1", "R", "1-3")
    right = ("mal", "2", None, "1-3")

    with graph.provenance_context(
        ProvenanceContext(stage="stage", actor="actor", details={"a": 1})
    ):
        graph.add_edge(left, right, details={"forced": True})

    assert graph.is_forced_edge(left, right)

    graph.remove_edge(left, right)
    assert not graph.has_edge(left, right)
    assert not graph.is_forced_edge(left, right)

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
