from anibridge_mappings.core.graph import EpisodeMappingGraph, IdMappingGraph
from anibridge_mappings.core.meta import MetaStore, SourceMeta, SourceType
from anibridge_mappings.core.validators import (
    MappingRangeValidator,
    ValidationContext,
    _iter_target_ranges,
    _spec_overlaps,
)
from anibridge_mappings.utils.mapping import format_descriptor


def test_validator_flags_same_provider_cross_link_and_limits() -> None:
    episode_graph = EpisodeMappingGraph()
    # same provider cross-link
    episode_graph.add_edge(("anidb", "1", "R", "1"), ("anidb", "2", "R", "1"))
    # over target limit
    episode_graph.add_edge(("anidb", "1", "R", "1-2"), ("mal", "3", None, "1-3"))

    store = MetaStore()
    store.set("mal", "3", SourceMeta(type=SourceType.TV, episodes=2), None)

    context = ValidationContext.from_graphs(episode_graph, store, IdMappingGraph())
    issues = MappingRangeValidator().validate(context)

    messages = {issue.message for issue in issues}
    assert "Same-provider cross-link" in messages
    assert "Target mapping exceeds available episodes" in messages


def test_validator_internal_helpers() -> None:
    source_ranges = {"1": {"2", "3"}}
    assert list(_iter_target_ranges(source_ranges)) == [("1", "2"), ("1", "3")]
    assert format_descriptor("anidb", "1", "R") == "anidb:1:R"

    left = MappingRangeValidator().validate
    assert callable(left)

    spec1 = MappingRangeValidator
    assert spec1 is not None

    # overlap helper via parsed target specs
    from anibridge_mappings.core.range_specs import parse_target_spec

    a = parse_target_spec("1-2")
    b = parse_target_spec("2-3")
    assert a is not None and b is not None
    assert _spec_overlaps(a, b)


def test_validator_reports_invalid_ranges_and_overlap_shapes() -> None:
    episode_graph = EpisodeMappingGraph()

    # Invalid source and target syntax.
    episode_graph.add_edge(("anidb", "1", "R", "bad"), ("mal", "10", None, "1"))
    episode_graph.add_edge(("anidb", "1", "R", "1"), ("mal", "10", None, "bad"))

    # Same source range with overlapping target windows.
    episode_graph.add_edge(("anidb", "1", "R", "2-3"), ("mal", "10", None, "5-6"))
    episode_graph.add_edge(("anidb", "1", "R", "2-3"), ("mal", "10", None, "6-7"))

    # Target scope overlap across different source ranges.
    episode_graph.add_edge(("anidb", "1", "R", "10-11"), ("mal", "20", None, "1-2"))
    episode_graph.add_edge(("anidb", "1", "R", "12-13"), ("mal", "20", None, "2-3"))

    context = ValidationContext.from_graphs(
        episode_graph, MetaStore(), IdMappingGraph()
    )
    issues = MappingRangeValidator().validate(context)

    messages = {issue.message for issue in issues}
    assert "Invalid source range syntax" in messages
    assert "Invalid target range syntax" in messages
    assert "Overlapping target ranges for the same source range" in messages
    assert "Overlapping target episode ranges for the same target scope" in messages


def test_validator_reports_units_and_cross_id_provider_overlap() -> None:
    episode_graph = EpisodeMappingGraph()

    # Unit mismatch: source has 2 units, target has 3.
    episode_graph.add_edge(("anidb", "1", "R", "1-2"), ("mal", "30", None, "1-3"))

    # Same target provider across different IDs with overlapping source windows.
    episode_graph.add_edge(("anidb", "1", "R", "20-21"), ("mal", "31", None, "1-2"))
    episode_graph.add_edge(("anidb", "1", "R", "21-22"), ("mal", "32", None, "1-2"))

    store = MetaStore()
    store.set("mal", "30", SourceMeta(type=SourceType.TV, episodes=100), None)

    context = ValidationContext.from_graphs(episode_graph, store, IdMappingGraph())
    issues = MappingRangeValidator().validate(context)

    messages = {issue.message for issue in issues}
    assert "Target segments expand beyond source range units" in messages
    assert (
        "Overlapping source episode ranges for the same target provider across IDs"
        in messages
    )
