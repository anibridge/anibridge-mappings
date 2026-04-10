import asyncio
from datetime import UTC, datetime

from anibridge_mappings.core.aggregator import (
    MappingAggregator,
    _episode_source_contributor,
    _validation_prune_reason,
    build_schema_payload,
)
from anibridge_mappings.core.graph import EpisodeMappingGraph, IdMappingGraph
from anibridge_mappings.core.meta import MetaStore
from anibridge_mappings.core.provenance import _normalize_timestamp
from anibridge_mappings.core.validators import (
    MappingValidator,
    ValidationContext,
    ValidationIssue,
)
from anibridge_mappings.utils.mapping import format_descriptor


class StubSource:
    async def prepare(self) -> None:
        return None


class StubIdSource(StubSource):
    def build_id_graph(self) -> IdMappingGraph:
        graph = IdMappingGraph()
        graph.add_edge(("anidb", "1", "R"), ("mal", "2", None))
        return graph


class StubMetaSource(StubSource):
    async def collect_metadata(self, id_graph: IdMappingGraph) -> MetaStore:
        del id_graph
        store = MetaStore()
        store.update("anidb", "1", "R", episodes=2)
        return store


class StubEpisodeSource(StubSource):
    def build_episode_graph(
        self, store: MetaStore, id_graph: IdMappingGraph
    ) -> EpisodeMappingGraph:
        del store, id_graph
        graph = EpisodeMappingGraph()
        graph.add_edge(("anidb", "1", "R", "1-2"), ("mal", "2", None, "1-2"))
        return graph


class StubValidator(MappingValidator):
    name = "stub"

    def validate(self, context: ValidationContext) -> list[ValidationIssue]:
        del context
        return []


def test_mapping_aggregator_run_end_to_end_without_network() -> None:
    aggregator = MappingAggregator(
        metadata_sources=(StubMetaSource(),),
        id_sources=(StubIdSource(),),
        episode_sources=(StubEpisodeSource(),),
        validators=(StubValidator(),),
    )

    artifacts = asyncio.run(aggregator.run(edits_file=None))

    assert artifacts.id_graph.node_count() >= 2
    assert artifacts.episode_graph.node_count() >= 2


def test_schema_payload_and_descriptor_helpers() -> None:
    graph = EpisodeMappingGraph()
    graph.add_edge(("anidb", "1", "R", "1"), ("mal", "2", None, "1"))

    payload = build_schema_payload(
        graph, schema_version="1.0.0", generated_on=datetime(2024, 1, 1, tzinfo=UTC)
    )
    assert payload["$meta"]["schema_version"] == "1.0.0"
    assert "anidb:1:R" in payload

    assert format_descriptor("anidb", "1", "R") == "anidb:1:R"
    assert _normalize_timestamp(datetime(2024, 1, 1, tzinfo=UTC)).endswith("Z")


def test_validation_reason_and_source_contributor_helpers() -> None:
    issue = ValidationIssue(
        validator="mapping_ranges",
        message="Bad mapping",
        source="anidb:1:R",
        target="mal:2",
        source_range="1",
        target_range="1",
        details={"foo": "bar"},
    )
    reason = _validation_prune_reason(issue)
    assert "Bad mapping" in reason and "foo=bar" in reason

    src = StubEpisodeSource()
    assert _episode_source_contributor(src).endswith(":StubEpisodeSource")
