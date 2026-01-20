"""Orchestration for AniBridge mapping generation."""

import asyncio
import importlib.metadata
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from logging import getLogger
from typing import Any

from anibridge_mappings.core.edits import apply_edits, load_edits
from anibridge_mappings.core.graph import (
    EpisodeMappingGraph,
    IdMappingGraph,
    ProvenanceContext,
)
from anibridge_mappings.core.inference import infer_episode_mappings
from anibridge_mappings.core.meta import MetaStore
from anibridge_mappings.core.validators import (
    MappingOverflowValidator,
    MappingOverlapValidator,
    MappingValidator,
    ValidationIssue,
    parse_descriptor,
)
from anibridge_mappings.sources.anilist import AnilistSource
from anibridge_mappings.sources.anime_aggregations import AnimeAggregationsSource
from anibridge_mappings.sources.anime_lists import AnimeListsSource
from anibridge_mappings.sources.anime_offline_database import (
    AnimeOfflineDatabaseSource,
)
from anibridge_mappings.sources.base import (
    BaseSource,
    EpisodeMappingSource,
    IdMappingSource,
    MetadataSource,
)
from anibridge_mappings.sources.shinkro import (
    ShinkroTmdbMappingSource,
    ShinkroTvdbMappingSource,
)
from anibridge_mappings.sources.skyhook import SkyhookSource
from anibridge_mappings.sources.tmdb import TmdbSource
from anibridge_mappings.sources.wikidata import WikidataSource
from anibridge_mappings.utils.mapping import (
    _provider_scope_sort_key,
    build_source_target_map,
    collapse_source_mappings,
    ordered_payload,
)

log = getLogger(__name__)


@dataclass(slots=True)
class AggregationArtifacts:
    """Container returned after the aggregation pipeline completes."""

    id_graph: IdMappingGraph
    meta_store: MetaStore
    episode_graph: EpisodeMappingGraph
    validation_issues: list[ValidationIssue]


class MappingAggregator:
    """Coordinate source preparation, collection, validation, and inference."""

    def __init__(
        self,
        *,
        metadata_sources: Sequence[MetadataSource],
        id_sources: Sequence[IdMappingSource],
        episode_sources: Sequence[EpisodeMappingSource],
        validators: Sequence[MappingValidator] | None = None,
    ) -> None:
        """Initialize the aggregator with the configured source collections.

        Args:
            metadata_sources (Sequence[MetadataSource]): Providers for metadata.
            id_sources (Sequence[IdMappingSource]): Providers for ID mappings.
            episode_sources (Sequence[EpisodeMappingSource]): Providers for episodes.
            validators (Sequence[MappingValidator] | None): Optional validators.
        """
        self._metadata_sources = tuple(metadata_sources)
        self._id_sources = tuple(id_sources)
        self._episode_sources = tuple(episode_sources)
        self._validators = tuple(validators or ())
        self._all_sources = self._dedupe_sources()

    async def run(self, *, edits_file: str | None = None) -> AggregationArtifacts:
        """Execute the full pipeline and return the collected artifacts.

        Args:
            edits_file (str | None): Optional path to edits file.

        Returns:
            AggregationArtifacts: The collected graphs and metadata.
        """
        await self._prepare_sources()

        log.info(f"Building ID graph from {len(self._id_sources)} sources")
        id_graph = self._build_id_graph()
        log.info(f"ID graph contains {id_graph.node_count()} nodes")

        log.info(f"Collecting metadata from {len(self._metadata_sources)} sources")
        meta_store = await self._collect_metadata(id_graph)
        log.info(f"Metadata store contains {len(meta_store.items())} entries")

        log.info(f"Building episode graph from {len(self._episode_sources)} sources")
        episode_graph = self._build_episode_graph(meta_store, id_graph)
        log.info(f"Episode graph contains {len(episode_graph.nodes())} nodes")

        inferred_graph = infer_episode_mappings(meta_store, id_graph)
        if inferred_graph.node_count():
            episode_graph.add_graph(
                inferred_graph,
                provenance=ProvenanceContext(
                    stage="Inference: metadata-driven episode alignment",
                    actor="Inference engine: metadata alignment",
                    reason="Inferred episode links via cross-source metadata alignment",
                ),
            )
            log.info(
                "Episode graph contains %d nodes after inference",
                len(episode_graph.nodes()),
            )

        if edits_file:
            edits = load_edits(edits_file)
            if edits:
                edited_scopes = apply_edits(episode_graph, edits)
                log.info("Applied edits for %d source scopes", len(edited_scopes))

        validation_issues = await self._run_validators(
            episode_graph, meta_store, id_graph
        )
        if validation_issues:
            self._prune_invalid_edges(episode_graph, validation_issues)
            log.warning("Validation produced %d issue(s)", len(validation_issues))
        else:
            log.info("Validation produced no issues")

        transitive_edges = episode_graph.add_transitive_edges(
            provenance=ProvenanceContext(
                stage="Graph enrichment: transitive closure",
                actor="Graph expander: transitive closure",
                reason="Added indirect links to improve mapping connectivity",
            )
        )
        if transitive_edges:
            log.info("Added %d transitive episode mapping edges", transitive_edges)

        return AggregationArtifacts(
            id_graph=id_graph,
            meta_store=meta_store,
            episode_graph=episode_graph,
            validation_issues=validation_issues,
        )

    async def _prepare_sources(self) -> None:
        """Prepare all sources concurrently."""
        if not self._all_sources:
            return
        async with asyncio.TaskGroup() as tg:
            for source in self._all_sources:
                tg.create_task(source.prepare())

    def _build_id_graph(self) -> IdMappingGraph:
        """Construct the combined ID mapping graph from all providers."""
        combined = IdMappingGraph()
        for provider in self._id_sources:
            graph = provider.build_id_graph()
            combined.add_graph(graph)
        return combined

    async def _collect_metadata(self, id_graph: IdMappingGraph) -> MetaStore:
        """Construct the combined metadata store from all providers."""
        store = MetaStore()
        if not self._metadata_sources:
            return store

        async with asyncio.TaskGroup() as tg:
            tasks = [
                tg.create_task(provider.collect_metadata(id_graph))
                for provider in self._metadata_sources
            ]

        results = [task.result() for task in tasks]
        for result in results:
            store.merge(result)
        return store

    def _build_episode_graph(
        self,
        meta_store: MetaStore,
        id_graph: IdMappingGraph,
    ) -> EpisodeMappingGraph:
        """Construct the combined episode mapping graph from all providers."""
        combined = EpisodeMappingGraph()
        for provider in self._episode_sources:
            graph = provider.build_episode_graph(meta_store, id_graph)
            context = ProvenanceContext(
                stage="Source ingestion: episode mappings",
                actor=f"Provider source: {provider.__class__.__name__}",
                reason="Direct episode mappings supplied by the source provider",
            )
            for source_node, target_node in graph.iter_edges():
                combined.add_edge(
                    source_node,
                    target_node,
                    bidirectional=True,
                    provenance=context,
                )
        return combined

    async def _run_validators(
        self,
        episode_graph: EpisodeMappingGraph,
        meta_store: MetaStore,
        id_graph: IdMappingGraph,
    ) -> list[ValidationIssue]:
        """Run the configured validators on the aggregation artifacts."""
        if not self._validators:
            return []
        issues: list[ValidationIssue] = []
        for validator in self._validators:
            try:
                result = validator.validate(episode_graph, meta_store, id_graph)
            except Exception as exc:  # pragma: no cover - defensive logging
                log.exception("Validator %s failed: %s", validator.name, exc)
                continue
            if result:
                issues.extend(result)
        return issues

    def _prune_invalid_edges(
        self,
        episode_graph: EpisodeMappingGraph,
        issues: list[ValidationIssue],
    ) -> None:
        """Remove edges associated with validation issues."""
        for issue in issues:
            if not issue.source or not issue.target:
                continue
            if not issue.source_range or not issue.target_range:
                continue

            try:
                src_provider, src_id, src_scope = parse_descriptor(issue.source)
                tgt_provider, tgt_id, tgt_scope = parse_descriptor(issue.target)
            except ValueError:
                continue

            source_node = (src_provider, src_id, src_scope, issue.source_range)
            target_node = (tgt_provider, tgt_id, tgt_scope, issue.target_range)

            if episode_graph.has_node(source_node) and episode_graph.has_node(
                target_node
            ):
                log.debug(
                    "Pruning invalid edge: %s -- %s because: %s",
                    source_node,
                    target_node,
                    issue.message,
                )
                episode_graph.remove_edge(
                    source_node,
                    target_node,
                    provenance=ProvenanceContext(
                        stage="Validation: rule-based pruning",
                        actor=f"Validator: {issue.validator}",
                        reason="Removed mapping that failed validation rules",
                        details={
                            "message": issue.message,
                            "validator": issue.validator,
                            "source": issue.source,
                            "target": issue.target,
                            "source_range": issue.source_range,
                            "target_range": issue.target_range,
                            "details": issue.details,
                        },
                    ),
                )

    def _dedupe_sources(self) -> tuple[BaseSource, ...]:
        """Deduplicate sources across all categories while preserving order."""
        seen: dict[int, BaseSource] = {}
        for source in (
            *self._metadata_sources,
            *self._id_sources,
            *self._episode_sources,
        ):
            seen[id(source)] = source
        return tuple(seen.values())


def mapping_descriptor(provider: str, entry_id: str, scope: str | None) -> str:
    """Return the schema descriptor `provider:entry_id[:scope]` string.

    Args:
        provider (str): The provider name.
        entry_id (str): The entry ID.
        scope (str | None): Optional scope; omitted when `None`.

    Returns:
        str: The combined mapping descriptor.
    """
    base = f"{provider}:{entry_id}"
    return base if scope is None else f"{base}:{scope}"


def build_schema_payload(
    episode_graph: EpisodeMappingGraph,
    *,
    schema_version: str | None = None,
    generated_on: datetime | None = None,
) -> dict[str, Any]:
    """Serialize the episode graph into the public mapping schema structure.

    Args:
        episode_graph (EpisodeMappingGraph): The episode mapping graph.
        schema_version (str | None): The schema version string.
        generated_on (datetime | None): The generation timestamp.

    Returns:
        dict[str, Any]: The serialized mapping payload.
    """
    if schema_version is None:
        schema_version = importlib.metadata.version("anibridge-mappings")
    timestamp = _normalize_timestamp(generated_on)
    payload: dict[str, Any] = {
        "$meta": {
            "schema_version": schema_version,
            "generated_on": timestamp,
        }
    }

    source_map = build_source_target_map(episode_graph)
    for source_scope, targets in sorted(
        source_map.items(),
        key=lambda item: _provider_scope_sort_key(mapping_descriptor(*item[0])),
    ):
        source_provider, source_id, source_scope_value = source_scope
        source_descriptor = mapping_descriptor(
            source_provider, source_id, source_scope_value
        )
        collapsed_targets: dict[str, dict[str, str]] = {}
        for target_scope, source_ranges in sorted(
            targets.items(),
            key=lambda item: _provider_scope_sort_key(mapping_descriptor(*item[0])),
        ):
            target_provider, target_id, target_scope_value = target_scope
            collapsed = collapse_source_mappings(source_ranges)
            if not collapsed:
                continue
            collapsed_targets[
                mapping_descriptor(target_provider, target_id, target_scope_value)
            ] = collapsed
        if collapsed_targets:
            payload[source_descriptor] = collapsed_targets

    payload = ordered_payload(payload)

    return payload


def _normalize_timestamp(value: datetime | None) -> str:
    """Normalize a datetime into a UTC ISO-8601 string."""
    moment = value or datetime.now(UTC)
    iso = moment.replace(microsecond=0).isoformat()
    return iso.replace("+00:00", "Z") if iso.endswith("+00:00") else iso


def default_aggregator() -> MappingAggregator:
    """Construct a `MappingAggregator` with the built-in source set.

    Returns:
        MappingAggregator: Configured aggregator instance.
    """
    anilist = AnilistSource()
    anime_aggregations = AnimeAggregationsSource()
    anime_lists = AnimeListsSource()
    anime_offline_db = AnimeOfflineDatabaseSource()
    shinkro_tmdb = ShinkroTmdbMappingSource()
    shinkro_tvdb = ShinkroTvdbMappingSource()
    skyhook = SkyhookSource()
    tmdb = TmdbSource()
    wikidata = WikidataSource()

    return MappingAggregator(
        # Order matters for metadata; later sources have higher precedence
        metadata_sources=(
            anime_offline_db,
            anilist,
            anime_aggregations,
            skyhook,
            tmdb,
        ),
        id_sources=(
            anime_aggregations,
            anime_lists,
            anime_offline_db,
            shinkro_tmdb,
            shinkro_tvdb,
            wikidata,
        ),
        episode_sources=(
            anime_lists,
            shinkro_tmdb,
            shinkro_tvdb,
        ),
        validators=(MappingOverlapValidator(), MappingOverflowValidator()),
    )
