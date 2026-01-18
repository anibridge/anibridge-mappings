"""Stats building for the aggregation pipeline."""

from typing import Any

from anibridge_mappings.core.aggregator import AggregationArtifacts
from anibridge_mappings.core.graph import _BaseGraph
from anibridge_mappings.core.validators import parse_descriptor


def _count_edges(graph: _BaseGraph) -> int:
    """Return edge counts derived from graph adjacency."""
    nodes = graph.nodes()
    link_count = sum(len(graph.neighbors(node)) for node in nodes)
    return link_count


def build_stats(
    artifacts: AggregationArtifacts, payload: dict[str, Any]
) -> dict[str, Any]:
    """Build a stats payload from aggregation artifacts.

    Args:
        artifacts (AggregationArtifacts): Aggregation results.
        payload (dict[str, Any]): Serialized mappings payload.

    Returns:
        dict[str, Any]: Stats payload.
    """
    id_graph = artifacts.id_graph
    episode_graph = artifacts.episode_graph
    meta_store = artifacts.meta_store
    issues = artifacts.validation_issues

    provider_stats: dict[str, dict[str, int]] = {}
    descriptor_sets: dict[str, set[tuple[str, str | None]]] = {}
    id_sets: dict[str, set[str]] = {}
    scope_sets: dict[str, set[str | None]] = {}
    source_descriptor_sets: dict[str, set[str]] = {}
    target_descriptor_sets: dict[str, set[str]] = {}
    source_range_counts: dict[str, int] = {}
    target_range_counts: dict[str, int] = {}

    source_ranges_total = 0
    target_ranges_total = 0
    descriptor_union: set[str] = set()

    for source_descriptor, targets in payload.items():
        if source_descriptor == "$meta":
            continue
        descriptor_union.add(source_descriptor)
        try:
            src_provider, src_id, src_scope = parse_descriptor(source_descriptor)
        except ValueError:
            continue
        descriptor_sets.setdefault(src_provider, set()).add((src_id, src_scope))
        id_sets.setdefault(src_provider, set()).add(src_id)
        scope_sets.setdefault(src_provider, set()).add(src_scope)
        source_descriptor_sets.setdefault(src_provider, set()).add(source_descriptor)

        for target_descriptor, range_map in targets.items():
            descriptor_union.add(target_descriptor)
            try:
                tgt_provider, tgt_id, tgt_scope = parse_descriptor(target_descriptor)
            except ValueError:
                continue
            descriptor_sets.setdefault(tgt_provider, set()).add((tgt_id, tgt_scope))
            id_sets.setdefault(tgt_provider, set()).add(tgt_id)
            scope_sets.setdefault(tgt_provider, set()).add(tgt_scope)
            target_descriptor_sets.setdefault(tgt_provider, set()).add(
                target_descriptor
            )

            source_range_units = len(range_map)
            source_range_counts[src_provider] = (
                source_range_counts.get(src_provider, 0) + source_range_units
            )
            source_ranges_total += source_range_units

            for target_spec in range_map.values():
                segments = [
                    seg.strip() for seg in str(target_spec).split(",") if seg.strip()
                ]
                target_range_counts[tgt_provider] = target_range_counts.get(
                    tgt_provider, 0
                ) + len(segments)
                target_ranges_total += len(segments)

    for provider in set(
        list(descriptor_sets)
        + list(id_sets)
        + list(scope_sets)
        + list(source_range_counts)
        + list(target_range_counts)
        + list(source_descriptor_sets)
        + list(target_descriptor_sets)
    ):
        stats = provider_stats.setdefault(provider, {})
        stats["distinct_descriptors"] = len(descriptor_sets.get(provider, set()))
        stats["distinct_ids"] = len(id_sets.get(provider, set()))
        stats["distinct_scopes"] = len(scope_sets.get(provider, set()))
        stats["source_range_units"] = source_range_counts.get(provider, 0)
        stats["target_range_units"] = target_range_counts.get(provider, 0)

    validator_counts: dict[str, int] = {}
    source_provider_counts: dict[str, int] = {}
    target_provider_counts: dict[str, int] = {}
    distinct_sources: set[str] = set()
    distinct_targets: set[str] = set()

    for issue in issues:
        validator_counts[issue.validator] = validator_counts.get(issue.validator, 0) + 1
        if issue.source:
            distinct_sources.add(issue.source)
            try:
                src_provider, _src_id, _src_scope = parse_descriptor(issue.source)
            except ValueError:
                src_provider = None
            if src_provider:
                source_provider_counts[src_provider] = (
                    source_provider_counts.get(src_provider, 0) + 1
                )
        if issue.target:
            distinct_targets.add(issue.target)
            try:
                tgt_provider, _tgt_id, _tgt_scope = parse_descriptor(issue.target)
            except ValueError:
                tgt_provider = None
            if tgt_provider:
                target_provider_counts[tgt_provider] = (
                    target_provider_counts.get(tgt_provider, 0) + 1
                )

    stats_payload: dict[str, Any] = {
        "meta": payload.get("$meta", {}),
        "summary": {
            "providers": len(provider_stats),
            "distinct_descriptors": len(descriptor_union),
            "source_range_units": source_ranges_total,
            "target_range_units": target_ranges_total,
            "validation_issues": len(issues),
        },
        "providers": {
            provider: provider_stats[provider] for provider in sorted(provider_stats)
        },
        "validator": {
            "total_issues": len(issues),
            "by_validator": dict(sorted(validator_counts.items())),
            "by_source_provider": dict(sorted(source_provider_counts.items())),
            "by_target_provider": dict(sorted(target_provider_counts.items())),
            "distinct_sources": len(distinct_sources),
            "distinct_targets": len(distinct_targets),
        },
        "internal": {
            "episode_graph_nodes": episode_graph.node_count(),
            "episode_graph_edges": _count_edges(episode_graph),
            "id_graph_nodes": id_graph.node_count(),
            "id_graph_edges": _count_edges(id_graph),
            "meta_entries": len(meta_store),
        },
    }
    return stats_payload
