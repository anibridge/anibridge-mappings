"""Inference helpers for metadata-backed episode mappings."""

from collections.abc import Iterable
from itertools import combinations
from logging import getLogger

from anibridge_mappings.core.graph import EpisodeMappingGraph, IdMappingGraph, IdNode
from anibridge_mappings.core.meta import MetaStore, SourceMeta, SourceType

log = getLogger(__name__)

MetaKey = tuple[SourceType | None, int | None, int | None, int | None]


def infer_episode_mappings(
    meta_store: MetaStore,
    id_graph: IdMappingGraph,
) -> EpisodeMappingGraph:
    """Infer episode mappings when metadata matches exactly within ID links.

    Args:
        meta_store (MetaStore): Metadata store providing per-entry metadata.
        id_graph (IdMappingGraph): ID mapping graph used to discover linked IDs.

    Returns:
        EpisodeMappingGraph: Inferred episode mapping edges.
    """
    inferred = EpisodeMappingGraph()
    components = list(_iter_components(id_graph))
    if not components:
        return inferred

    for component in components:
        grouped: dict[MetaKey, list[IdNode]] = {}
        for provider, entry_id, scope in component:
            meta = meta_store.peek(provider, entry_id, scope)
            if meta is None:
                continue
            if meta.episodes is None or meta.episodes <= 0:
                continue
            key = _meta_key(meta)
            grouped.setdefault(key, []).append((provider, entry_id, scope))

        for meta_key, nodes in grouped.items():
            if len(nodes) < 2:
                continue
            episode_range = _range_from_meta_key(meta_key)
            if episode_range is None:
                continue
            for left, right in combinations(nodes, 2):
                left_provider, left_id, left_scope = left
                right_provider, right_id, right_scope = right
                left_node = (left_provider, left_id, left_scope, episode_range)
                right_node = (right_provider, right_id, right_scope, episode_range)
                inferred.add_edge(left_node, right_node)

    if inferred.node_count():
        log.info(
            "Inferred %d episode mapping node(s) from metadata", inferred.node_count()
        )

    return inferred


def _meta_key(meta: SourceMeta) -> MetaKey:
    """Build a hashable metadata key from a SourceMeta instance."""
    return (meta.type, meta.episodes, meta.duration, meta.start_year)


def _range_from_meta_key(meta_key: MetaKey) -> str | None:
    """Convert a metadata key to a normalized episode range string."""
    _meta_type, episodes, _duration, _start_year = meta_key
    if episodes is None or episodes <= 0:
        return None
    return "1" if episodes == 1 else f"1-{episodes}"


def _iter_components(id_graph: IdMappingGraph) -> Iterable[set[IdNode]]:
    """Yield unique connected components from an ID graph."""
    visited: set[IdNode] = set()
    for node in id_graph.nodes():
        if node in visited:
            continue
        component = id_graph.get_component(node)
        visited.update(component)
        yield component
