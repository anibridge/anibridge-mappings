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
        # Gather all meta for nodes in the component
        meta_nodes: list[tuple[SourceMeta, IdNode]] = []
        for provider, entry_id, scope in component:
            meta = meta_store.peek(provider, entry_id, scope)
            if meta is None:
                continue
            if meta.episodes is None or meta.episodes <= 0:
                continue
            meta_nodes.append((meta, (provider, entry_id, scope)))

        # Try all pairs for matching
        for (meta1, node1), (meta2, node2) in combinations(meta_nodes, 2):
            if _meta_match(meta1, meta2):
                episode_range = _range_from_meta_key(_meta_key(meta1))
                if episode_range is None:
                    continue
                left_node = (*node1, episode_range)
                right_node = (*node2, episode_range)
                inferred.add_edge(left_node, right_node)

    if inferred.node_count():
        log.info(
            "Inferred %d episode mapping node(s) from metadata", inferred.node_count()
        )

    return inferred


def _meta_key(meta: SourceMeta) -> MetaKey:
    """Build a hashable metadata key from a SourceMeta instance."""
    return (meta.type, meta.episodes, meta.duration, meta.start_year)


def _meta_match(meta1: SourceMeta, meta2: SourceMeta) -> bool:
    """Check if two SourceMeta objects match under our inference rules."""
    # Type and episodes must match exactly
    if meta1.type != meta2.type:
        return False
    if meta1.episodes != meta2.episodes:
        return False

    y1, y2 = meta1.start_year, meta2.start_year
    if meta1.type == SourceType.MOVIE and (not y1 or not y2 or y1 != y2):
        return False
    if meta1.type == SourceType.TV and (y1 and y2) and (y1 != y2):
        return False

    d1, d2 = meta1.duration, meta2.duration
    relative_d = _relative_delta(d1, d2)
    if meta1.type == SourceType.MOVIE and (not d1 or not d2 or relative_d > 0.1):
        return False
    if meta1.type == SourceType.TV and (d1 and d2) and (relative_d > 0.1):  # noqa: SIM103
        return False

    return True


def _relative_delta(a: int | None, b: int | None) -> float:
    """Calculate relative delta between two integer values."""
    if a is None or b is None:
        return -1.0
    if a == 0 and b == 0:
        return 0.0
    denominator = max(abs(a), abs(b))
    if denominator == 0:
        return 0.0
    return abs(a - b) / denominator


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
