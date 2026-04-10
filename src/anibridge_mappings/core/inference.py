"""Inference helpers for metadata-backed episode mappings."""

import re
from collections.abc import Iterable
from dataclasses import replace
from difflib import SequenceMatcher
from itertools import combinations
from logging import getLogger

from anibridge_mappings.core.graph import EpisodeMappingGraph, IdMappingGraph, IdNode
from anibridge_mappings.core.meta import MetaStore, SourceMeta

log = getLogger(__name__)

_TITLE_TOKEN_RE = re.compile(r"\w+", re.UNICODE)
_MIN_INFERENCE_TITLE_SCORE = 0.45
_MIN_INFERENCE_SCORE = 0.65


def infer_episode_mappings(
    meta_store: MetaStore,
    id_graph: IdMappingGraph,
) -> EpisodeMappingGraph:
    """Infer episode mappings when metadata align."""
    inferred = EpisodeMappingGraph()

    for component in _iter_components(id_graph):
        candidates = _component_meta_candidates(meta_store, component)
        if len(candidates) < 2:
            continue

        for node_left, node_right, episode_range in _select_inference_pairs(candidates):
            inferred.add_edge((*node_left, episode_range), (*node_right, episode_range))

    if inferred.node_count():
        log.info(
            "Inferred %d episode mapping node(s) from metadata",
            inferred.node_count(),
        )

    return inferred


def _component_meta_candidates(
    meta_store: MetaStore,
    component: set[IdNode],
) -> list[tuple[SourceMeta, IdNode]]:
    """Find all nodes in the component with valid metadata for episode inference."""
    raw_candidates: list[tuple[SourceMeta, IdNode]] = []
    related_by_entry: dict[tuple[str, str], list[SourceMeta]] = {}

    for node in component:
        meta = meta_store.peek(*node)
        if meta is None or meta.episodes is None or meta.episodes <= 0:
            continue
        raw_candidates.append((meta, node))
        related_by_entry.setdefault(node[:2], []).append(meta)

    return [
        (_merge_context(meta, related_by_entry[node[:2]]), node)
        for meta, node in raw_candidates
    ]


def _select_inference_pairs(
    candidates: list[tuple[SourceMeta, IdNode]],
) -> list[tuple[IdNode, IdNode, str]]:
    """Return cross-provider candidates for inference via greedy matching.

    Nodes whose best score is tied across multiple partners are skipped to
    avoid ambiguous matches.
    """
    by_provider: dict[str, list[tuple[SourceMeta, IdNode]]] = {}
    for meta, node in candidates:
        by_provider.setdefault(node[0], []).append((meta, node))

    result: list[tuple[IdNode, IdNode, str]] = []
    for left_provider, right_provider in combinations(sorted(by_provider), 2):
        pair_scores: list[tuple[float, IdNode, IdNode, str]] = []
        for meta_left, node_left in by_provider[left_provider]:
            for meta_right, node_right in by_provider[right_provider]:
                score = _match_score(meta_left, meta_right)
                if score is None:
                    continue
                episode_range = _episode_range(meta_left)
                if episode_range is None:
                    continue
                pair_scores.append((score, node_left, node_right, episode_range))

        # Identify ambiguous nodes: those whose top score is tied across
        # different partners. These are excluded from greedy matching.
        best: dict[IdNode, tuple[float, IdNode | None]] = {}
        for score, node_left, node_right, _ep in pair_scores:
            for key_node, other in ((node_left, node_right), (node_right, node_left)):
                cur = best.get(key_node)
                if cur is None or score > cur[0]:
                    best[key_node] = (score, other)
                elif score == cur[0] and cur[1] != other:
                    best[key_node] = (score, None)
        ambiguous = {n for n, (_, partner) in best.items() if partner is None}

        # Greedy: sort by score descending, pick best unmatched pairs
        pair_scores.sort(
            key=lambda item: (
                -item[0],
                item[1][1],
                item[1][2] or "",
                item[2][1],
                item[2][2] or "",
            )
        )
        matched_left: set[IdNode] = set()
        matched_right: set[IdNode] = set()
        for _score, node_left, node_right, episode_range in pair_scores:
            if node_left in matched_left or node_right in matched_right:
                continue
            if node_left in ambiguous or node_right in ambiguous:
                continue
            result.append((node_left, node_right, episode_range))
            matched_left.add(node_left)
            matched_right.add(node_right)

    return result


def _merge_context(base: SourceMeta, related: list[SourceMeta]) -> SourceMeta:
    """Fill missing title fields from same-entry related scopes."""
    if base.titles:
        return base

    for meta in related:
        if meta.titles:
            return replace(base, titles=meta.titles)

    return base


def _match_score(left: SourceMeta, right: SourceMeta) -> float | None:
    if left.episodes != right.episodes:
        return None

    type_score = _type_score(left, right)
    if type_score is None:
        return None

    title_score = _title_score(left, right)
    if title_score < _MIN_INFERENCE_TITLE_SCORE:
        return None

    year_score = _year_score(left, right)
    if year_score is None:
        return None

    duration_score = _duration_score(left, right)

    score = title_score + year_score + duration_score + type_score
    return score if score >= _MIN_INFERENCE_SCORE else None


def _type_score(left: SourceMeta, right: SourceMeta) -> float | None:
    """Return a compatibility bonus/penalty for type alignment."""
    if left.type is None or right.type is None:
        return 0.0
    if left.type == right.type:
        return 0.1
    # tv <-> movie mismatch: penalize but don't reject outright
    return -0.2


def _year_score(left: SourceMeta, right: SourceMeta) -> float | None:
    """Return a compatibility bonus for year alignment."""
    left_year, right_year = left.start_year, right.start_year
    if left_year is None or right_year is None:
        return 0.0

    delta = abs(left_year - right_year)
    if delta == 0:
        return 0.25
    if delta == 1:
        return 0.1
    return None


def _duration_score(left: SourceMeta, right: SourceMeta) -> float:
    """Return a compatibility bonus/penalty for runtime alignment."""
    left_duration, right_duration = left.duration, right.duration
    if left_duration and right_duration:
        abs_delta = abs(left_duration - right_duration)
        rel_delta = _relative_delta(left_duration, right_duration)
        if rel_delta <= 0.1 or abs_delta <= 2:
            return 0.1
        if rel_delta <= 0.25 or abs_delta <= 10:
            return 0.0
        return -0.3
    return 0.0


def _title_score(left: SourceMeta, right: SourceMeta) -> float:
    left_titles = [t for title in left.titles if (t := _normalize_title(title))]
    right_titles = [t for title in right.titles if (t := _normalize_title(title))]
    if not left_titles or not right_titles:
        return 0.0

    best = 0.0
    for left_title in left_titles:
        left_tokens = set(left_title.split())
        for right_title in right_titles:
            if left_title == right_title:
                return 1.0
            right_tokens = set(right_title.split())
            if not left_tokens or not right_tokens:
                continue
            token_score = len(left_tokens & right_tokens) / len(
                left_tokens | right_tokens
            )
            sequence_score = SequenceMatcher(None, left_title, right_title).ratio()
            best = max(best, token_score, sequence_score)
    return best


def _normalize_title(title: str) -> str:
    """Normalize a title into a tokenized lowercase string."""
    return " ".join(
        token.replace("_", "") for token in _TITLE_TOKEN_RE.findall(title.casefold())
    ).strip()


def _relative_delta(a: int, b: int) -> float:
    """Calculate relative delta between two values."""
    denominator = max(abs(a), abs(b))
    if denominator == 0:
        return 0.0
    return abs(a - b) / denominator


def _episode_range(meta: SourceMeta) -> str | None:
    """Format episode range from metadata."""
    episodes = meta.episodes
    if episodes is None or episodes <= 0:
        return None
    return "1" if episodes == 1 else f"1-{episodes}"


def _iter_components(id_graph: IdMappingGraph) -> Iterable[set[IdNode]]:
    """Yield connected components of the ID mapping graph."""
    visited: set[IdNode] = set()
    for node in id_graph.nodes():
        if node in visited:
            continue
        component = id_graph.get_component(node)
        visited.update(component)
        yield component
