"""Validation helpers for mapping integrity checks."""

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, cast

from anibridge_mappings.core.graph import EpisodeMappingGraph, IdMappingGraph
from anibridge_mappings.core.meta import MetaStore
from anibridge_mappings.utils.mapping import build_source_target_map, parse_range_bounds


@dataclass(slots=True)
class ValidationIssue:
    """Represents a validation finding."""

    validator: str
    message: str
    source: str | None = None
    target: str | None = None
    source_range: str | None = None
    target_range: str | None = None
    details: dict[str, Any] | None = None


class MappingValidator:
    """Base class for mapping validators."""

    name: str = "validator"

    def validate(
        self,
        episode_graph: EpisodeMappingGraph,
        meta_store: MetaStore,
        id_graph: IdMappingGraph,
    ) -> list[ValidationIssue]:
        """Return validation issues for the provided graphs.

        Args:
            episode_graph (EpisodeMappingGraph): Episode mapping graph.
            meta_store (MetaStore): Metadata store.
            id_graph (IdMappingGraph): ID mapping graph.

        Returns:
            list[ValidationIssue]: Validation issues, if any.
        """
        raise NotImplementedError


def _descriptor(provider: str, entry_id: str, scope: str | None) -> str:
    """Build a provider descriptor string from components."""
    if scope is None:
        return f"{provider}:{entry_id}"
    return f"{provider}:{entry_id}:{scope}"


def parse_descriptor(descriptor: str) -> tuple[str, str, str | None]:
    """Parse `provider:id[:scope]` strings back into tuple form.

    Args:
        descriptor (str): Provider descriptor string.

    Returns:
        tuple[str, str, str | None]: Provider, entry ID, and optional scope.
    """
    parts = descriptor.split(":", 2)
    if len(parts) == 2:
        provider, entry_id = parts
        return provider, entry_id, None
    if len(parts) == 3:
        provider, entry_id, scope = parts
        return provider, entry_id, scope
    raise ValueError(f"Invalid descriptor: {descriptor}")


def _iter_target_ranges(
    source_ranges: dict[str, set[str]],
) -> Iterable[tuple[str, str]]:
    """Yield (source_range, target_range_spec) pairs from a source-range map."""
    for source_range, target_ranges in source_ranges.items():
        for target_range in target_ranges:
            yield source_range, target_range


class MappingOverlapValidator(MappingValidator):
    """Detect overlapping target ranges for a given target scope."""

    name = "mapping_overlap"

    def validate(
        self,
        episode_graph: EpisodeMappingGraph,
        meta_store: MetaStore,
        id_graph: IdMappingGraph,
    ) -> list[ValidationIssue]:
        """Return overlaps where multiple source ranges hit overlapping targets.

        Args:
            episode_graph (EpisodeMappingGraph): Episode mapping graph.
            meta_store (MetaStore): Metadata store (unused).
            id_graph (IdMappingGraph): ID mapping graph (unused).

        Returns:
            list[ValidationIssue]: Overlap issues found.
        """
        del meta_store, id_graph
        issues: list[ValidationIssue] = []

        source_map = build_source_target_map(episode_graph)
        for (src_provider, src_id, src_scope), targets in source_map.items():
            for (t_provider, t_id, t_scope), source_ranges in targets.items():
                segments: list[tuple[int, int, str]] = []
                for src_range, target_range in _iter_target_ranges(source_ranges):
                    base = target_range.split("|", 1)[0]
                    bounds = parse_range_bounds(base)
                    if bounds is None or bounds[1] is None:
                        continue
                    start, end_opt = bounds
                    end = cast(int, end_opt)
                    for prev_start, prev_end, prev_src in segments:
                        if not (end < prev_start or start > prev_end):
                            issues.append(
                                ValidationIssue(
                                    validator=self.name,
                                    message=(
                                        "Overlapping target episode ranges for the "
                                        "same target scope"
                                    ),
                                    source=_descriptor(src_provider, src_id, src_scope),
                                    target=_descriptor(t_provider, t_id, t_scope),
                                    source_range=src_range,
                                    target_range=base,
                                    details={
                                        "source_range": src_range,
                                        "target_range": base,
                                        "overlaps_with_source_range": prev_src,
                                        "overlaps_with_target_range": (
                                            f"{prev_start}-{prev_end}"
                                        ),
                                    },
                                )
                            )
                    segments.append((start, end, src_range))

        return issues


class MappingOverflowValidator(MappingValidator):
    """Detect target mappings that exceed known episode counts."""

    name = "mapping_overflow"

    def validate(
        self,
        episode_graph: EpisodeMappingGraph,
        meta_store: MetaStore,
        id_graph: IdMappingGraph,
    ) -> list[ValidationIssue]:
        """Return mappings whose target ranges exceed known episode counts.

        Args:
            episode_graph (EpisodeMappingGraph): Episode mapping graph.
            meta_store (MetaStore): Metadata store.
            id_graph (IdMappingGraph): ID mapping graph (unused).

        Returns:
            list[ValidationIssue]: Overflow issues found.
        """
        del id_graph
        issues: list[ValidationIssue] = []

        source_map = build_source_target_map(episode_graph)
        for (_src_provider, _src_id, _src_scope), targets in source_map.items():
            for (t_provider, t_id, t_scope), source_ranges in targets.items():
                meta = meta_store.peek(t_provider, t_id, t_scope)
                limit = meta.episodes if meta else None
                if not limit or limit <= 0:
                    continue

                for src_range, target_range in _iter_target_ranges(source_ranges):
                    base = target_range.split("|", 1)[0]
                    bounds = parse_range_bounds(base)
                    if bounds is None:
                        continue
                    start, end_opt = bounds
                    # Open upper bound: flag if the start already exceeds the limit.
                    if end_opt is None:
                        if start > limit:
                            issues.append(
                                ValidationIssue(
                                    validator=self.name,
                                    message="Target mapping exceeds available episodes",
                                    source=_descriptor(
                                        _src_provider, _src_id, _src_scope
                                    ),
                                    target=_descriptor(t_provider, t_id, t_scope),
                                    source_range=src_range,
                                    target_range=base,
                                    details={
                                        "source_range": src_range,
                                        "target_range": base,
                                        "episode_limit": limit,
                                    },
                                )
                            )
                        continue

                    end = end_opt
                    if end > limit:
                        issues.append(
                            ValidationIssue(
                                validator=self.name,
                                message="Target mapping exceeds available episodes",
                                source=_descriptor(_src_provider, _src_id, _src_scope),
                                target=_descriptor(t_provider, t_id, t_scope),
                                source_range=src_range,
                                target_range=base,
                                details={
                                    "source_range": src_range,
                                    "target_range": base,
                                    "episode_limit": limit,
                                },
                            )
                        )

        return issues
