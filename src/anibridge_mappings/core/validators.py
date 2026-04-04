"""Validation helpers for mapping integrity checks."""

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from anibridge.utils.mappings import format_mapping_range

from anibridge_mappings.core.graph import EpisodeMappingGraph, IdMappingGraph
from anibridge_mappings.core.meta import MetaStore, SourceMeta
from anibridge_mappings.core.range_specs import (
    TargetSpec,
    has_internal_overlap,
    parse_source_segment,
    parse_target_spec,
    ranges_overlap,
    target_units,
)
from anibridge_mappings.utils.mapping import (
    SourceTargetMap,
    build_source_target_map,
    provider_scope_sort_key,
)


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


@dataclass(slots=True)
class ValidationContext:
    """Shared context for validators."""

    episode_graph: EpisodeMappingGraph
    meta_store: MetaStore
    id_graph: IdMappingGraph
    source_map: SourceTargetMap

    @classmethod
    def from_graphs(
        cls,
        episode_graph: EpisodeMappingGraph,
        meta_store: MetaStore,
        id_graph: IdMappingGraph,
    ) -> ValidationContext:
        """Build a validation context from graph and metadata inputs."""
        return cls(
            episode_graph=episode_graph,
            meta_store=meta_store,
            id_graph=id_graph,
            source_map=build_source_target_map(episode_graph),
        )


class MappingValidator:
    """Base class for mapping validators."""

    name: str = "validator"

    def validate(self, context: ValidationContext) -> list[ValidationIssue]:
        """Validate mappings and return all issues found."""
        raise NotImplementedError

    def issue(
        self,
        message: str,
        *,
        source: str | None = None,
        target: str | None = None,
        source_range: str | None = None,
        target_range: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> ValidationIssue:
        """Create a typed issue entry owned by this validator."""
        return ValidationIssue(
            validator=self.name,
            message=message,
            source=source,
            target=target,
            source_range=source_range,
            target_range=target_range,
            details=details,
        )


class MappingRangeValidator(MappingValidator):
    """Validate mapping range syntax and consistency."""

    name = "mapping_ranges"

    def validate(self, context: ValidationContext) -> list[ValidationIssue]:
        """Run all range validations against the mapping context."""
        issues: list[ValidationIssue] = []

        for source_scope, targets in context.source_map.items():
            source_descriptor = _descriptor(*source_scope)
            source_meta = context.meta_store.peek(*source_scope)
            provider_windows: dict[
                str,
                list[tuple[int, int | None, str, str, str, str]],
            ] = {}

            for target_scope, source_ranges in targets.items():
                target_descriptor = _descriptor(*target_scope)
                target_provider, target_id, _target_scope_value = target_scope

                if source_scope[0] == target_provider:
                    issues.extend(
                        self.issue(
                            "Same-provider cross-link",
                            source=source_descriptor,
                            target=target_descriptor,
                            source_range=src,
                            target_range=tgt,
                            details={"provider": source_scope[0]},
                        )
                        for src, tgt in _iter_target_ranges(source_ranges)
                    )
                    continue

                meta = context.meta_store.peek(*target_scope)
                episode_limit = meta.episodes if meta else None
                target_scope_windows: list[tuple[int, int | None, str, str]] = []
                source_targets: dict[str, list[tuple[str, TargetSpec]]] = {}

                for source_range, target_range in _iter_target_ranges(source_ranges):
                    source_segment = parse_source_segment(source_range)
                    if source_segment is None:
                        issues.append(
                            self.issue(
                                "Invalid source range syntax",
                                source=source_descriptor,
                                target=target_descriptor,
                                source_range=source_range,
                                target_range=target_range,
                                details={"source_range": source_range},
                            )
                        )
                        continue

                    spec = parse_target_spec(target_range)
                    if spec is None:
                        issues.append(
                            self.issue(
                                "Invalid target range syntax",
                                source=source_descriptor,
                                target=target_descriptor,
                                source_range=source_range,
                                target_range=target_range,
                                details={"target_range": target_range},
                            )
                        )
                        continue

                    provider_windows.setdefault(target_provider, []).append(
                        (
                            source_segment.start,
                            source_segment.end,
                            target_descriptor,
                            target_id,
                            source_range,
                            target_range,
                        )
                    )

                    _validate_same_source_target_overlap(
                        self,
                        issues,
                        source_descriptor,
                        target_descriptor,
                        source_range,
                        target_range,
                        spec,
                        source_targets,
                    )
                    _validate_target_spec_shape(
                        self,
                        issues,
                        source_descriptor,
                        target_descriptor,
                        source_range,
                        target_range,
                        spec,
                    )
                    _validate_edge_compatibility(
                        self,
                        issues,
                        source_descriptor,
                        target_descriptor,
                        source_segment,
                        source_range,
                        target_range,
                        spec,
                        source_meta,
                        meta,
                        episode_limit,
                    )

                    for segment in spec.segments:
                        target_scope_windows.append(
                            (segment.start, segment.end, source_range, target_range)
                        )

                _validate_target_scope_overlap(
                    self,
                    issues,
                    source_descriptor,
                    target_descriptor,
                    target_scope_windows,
                )

            _validate_provider_cross_id_overlap(
                self,
                issues,
                source_descriptor,
                provider_windows,
            )

        return issues


def _iter_target_ranges(
    source_ranges: dict[str, set[str]],
) -> Iterable[tuple[str, str]]:
    """Yield normalized source-target range pairs in stable order."""
    for source_range in sorted(source_ranges):
        for target_range in sorted(source_ranges[source_range]):
            yield source_range, target_range


def _descriptor(provider: str, entry_id: str, scope: str | None) -> str:
    """Format a provider descriptor from tuple parts."""
    if scope is None:
        return f"{provider}:{entry_id}"
    return f"{provider}:{entry_id}:{scope}"


def _spec_overlaps(left: TargetSpec, right: TargetSpec) -> bool:
    """Return True when two target specs share any overlapping segment."""
    for left_segment in left.segments:
        for right_segment in right.segments:
            if ranges_overlap(
                left_segment.start,
                left_segment.end,
                right_segment.start,
                right_segment.end,
            ):
                return True
    return False


def _validate_same_source_target_overlap(
    validator: MappingRangeValidator,
    issues: list[ValidationIssue],
    source_descriptor: str,
    target_descriptor: str,
    source_range: str,
    target_range: str,
    spec: TargetSpec,
    source_targets: dict[str, list[tuple[str, TargetSpec]]],
) -> None:
    """Reject multiple targets with overlapping windows for one source range."""
    existing = source_targets.setdefault(source_range, [])
    overlap_with = next(
        (
            prior_target_range
            for prior_target_range, prior_spec in existing
            if _spec_overlaps(spec, prior_spec)
        ),
        None,
    )
    if overlap_with is not None:
        issues.append(
            validator.issue(
                "Overlapping target ranges for the same source range",
                source=source_descriptor,
                target=target_descriptor,
                source_range=source_range,
                target_range=target_range,
                details={
                    "source_range": source_range,
                    "target_range": target_range,
                    "overlaps_with_target_range": overlap_with,
                },
            )
        )
        return
    existing.append((target_range, spec))


def _validate_target_spec_shape(
    validator: MappingRangeValidator,
    issues: list[ValidationIssue],
    source_descriptor: str,
    target_descriptor: str,
    source_range: str,
    target_range: str,
    spec: TargetSpec,
) -> None:
    """Reject self-overlapping segments inside a single target spec."""
    if not has_internal_overlap(spec):
        return

    sorted_segments = sorted(
        spec.segments,
        key=lambda segment: (
            segment.start,
            float("inf") if segment.end is None else segment.end,
        ),
    )
    previous = sorted_segments[0]
    for current in sorted_segments[1:]:
        if ranges_overlap(previous.start, previous.end, current.start, current.end):
            issues.append(
                validator.issue(
                    "Overlapping target segments within a mapping",
                    source=source_descriptor,
                    target=target_descriptor,
                    source_range=source_range,
                    target_range=target_range,
                    details={
                        "overlaps_with": format_mapping_range(previous),
                        "segment": format_mapping_range(current),
                    },
                )
            )
            return

        previous_end = float("inf") if previous.end is None else previous.end
        current_end = float("inf") if current.end is None else current.end
        if current_end > previous_end:
            previous = current


def _validate_edge_compatibility(
    validator: MappingRangeValidator,
    issues: list[ValidationIssue],
    source_descriptor: str,
    target_descriptor: str,
    source_segment: Any,
    source_range: str,
    target_range: str,
    spec: TargetSpec,
    source_meta: SourceMeta | None,
    target_meta: SourceMeta | None,
    episode_limit: int | None,
) -> None:
    """Validate type, target bounds, and unit-count compatibility for one edge."""
    if episode_limit and episode_limit > 0:
        for segment in spec.segments:
            segment_end = segment.start if segment.end is None else segment.end
            if segment_end <= episode_limit:
                continue
            formatted = format_mapping_range(segment)
            issues.append(
                validator.issue(
                    "Target mapping exceeds available episodes",
                    source=source_descriptor,
                    target=target_descriptor,
                    source_range=source_range,
                    target_range=formatted,
                    details={
                        "source_range": source_range,
                        "target_range": formatted,
                        "episode_limit": episode_limit,
                    },
                )
            )

    source_units = source_segment.length
    if source_units is None:
        return
    units = target_units(spec)
    if units is None or source_units == units:
        return

    issues.append(
        validator.issue(
            "Target segments expand beyond source range units",
            source=source_descriptor,
            target=target_descriptor,
            source_range=source_range,
            target_range=target_range,
            details={"source_units": source_units, "target_units": units},
        )
    )


def _validate_target_scope_overlap(
    validator: MappingRangeValidator,
    issues: list[ValidationIssue],
    source_descriptor: str,
    target_descriptor: str,
    target_scope_windows: list[tuple[int, int | None, str, str]],
) -> None:
    """Reject overlaps across different source ranges for one target scope."""
    if len(target_scope_windows) <= 1:
        return

    target_scope_windows.sort(
        key=lambda item: (
            item[0],
            float("inf") if item[1] is None else item[1],
        )
    )
    previous: tuple[int, int | None, str, str] | None = None

    for start, end, source_range, target_range in target_scope_windows:
        if previous is not None:
            prev_start, prev_end, prev_source_range, prev_target_range = previous
            if source_range != prev_source_range and ranges_overlap(
                start,
                end,
                prev_start,
                prev_end,
            ):
                issues.append(
                    validator.issue(
                        "Overlapping target episode ranges for the same target scope",
                        source=source_descriptor,
                        target=target_descriptor,
                        source_range=source_range,
                        target_range=target_range,
                        details={
                            "source_range": source_range,
                            "target_range": target_range,
                            "overlaps_with_source_range": prev_source_range,
                            "overlaps_with_target_range": prev_target_range,
                        },
                    )
                )

        if previous is None:
            previous = (start, end, source_range, target_range)
            continue

        previous_end = float("inf") if previous[1] is None else previous[1]
        current_end = float("inf") if end is None else end
        if current_end >= previous_end:
            previous = (start, end, source_range, target_range)


def _validate_provider_cross_id_overlap(
    validator: MappingRangeValidator,
    issues: list[ValidationIssue],
    source_descriptor: str,
    provider_windows: dict[str, list[tuple[int, int | None, str, str, str, str]]],
) -> None:
    """Reject source overlaps across different IDs inside the same provider."""
    for target_provider, items in provider_windows.items():
        if len(items) <= 1:
            continue

        items.sort(
            key=lambda item: (
                item[0],
                10**9 if item[1] is None else item[1],
                provider_scope_sort_key(item[2]),
                item[4],
                item[5],
            )
        )

        accepted: list[tuple[int, int | None, str, str, str, str]] = []
        for (
            start,
            end,
            target_descriptor,
            target_id,
            source_range,
            target_range,
        ) in items:
            overlap_with = next(
                (
                    accepted_item
                    for accepted_item in accepted
                    if target_id != accepted_item[3]
                    and ranges_overlap(
                        start,
                        end,
                        accepted_item[0],
                        accepted_item[1],
                    )
                ),
                None,
            )
            if overlap_with is None:
                accepted.append(
                    (
                        start,
                        end,
                        target_descriptor,
                        target_id,
                        source_range,
                        target_range,
                    )
                )
                continue

            (
                _,
                _,
                previous_target,
                previous_target_id,
                previous_source_range,
                previous_target_range,
            ) = overlap_with
            issues.append(
                validator.issue(
                    "Overlapping source episode ranges for the same target provider "
                    "across IDs",
                    source=source_descriptor,
                    target=target_descriptor,
                    source_range=source_range,
                    target_range=target_range,
                    details={
                        "target_provider": target_provider,
                        "target_id": target_id,
                        "overlaps_with_target": previous_target,
                        "overlaps_with_target_id": previous_target_id,
                        "overlaps_with_source_range": previous_source_range,
                        "overlaps_with_target_range": previous_target_range,
                    },
                )
            )
