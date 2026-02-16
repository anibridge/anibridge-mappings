"""Validation helpers for mapping integrity checks."""

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, cast

from anibridge_mappings.core.graph import EpisodeMappingGraph, IdMappingGraph
from anibridge_mappings.core.meta import MetaStore
from anibridge_mappings.utils.mapping import (
    SourceTargetMap,
    build_source_target_map,
    parse_range_bounds,
    provider_scope_sort_key,
    split_ratio,
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
    """Shared context for validators.

    This caches the computed source-target map so that validators
    can iterate the same derived structures without recomputing them.
    """

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
        """Construct a validation context with a cached source map."""
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
        """Return validation issues for the provided graphs.

        Args:
            context (ValidationContext): Shared validation context.

        Returns:
            list[ValidationIssue]: Validation issues, if any.
        """
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
        """Build a standardized validation issue."""
        return ValidationIssue(
            validator=self.name,
            message=message,
            source=source,
            target=target,
            source_range=source_range,
            target_range=target_range,
            details=details,
        )


@dataclass(slots=True)
class RangeSpec:
    """Parsed range specification data."""

    raw: str
    base: str | None
    ratio: int | None
    bounds: tuple[int, int | None] | None

    @property
    def is_valid(self) -> bool:
        """Return True when both base and bounds parsed successfully."""
        return self.base is not None and self.bounds is not None


@dataclass(slots=True)
class SegmentBounds:
    """Parsed segment bounds with metadata."""

    start: int
    end: int | None
    ratio: int | None
    raw: str


def _descriptor(provider: str, entry_id: str, scope: str | None) -> str:
    """Build a provider descriptor string from components."""
    if scope is None:
        return f"{provider}:{entry_id}"
    return f"{provider}:{entry_id}:{scope}"


def _iter_target_ranges(
    source_ranges: dict[str, set[str]],
) -> Iterable[tuple[str, str]]:
    """Yield (source_range, target_range_spec) pairs from a source-range map."""
    for source_range in sorted(source_ranges):
        target_ranges = source_ranges[source_range]
        for target_range in sorted(target_ranges):
            yield source_range, target_range


def _range_length_and_ratio(range_key: str) -> tuple[int, int | None] | None:
    """Return (length, ratio) for a simple numeric range key, if possible."""
    if not range_key or "," in range_key:
        return None
    spec = _parse_range_spec(range_key)
    if not spec.is_valid or spec.bounds is None or spec.bounds[1] is None:
        return None
    start, end_opt = spec.bounds
    end = cast(int, end_opt)
    return (end - start + 1, spec.ratio)


def _iter_target_segment_strings(target_range: str) -> Iterable[str]:
    """Yield individual target range segments (comma-separated)."""
    for segment in target_range.split(","):
        segment = segment.strip()
        if segment:
            yield segment


def _parse_range_spec(range_key: str) -> RangeSpec:
    """Parse a range key into its base, ratio, and bounds."""
    split = split_ratio(range_key)
    if split is None:
        return RangeSpec(raw=range_key, base=None, ratio=None, bounds=None)
    base, ratio = split
    bounds = parse_range_bounds(base)
    return RangeSpec(raw=range_key, base=base, ratio=ratio, bounds=bounds)


def _ranges_overlap(
    start_a: int,
    end_a: int | None,
    start_b: int,
    end_b: int | None,
) -> bool:
    """Return True if two inclusive ranges overlap (supports open-ended)."""
    return not (
        (end_a is not None and end_a < start_b)
        or (end_b is not None and end_b < start_a)
    )


def _iter_segment_bounds(target_range: str) -> Iterable[SegmentBounds]:
    """Yield parsed bounds for each target range segment."""
    for segment in _iter_target_segment_strings(target_range):
        spec = _parse_range_spec(segment)
        if not spec.is_valid or spec.bounds is None:
            continue
        start, end = spec.bounds
        yield SegmentBounds(
            start=start,
            end=end,
            ratio=spec.ratio,
            raw=segment,
        )


def _segment_source_units(segment: SegmentBounds) -> int | None:
    """Return source units represented by a target segment, if determinable."""
    if segment.end is None:
        return None
    length = segment.end - segment.start + 1
    if segment.ratio is None:
        return length
    if segment.ratio > 0:
        if length % segment.ratio != 0:
            return None
        return length // segment.ratio
    return length * abs(segment.ratio)


class MappingRangeValidator(MappingValidator):
    """Validate mapping range syntax and consistency."""

    name = "mapping_ranges"

    def validate(self, context: ValidationContext) -> list[ValidationIssue]:
        """Return issues found across source/target range specifications.

        Args:
            context (ValidationContext): Shared validation context.

        Returns:
            list[ValidationIssue]: Range validation issues found.
        """
        issues: list[ValidationIssue] = []

        for (src_provider, src_id, src_scope), targets in context.source_map.items():
            source_descriptor = _descriptor(src_provider, src_id, src_scope)
            provider_source_ranges: dict[
                str, list[tuple[int, int | None, str, str, str]]
            ] = {}
            for (t_provider, t_id, t_scope), source_ranges in targets.items():
                target_descriptor = _descriptor(t_provider, t_id, t_scope)
                meta = context.meta_store.peek(t_provider, t_id, t_scope)
                limit = meta.episodes if meta else None
                target_segments: list[tuple[int, int | None, str, str]] = []

                for source_range, target_range in _iter_target_ranges(source_ranges):
                    source_spec = _parse_range_spec(source_range)
                    if "," in source_range:
                        issues.append(
                            self.issue(
                                "Source ranges must be contiguous (no commas)",
                                source=source_descriptor,
                                target=target_descriptor,
                                source_range=source_range,
                                target_range=target_range,
                                details={"source_range": source_range},
                            )
                        )
                    if not source_spec.is_valid:
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
                    elif source_spec.bounds is not None:
                        source_start, source_end = source_spec.bounds
                        provider_source_ranges.setdefault(t_provider, []).append(
                            (
                                source_start,
                                source_end,
                                target_descriptor,
                                source_range,
                                target_range,
                            )
                        )

                    src_info = _range_length_and_ratio(source_range)
                    src_len: int | None = None
                    src_ratio: int | None = None
                    if src_info is not None:
                        src_len, src_ratio = src_info

                    segments = list(_iter_segment_bounds(target_range))
                    for segment in segments:
                        target_segments.append(
                            (
                                segment.start,
                                segment.end,
                                source_range,
                                target_range,
                            )
                        )

                        if limit and limit > 0:
                            if segment.end is None:
                                if segment.start > limit:
                                    issues.append(
                                        self.issue(
                                            "Target mapping exceeds available episodes",
                                            source=source_descriptor,
                                            target=target_descriptor,
                                            source_range=source_range,
                                            target_range=segment.raw,
                                            details={
                                                "source_range": source_range,
                                                "target_range": segment.raw,
                                                "episode_limit": limit,
                                            },
                                        )
                                    )
                            elif segment.end > limit:
                                issues.append(
                                    self.issue(
                                        "Target mapping exceeds available episodes",
                                        source=source_descriptor,
                                        target=target_descriptor,
                                        source_range=source_range,
                                        target_range=segment.raw,
                                        details={
                                            "source_range": source_range,
                                            "target_range": segment.raw,
                                            "episode_limit": limit,
                                        },
                                    )
                                )

                    for segment in _iter_target_segment_strings(target_range):
                        target_spec = _parse_range_spec(segment)
                        if target_spec.is_valid:
                            continue
                        issues.append(
                            self.issue(
                                "Invalid target range syntax",
                                source=source_descriptor,
                                target=target_descriptor,
                                source_range=source_range,
                                target_range=segment,
                                details={"target_range": segment},
                            )
                        )

                    if len(segments) > 1:
                        segments.sort(
                            key=lambda item: (
                                item.start,
                                float("inf") if item.end is None else item.end,
                            )
                        )
                        prev = segments[0]
                        for current in segments[1:]:
                            if _ranges_overlap(
                                prev.start,
                                prev.end,
                                current.start,
                                current.end,
                            ):
                                issues.append(
                                    self.issue(
                                        "Overlapping target segments within a mapping",
                                        source=source_descriptor,
                                        target=target_descriptor,
                                        source_range=source_range,
                                        target_range=target_range,
                                        details={
                                            "overlaps_with": prev.raw,
                                            "segment": current.raw,
                                        },
                                    )
                                )
                            prev_end_value = (
                                float("inf") if prev.end is None else prev.end
                            )
                            current_end_value = (
                                float("inf") if current.end is None else current.end
                            )
                            if current_end_value > prev_end_value:
                                prev = current

                    if src_len is None or src_ratio is not None:
                        continue
                    if not segments:
                        continue

                    segment_units: list[int] = []
                    units_unknown = False
                    for segment in segments:
                        units = _segment_source_units(segment)
                        if units is None:
                            units_unknown = True
                            break
                        segment_units.append(units)

                    if units_unknown or not segment_units:
                        continue

                    total_units = sum(segment_units)
                    if total_units != src_len:
                        issues.append(
                            self.issue(
                                "Target segments expand beyond source range units",
                                source=source_descriptor,
                                target=target_descriptor,
                                source_range=source_range,
                                target_range=target_range,
                                details={
                                    "source_units": src_len,
                                    "target_units": total_units,
                                },
                            )
                        )

                if len(target_segments) > 1:
                    target_segments.sort(
                        key=lambda item: (
                            item[0],
                            float("inf") if item[1] is None else item[1],
                        )
                    )
                    prev: tuple[int, int | None, str, str] | None = None
                    for start, end, src_range, tgt_range in target_segments:
                        if prev is not None:
                            prev_start, prev_end, prev_src, prev_base = prev
                            if src_range != prev_src and _ranges_overlap(
                                start, end, prev_start, prev_end
                            ):
                                issues.append(
                                    self.issue(
                                        "Overlapping target episode ranges for the "
                                        "same target scope",
                                        source=source_descriptor,
                                        target=target_descriptor,
                                        source_range=src_range,
                                        target_range=tgt_range,
                                        details={
                                            "source_range": src_range,
                                            "target_range": tgt_range,
                                            "overlaps_with_source_range": prev_src,
                                            "overlaps_with_target_range": prev_base,
                                        },
                                    )
                                )
                        prev_end_value = (
                            float("inf")
                            if prev is None
                            else (float("inf") if prev[1] is None else prev[1])
                        )
                        current_end_value = float("inf") if end is None else end
                        if prev is None or current_end_value >= prev_end_value:
                            prev = (start, end, src_range, tgt_range)

            for target_provider, items in provider_source_ranges.items():
                if len(items) <= 1:
                    continue
                items.sort(
                    key=lambda item: (
                        item[0],
                        10**9 if item[1] is None else item[1],
                        provider_scope_sort_key(item[2]),
                        item[3],
                        item[4],
                    )
                )
                accepted: list[tuple[int, int | None, str, str, str]] = []
                for start, end, target_descriptor, source_range, target_range in items:
                    overlap_with = next(
                        (
                            accepted_item
                            for accepted_item in accepted
                            if _ranges_overlap(
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
                                source_range,
                                target_range,
                            )
                        )
                        continue

                    _, _, prev_target, prev_source_range, prev_target_range = (
                        overlap_with
                    )
                    issues.append(
                        self.issue(
                            "Overlapping source episode ranges for the same target "
                            "provider",
                            source=source_descriptor,
                            target=target_descriptor,
                            source_range=source_range,
                            target_range=target_range,
                            details={
                                "target_provider": target_provider,
                                "overlaps_with_target": prev_target,
                                "overlaps_with_source_range": prev_source_range,
                                "overlaps_with_target_range": prev_target_range,
                            },
                        )
                    )

        return issues
