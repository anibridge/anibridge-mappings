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
    ) -> "ValidationContext":
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
class TargetSegment:
    """Parsed target range segment with its originating source range."""

    source_range: str
    target_range: str
    spec: RangeSpec


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


def _iter_target_segments_from_ranges(
    source_ranges: dict[str, set[str]],
) -> Iterable[TargetSegment]:
    """Yield parsed target segments for the provided source ranges."""
    for source_range, target_range in _iter_target_ranges(source_ranges):
        for segment in _iter_target_segment_strings(target_range):
            yield TargetSegment(
                source_range=source_range,
                target_range=segment,
                spec=_parse_range_spec(segment),
            )


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


class MappingRangeSyntaxValidator(MappingValidator):
    """Detect invalid range syntax in source or target ranges."""

    name = "mapping_range_syntax"

    def validate(self, context: ValidationContext) -> list[ValidationIssue]:
        """Return syntax errors found in range specifications.

        Args:
            context (ValidationContext): Shared validation context.

        Returns:
            list[ValidationIssue]: Range syntax issues found.
        """
        issues: list[ValidationIssue] = []

        for (src_provider, src_id, src_scope), targets in context.source_map.items():
            src_descriptor = _descriptor(src_provider, src_id, src_scope)
            for (t_provider, t_id, t_scope), source_ranges in targets.items():
                tgt_descriptor = _descriptor(t_provider, t_id, t_scope)
                for source_range, target_range in _iter_target_ranges(source_ranges):
                    if "," in source_range:
                        issues.append(
                            self.issue(
                                "Source ranges must be contiguous (no commas)",
                                source=src_descriptor,
                                target=tgt_descriptor,
                                source_range=source_range,
                                target_range=target_range,
                                details={"source_range": source_range},
                            )
                        )
                        continue

                    source_spec = _parse_range_spec(source_range)
                    if not source_spec.is_valid:
                        issues.append(
                            self.issue(
                                "Invalid source range syntax",
                                source=src_descriptor,
                                target=tgt_descriptor,
                                source_range=source_range,
                                target_range=target_range,
                                details={"source_range": source_range},
                            )
                        )

                    for segment in _iter_target_segment_strings(target_range):
                        target_spec = _parse_range_spec(segment)
                        if target_spec.is_valid:
                            continue
                        issues.append(
                            self.issue(
                                "Invalid target range syntax",
                                source=src_descriptor,
                                target=tgt_descriptor,
                                source_range=source_range,
                                target_range=segment,
                                details={"target_range": segment},
                            )
                        )

        return issues


class MappingOverlapValidator(MappingValidator):
    """Detect overlapping target ranges for a given target scope."""

    name = "mapping_overlap"

    def validate(self, context: ValidationContext) -> list[ValidationIssue]:
        """Return overlaps where multiple source ranges hit overlapping targets.

        Args:
            context (ValidationContext): Shared validation context.

        Returns:
            list[ValidationIssue]: Overlap issues found.
        """
        issues: list[ValidationIssue] = []

        for (src_provider, src_id, src_scope), targets in context.source_map.items():
            source_descriptor = _descriptor(src_provider, src_id, src_scope)
            for (t_provider, t_id, t_scope), source_ranges in targets.items():
                target_descriptor = _descriptor(t_provider, t_id, t_scope)
                segments: list[tuple[int, int | None, str, str]] = []
                for segment in _iter_target_segments_from_ranges(source_ranges):
                    if not segment.spec.is_valid or segment.spec.bounds is None:
                        continue
                    start, end = segment.spec.bounds
                    segments.append(
                        (
                            start,
                            end,
                            segment.source_range,
                            segment.spec.base or segment.target_range,
                        )
                    )

                segments.sort(
                    key=lambda item: (
                        item[0],
                        float("inf") if item[1] is None else item[1],
                    )
                )

                prev: tuple[int, int | None, str, str] | None = None
                for start, end, src_range, tgt_base in segments:
                    if prev is not None:
                        prev_start, prev_end, prev_src, prev_base = prev
                        if _ranges_overlap(start, end, prev_start, prev_end):
                            issues.append(
                                self.issue(
                                    "Overlapping target episode ranges for the same "
                                    "target scope",
                                    source=source_descriptor,
                                    target=target_descriptor,
                                    source_range=src_range,
                                    target_range=tgt_base,
                                    details={
                                        "source_range": src_range,
                                        "target_range": tgt_base,
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
                        prev = (start, end, src_range, tgt_base)

        return issues


class MappingOverflowValidator(MappingValidator):
    """Detect target mappings that exceed known episode counts."""

    name = "mapping_overflow"

    def validate(self, context: ValidationContext) -> list[ValidationIssue]:
        """Return mappings whose target ranges exceed known episode counts.

        Args:
            context (ValidationContext): Shared validation context.

        Returns:
            list[ValidationIssue]: Overflow issues found.
        """
        issues: list[ValidationIssue] = []

        for (_src_provider, _src_id, _src_scope), targets in context.source_map.items():
            for (t_provider, t_id, t_scope), source_ranges in targets.items():
                meta = context.meta_store.peek(t_provider, t_id, t_scope)
                limit = meta.episodes if meta else None
                if not limit or limit <= 0:
                    continue

                for src_range, target_range in _iter_target_ranges(source_ranges):
                    for segment in _iter_target_segment_strings(target_range):
                        spec = _parse_range_spec(segment)
                        if not spec.is_valid or spec.bounds is None:
                            continue
                        start, end_opt = spec.bounds
                        base = spec.base or segment
                        # Open upper bound: flag if the start already exceeds the limit.
                        if end_opt is None:
                            if start > limit:
                                issues.append(
                                    self.issue(
                                        "Target mapping exceeds available episodes",
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
                                self.issue(
                                    "Target mapping exceeds available episodes",
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

        return issues


class MappingUnitMismatchValidator(MappingValidator):
    """Detect mappings where source/target unit counts are incompatible."""

    name = "mapping_unit_mismatch"

    def validate(self, context: ValidationContext) -> list[ValidationIssue]:
        """Return mappings whose source/target unit counts do not align.

        Args:
            context (ValidationContext): Shared validation context.

        Returns:
            list[ValidationIssue]: Unit mismatch issues found.
        """
        issues: list[ValidationIssue] = []

        for (src_provider, src_id, src_scope), targets in context.source_map.items():
            for (t_provider, t_id, t_scope), source_ranges in targets.items():
                for src_range, target_range in _iter_target_ranges(source_ranges):
                    src_info = _range_length_and_ratio(src_range)
                    tgt_info = _range_length_and_ratio(target_range)
                    if src_info is None or tgt_info is None:
                        continue

                    src_len, src_ratio = src_info
                    tgt_len, tgt_ratio = tgt_info

                    # Skip ranges that already carry a source-side ratio.
                    if src_ratio is not None:
                        continue

                    if tgt_ratio is None:
                        expected = src_len
                        if tgt_len != expected:
                            issues.append(
                                self.issue(
                                    "Source and target range units do not match",
                                    source=_descriptor(src_provider, src_id, src_scope),
                                    target=_descriptor(t_provider, t_id, t_scope),
                                    source_range=src_range,
                                    target_range=target_range,
                                    details={
                                        "source_units": src_len,
                                        "target_units": tgt_len,
                                    },
                                )
                            )
                        continue

                    if tgt_ratio > 0:
                        expected = src_len * tgt_ratio
                        if tgt_len != expected:
                            issues.append(
                                self.issue(
                                    "Target range units do not match ratio",
                                    source=_descriptor(src_provider, src_id, src_scope),
                                    target=_descriptor(t_provider, t_id, t_scope),
                                    source_range=src_range,
                                    target_range=target_range,
                                    details={
                                        "source_units": src_len,
                                        "target_units": tgt_len,
                                        "ratio": tgt_ratio,
                                    },
                                )
                            )
                        continue

                    expected = tgt_len * abs(tgt_ratio)
                    if src_len != expected:
                        issues.append(
                            self.issue(
                                "Source range units do not match ratio",
                                source=_descriptor(src_provider, src_id, src_scope),
                                target=_descriptor(t_provider, t_id, t_scope),
                                source_range=src_range,
                                target_range=target_range,
                                details={
                                    "source_units": src_len,
                                    "target_units": tgt_len,
                                    "ratio": tgt_ratio,
                                },
                            )
                        )

        return issues
