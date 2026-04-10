"""Range parsing helpers."""

import math
from dataclasses import dataclass

from anibridge.utils.mappings import (
    AnibridgeMappingRange,
    format_mapping_range,
    is_valid_target_range,
)


@dataclass(frozen=True, slots=True)
class TargetSpec:
    """Parsed target range spec with optional ratio."""

    segments: tuple[AnibridgeMappingRange, ...]
    ratio: int | None


def parse_source_segment(value: str) -> AnibridgeMappingRange | None:
    """Parse one source range segment (no commas or ratios)."""
    normalized = value.strip()
    if not normalized or "," in normalized or "|" in normalized:
        return None
    try:
        return AnibridgeMappingRange.parse(normalized)
    except ValueError:
        return None


def parse_target_spec(value: str) -> TargetSpec | None:
    """Parse one target spec with optional trailing ratio."""
    normalized = value.strip()
    if not normalized or not is_valid_target_range(normalized):
        return None

    ratio: int | None = None
    ranges_part = normalized
    if "|" in normalized:
        ranges_part, ratio_raw = normalized.rsplit("|", 1)
        ratio = int(ratio_raw)
        if ratio == 0:
            return None

    segments: list[AnibridgeMappingRange] = []
    for part in ranges_part.split(","):
        segment_text = part.strip()
        if not segment_text:
            continue
        try:
            segments.append(AnibridgeMappingRange.parse(segment_text))
        except ValueError:
            return None
    if not segments:
        return None
    return TargetSpec(segments=tuple(segments), ratio=ratio)


def format_target_spec(spec: TargetSpec) -> str:
    """Serialize a parsed target spec."""
    base = ",".join(format_mapping_range(segment) for segment in spec.segments)
    return base if spec.ratio is None else f"{base}|{spec.ratio}"


def range_bounds(value: str) -> tuple[int, int | None] | None:
    """Parse bounds from a single range expression with optional ratio."""
    source_segment = parse_source_segment(value)
    if source_segment is not None:
        return source_segment.start, source_segment.end

    target_spec = parse_target_spec(value)
    if target_spec is None or len(target_spec.segments) != 1:
        return None
    segment = target_spec.segments[0]
    return segment.start, segment.end


def ranges_overlap(
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


def target_units(spec: TargetSpec) -> int | None:
    """Return target units represented by a target spec when determinable."""
    total = 0
    for segment in spec.segments:
        length = segment.length
        if length is None:
            return None
        if spec.ratio is None:
            total += length
            continue
        if spec.ratio < 0:
            divisor = abs(spec.ratio)
            if length % divisor != 0:
                return None
            total += length // divisor
            continue
        total += length * spec.ratio
    return total


def merge_segments(
    segments: list[AnibridgeMappingRange],
) -> list[AnibridgeMappingRange]:
    """Merge overlapping or touching bounded segments."""
    if not segments:
        return []

    normalized = sorted(
        segments,
        key=lambda seg: (seg.start, math.inf if seg.end is None else seg.end),
    )
    merged: list[AnibridgeMappingRange] = [normalized[0]]
    for segment in normalized[1:]:
        prev = merged[-1]
        prev_end = prev.end
        if prev_end is None:
            continue
        if segment.start <= prev_end + 1:
            next_end = segment.end
            if next_end is None:
                merged[-1] = AnibridgeMappingRange(start=prev.start, end=None)
            else:
                merged[-1] = AnibridgeMappingRange(
                    start=prev.start,
                    end=max(prev_end, next_end),
                )
            continue
        merged.append(segment)
    return merged


def has_internal_overlap(spec: TargetSpec) -> bool:
    """Return True if a target spec has overlapping internal segments."""
    if len(spec.segments) <= 1:
        return False
    sorted_segments = sorted(
        spec.segments,
        key=lambda seg: (seg.start, math.inf if seg.end is None else seg.end),
    )
    prev = sorted_segments[0]
    for current in sorted_segments[1:]:
        if ranges_overlap(prev.start, prev.end, current.start, current.end):
            return True
        prev_end_value = math.inf if prev.end is None else prev.end
        current_end_value = math.inf if current.end is None else current.end
        if current_end_value > prev_end_value:
            prev = current
    return False


def normalize_reversed_pair(
    source_range: str,
    target_range: str,
) -> list[tuple[str, str]]:
    """Normalize reversed source/target ranges into schema-valid source entries."""
    normalized_source = source_range.strip()
    normalized_target = target_range.strip()

    if "|" in normalized_source and "," not in normalized_source:
        source_base, source_ratio_raw = normalized_source.rsplit("|", 1)
        try:
            source_ratio = int(source_ratio_raw)
        except ValueError:
            source_ratio = 0
        if source_ratio != 0 and "|" not in normalized_target:
            normalized_source = source_base
            normalized_target = f"{normalized_target}|{-source_ratio}"

    if "," not in normalized_source:
        return [(normalized_source, normalized_target)]

    if "|" in normalized_source or "," in normalized_target or "|" in normalized_target:
        return []

    target = parse_source_segment(normalized_target)
    if target is None or target.end is None:
        return []
    target_start = target.start
    target_length = target.length
    if target_length is None:
        return []

    source_segments: list[AnibridgeMappingRange] = []
    for part in normalized_source.split(","):
        segment = parse_source_segment(part)
        if segment is None or segment.end is None:
            return []
        source_segments.append(segment)
    if not source_segments:
        return []

    source_total = 0
    for segment in source_segments:
        segment_length = segment.length
        if segment_length is None:
            return []
        source_total += segment_length

    target_total = target_length
    if source_total != target_total:
        return []

    out: list[tuple[str, str]] = []
    cursor = target_start
    for segment in source_segments:
        segment_length = segment.length
        if segment_length is None:
            return []
        mapped_end = cursor + segment_length - 1
        out.append((format_mapping_range(segment), format_range(cursor, mapped_end)))
        cursor = mapped_end + 1
    return out


def format_range(start: int, end: int) -> str:
    """Format a range as 'start-end' or 'start' if single unit."""
    if start == end:
        return str(start)
    return f"{start}-{end}"
