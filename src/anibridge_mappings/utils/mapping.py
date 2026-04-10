"""Shared helpers for manipulating episode mapping graphs."""

import re
from collections.abc import Mapping
from typing import Any, cast

from anibridge.utils.mappings import parse_mapping_descriptor

from anibridge_mappings.core.graph import EpisodeMappingGraph
from anibridge_mappings.core.range_specs import (
    TargetSpec,
    format_range,
    format_target_spec,
    merge_segments,
    normalize_reversed_pair,
    parse_source_segment,
    parse_target_spec,
    range_bounds,
)

type SourceNode = tuple[str, str, str | None]
type TargetNode = tuple[str, str, str | None]
type SourceTargetMap = dict[
    SourceNode,
    dict[TargetNode, dict[str, set[str]]],
]
type SortPart = tuple[int, int | str]
type ProviderScopeSortKey = tuple[int, str, SortPart, SortPart]


def parse_descriptor(descriptor: str) -> tuple[str, str, str | None]:
    """Parse provider descriptor strings into tuple form."""
    return parse_mapping_descriptor(descriptor)


def format_descriptor(provider: str, entry_id: str, scope: str | None) -> str:
    """Format a provider descriptor from tuple parts."""
    if scope is None or scope == "":
        return f"{provider}:{entry_id}"
    return f"{provider}:{entry_id}:{scope}"


def normalize_episode_key(value: str | None) -> str | None:
    """Normalize one episode key string."""
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def parse_range_bounds(range_key: str) -> tuple[int, int | None] | None:
    """Return inclusive bounds for a single range expression."""
    return range_bounds(range_key)


def build_source_target_map(graph: EpisodeMappingGraph) -> SourceTargetMap:
    """Create a mapping of source scopes to their target range references."""
    source_mappings: SourceTargetMap = {}

    for node in sorted(
        graph.nodes(),
        key=lambda n: (n[0], n[1], n[2] or "", n[3]),
    ):
        provider, entry_id, scope, source_raw = node
        source_range = normalize_episode_key(source_raw)
        if source_range is None:
            continue

        for neighbor in sorted(
            graph.neighbors(node),
            key=lambda n: (n[0], n[1], n[2] or "", n[3]),
        ):
            if neighbor == node:
                continue

            target_provider, target_entry, target_scope, target_raw = neighbor
            if (
                provider == target_provider
                and entry_id == target_entry
                and scope == target_scope
            ):
                continue

            target_range = normalize_episode_key(target_raw)
            if target_range is None:
                continue

            normalized_pairs = normalize_reversed_pair(source_range, target_range)
            if not normalized_pairs:
                continue

            source_bucket = source_mappings.setdefault((provider, entry_id, scope), {})
            target_bucket = source_bucket.setdefault(
                (target_provider, target_entry, target_scope),
                {},
            )
            for normalized_source, normalized_target in normalized_pairs:
                target_bucket.setdefault(
                    normalized_source,
                    set(),
                ).add(normalized_target)

    return source_mappings


def collapse_source_mappings(source_map: Mapping[str, set[str]]) -> dict[str, str]:
    """Collapse raw source-to-target mappings into schema-friendly specs."""
    collapsed: dict[str, str] = {}

    for source_range, target_ranges in source_map.items():
        normalized_source = source_range.strip()
        if parse_source_segment(normalized_source) is None:
            continue

        specs: list[TargetSpec] = []
        raw_targets: list[str] = []
        for value in sorted({item.strip() for item in target_ranges if item.strip()}):
            spec = parse_target_spec(value)
            if spec is None:
                raw_targets.append(value)
                continue
            specs.append(spec)

        if raw_targets:
            collapsed[normalized_source] = ",".join(raw_targets)
            continue
        if not specs:
            continue

        ratios = {spec.ratio for spec in specs}
        if len(ratios) > 1:
            # If there are multiple ratios, we can't merge specs, so we choose the
            # longest one (for determinism) and discard the rest since they conflict.
            chosen = max(specs, key=lambda spec: _bounded_length(spec.segments))
            collapsed[normalized_source] = format_target_spec(chosen)
            continue

        ratio = next(iter(ratios))
        merged = merge_segments([seg for spec in specs for seg in spec.segments])
        collapsed[normalized_source] = format_target_spec(
            TargetSpec(segments=tuple(merged), ratio=ratio)
        )

    return _merge_adjacent_linear_keys(_merge_adjacent_numeric_keys(collapsed))


def _bounded_length(segments: tuple[Any, ...]) -> int:
    total = 0
    for segment in segments:
        length = segment.length
        if length is None:
            return 10**9
        total += length
    return total


def _source_bounds(key: str) -> tuple[int, int] | None:
    """Return inclusive start and end bounds for a source key."""
    bounds = parse_range_bounds(key)
    if bounds is None or bounds[1] is None:
        return None
    return bounds[0], bounds[1]


def _merge_adjacent_numeric_keys(mapping: dict[str, str]) -> dict[str, str]:
    """Merge adjacent numeric source keys that share identical target specs."""
    numeric: list[tuple[int, int, str]] = []
    others: dict[str, str] = {}

    for key, value in mapping.items():
        bounds = _source_bounds(key)
        if bounds is None:
            others[key] = value
            continue
        numeric.append((bounds[0], bounds[1], value))

    if not numeric:
        return dict(mapping)

    numeric.sort(key=lambda item: (item[0], item[1], item[2]))
    merged: dict[str, str] = {}
    run_start, run_end, run_value = numeric[0]

    for start, end, value in numeric[1:]:
        if value == run_value and start == run_end + 1:
            run_end = end
            continue
        merged[format_range(run_start, run_end)] = run_value
        run_start, run_end, run_value = start, end, value

    merged[format_range(run_start, run_end)] = run_value
    merged.update(others)
    return merged


def _merge_adjacent_linear_keys(mapping: dict[str, str]) -> dict[str, str]:
    """Merge contiguous source ranges when target ranges advance linearly too."""
    linear_items: list[tuple[int, int, int, int, int | None]] = []
    passthrough: dict[str, str] = {}

    for source_key, target_value in mapping.items():
        source_bounds = _source_bounds(source_key)
        if source_bounds is None:
            passthrough[source_key] = target_value
            continue

        target_spec = parse_target_spec(target_value)
        if target_spec is None or len(target_spec.segments) != 1:
            passthrough[source_key] = target_value
            continue

        segment = target_spec.segments[0]
        if segment.end is None:
            passthrough[source_key] = target_value
            continue

        linear_items.append(
            (
                source_bounds[0],
                source_bounds[1],
                segment.start,
                segment.end,
                target_spec.ratio,
            )
        )

    if not linear_items:
        return dict(mapping)

    linear_items.sort(key=lambda item: (item[0], item[1], item[2], item[3]))
    merged: dict[str, str] = {}

    run_source_start, run_source_end, run_target_start, run_target_end, run_ratio = (
        linear_items[0]
    )
    run_offset = run_target_start - run_source_start

    for source_start, source_end, target_start, target_end, ratio in linear_items[1:]:
        can_merge = (
            ratio == run_ratio
            and source_start == run_source_end + 1
            and target_start == run_target_end + 1
            and (target_start - source_start) == run_offset
        )
        if can_merge:
            run_source_end = source_end
            run_target_end = target_end
            continue

        merged[format_range(run_source_start, run_source_end)] = _format_target_value(
            run_target_start,
            run_target_end,
            run_ratio,
        )
        run_source_start = source_start
        run_source_end = source_end
        run_target_start = target_start
        run_target_end = target_end
        run_ratio = ratio
        run_offset = run_target_start - run_source_start

    merged[format_range(run_source_start, run_source_end)] = _format_target_value(
        run_target_start,
        run_target_end,
        run_ratio,
    )
    merged.update(passthrough)
    return merged


def _format_target_value(start: int, end: int, ratio: int | None) -> str:
    """Output a string representation of a target range."""
    base = format_range(start, end)
    return base if ratio is None else f"{base}|{ratio}"


def provider_scope_sort_key(k: str) -> ProviderScopeSortKey:
    """Return a sort key for provider-scoped mapping descriptors."""
    if k.startswith("$"):
        return (0, k, (0, ""), (0, ""))

    match = re.match(
        r"^(?P<provider>[a-zA-Z_][a-zA-Z0-9_]*):(?P<id>[^:]+)(?::(?P<scope>[^:]+))?$",
        k,
    )
    if not match:
        return (2, k, (0, ""), (0, ""))

    provider = cast(str, match.group("provider"))
    id_str = cast(str, match.group("id"))
    scope = cast(str | None, match.group("scope"))
    id_key = (0, int(id_str)) if id_str.isdigit() else (1, id_str)

    if scope is None:
        scope_key = (0, "")
    else:
        scope_upper = scope.upper()
        anidb_scope_order = {"R": 0, "S": 1, "O": 2, "C": 3, "T": 4, "P": 5}
        if scope_upper in anidb_scope_order:
            scope_key = (1, anidb_scope_order[scope_upper])
        else:
            scope_match = re.match(r"^s([0-9]+)$", scope)
            scope_key = (2, int(scope_match.group(1))) if scope_match else (3, scope)

    return (1, provider, id_key, scope_key)


def ordered_payload(d: dict[str, Any]) -> dict[str, Any]:
    """Return a new dict with ordered keys for stable serialization."""
    items: list[tuple[str, Any]] = []
    if "$meta" in d:
        items.append(("$meta", d["$meta"]))

    for key in sorted((k for k in d if k != "$meta"), key=provider_scope_sort_key):
        value = d[key]
        if not isinstance(value, dict):
            items.append((key, value))
            continue

        inner: dict[str, Any] = {}
        for target_key in sorted(
            value.keys(), key=lambda k: provider_scope_sort_key(k)
        ):
            if not isinstance(target_key, str):
                continue
            ranges = value[target_key]
            if isinstance(ranges, dict):
                sorted_ranges = dict(
                    sorted(
                        ranges.items(),
                        key=lambda item: _range_sort_key(item[0]),
                    )
                )
                inner[target_key] = sorted_ranges
            else:
                inner[target_key] = ranges
        items.append((key, inner))

    return dict(items)


def _range_sort_key(key: str):
    """Sort key for YAML sorting."""
    bounds = parse_range_bounds(key)
    if bounds is None:
        return 1, key
    return 0, bounds[0], key
