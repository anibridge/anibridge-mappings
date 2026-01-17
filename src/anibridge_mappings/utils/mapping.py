"""Shared helpers for manipulating episode mapping graphs."""

import re
from collections.abc import Mapping
from typing import Any

from anibridge_mappings.core.graph import EpisodeMappingGraph

SourceNode = tuple[str, str, str | None]
TargetNode = tuple[str, str, str | None]
SourceTargetMap = dict[
    SourceNode,
    dict[TargetNode, dict[str, set[str]]],
]


def normalize_episode_key(value: str | None) -> str | None:
    """Normalize an episode key to digits or `start-end` ranges.

    Args:
        value (str | None): Raw episode label.

    Returns:
        str | None: Normalized episode key or `None` if invalid.
    """
    if not value:
        return None
    trimmed = value.strip()
    if not trimmed:
        return None
    return trimmed or None


def _split_ratio(range_key: str) -> tuple[str, int | None] | None:
    """Split a range key into base range and optional ratio."""
    if "|" not in range_key:
        return range_key, None
    base, ratio_raw = range_key.split("|", 1)
    if ratio_raw == "":
        return None
    try:
        ratio = int(ratio_raw)
    except ValueError:
        return None
    if ratio == 0:
        return None
    return base, ratio


def parse_range_bounds(range_key: str) -> tuple[int, int | None] | None:
    """Return inclusive (start, end) bounds for a normalized range key.

    Args:
        range_key (str): Normalized range key.

    Returns:
        tuple[int, int | None] | None: Parsed bounds or `None` if invalid.
    """
    if not range_key:
        return None
    normalized = range_key.strip()
    if not normalized:
        return None

    if "," in normalized:
        return None

    split = _split_ratio(normalized)
    if split is None:
        return None
    base, _ratio = split

    if "-" in base:
        left, right = base.split("-", 1)
        try:
            start = int(left)
        except ValueError:
            return None
        end: int | None
        if right == "":
            end = None
        else:
            try:
                end = int(right)
            except ValueError:
                return None
        if end is not None and start > end:
            start, end = end, start
        return (start, end)
    try:
        value = int(base)
    except ValueError:
        return None
    return (value, value)


def build_source_target_map(graph: EpisodeMappingGraph) -> SourceTargetMap:
    """Create a mapping of source scopes to their target range references.

    Args:
        graph (EpisodeMappingGraph): Episode mapping graph.

    Returns:
        SourceTargetMap: Mapping of sources to target range specs.
    """
    source_mappings: SourceTargetMap = {}
    for node in sorted(
        graph.nodes(),
        key=lambda n: (n[0], n[1], "" if n[2] is None else n[2], n[3]),
    ):
        provider, entry_id, scope, source_range_raw = node
        source_range = normalize_episode_key(source_range_raw)
        if source_range is None:
            continue

        for neighbor in sorted(
            graph.neighbors(node),
            key=lambda n: (n[0], n[1], "" if n[2] is None else n[2], n[3]),
        ):
            if neighbor == node:
                continue

            target_provider, target_entry, target_scope, target_range_raw = neighbor
            if (
                provider == target_provider
                and entry_id == target_entry
                and scope == target_scope
            ):
                continue

            target_range = normalize_episode_key(target_range_raw)
            if target_range is None:
                continue

            source_bucket = source_mappings.setdefault((provider, entry_id, scope), {})
            target_bucket = source_bucket.setdefault(
                (target_provider, target_entry, target_scope),
                {},
            )
            target_bucket.setdefault(source_range, set()).add(target_range)

    return source_mappings


def collapse_source_mappings(source_map: Mapping[str, set[str]]) -> dict[str, str]:
    """Collapse raw source-to-target mappings into schema-friendly specs.

    Args:
        source_map (Mapping[str, set[str]]): Raw source-to-target map.

    Returns:
        dict[str, str]: Collapsed source mappings suitable for schema output.
    """
    numeric_entries: list[tuple[tuple[int, int], set[str]]] = []
    special_entries: dict[str, set[str]] = {}

    for source_range, target_ranges in source_map.items():
        source_parts = [
            part.strip() for part in source_range.split(",") if part.strip()
        ]
        for part in source_parts:
            if "|" in part:
                special_entries[part] = target_ranges
                continue
            bounds = parse_range_bounds(part)
            if bounds is not None:
                start, end = bounds
                if end is not None:
                    target_bounds = [
                        parse_range_bounds(value) for value in target_ranges
                    ]
                    all_numeric = all(
                        bound is not None and bound[1] is not None
                        for bound in target_bounds
                    )
                    source_len = end - start + 1
                    numeric_targets = _expand_numeric_targets(target_ranges)
                    if (
                        all_numeric
                        and numeric_targets
                        and len(numeric_targets) == source_len
                    ):
                        numeric_entries.append(((start, end), target_ranges))
                    else:
                        special_entries[part] = target_ranges
                    continue
            special_entries[part] = target_ranges

    result: dict[str, str] = {}
    if numeric_entries:
        result.update(_collapse_numeric_entries(numeric_entries))

    for source_range, target_ranges in special_entries.items():
        spec = _format_special_targets(target_ranges)
        if spec:
            result[source_range] = spec

    # Post-process to merge adjacent numeric source keys that have identical
    # target specs (e.g., "1": "x", "2": "x" -> "1-2": "x"). This
    # reduces excessively fragmented output when contiguous sources map to
    # the same targets.
    return _merge_adjacent_numeric_keys(result)


def _collapse_numeric_entries(
    entries: list[tuple[tuple[int, int], set[str]]],
) -> dict[str, str]:
    """Collapse numeric-only source mappings into schema-friendly specs."""
    per_source: dict[int, list[int]] = {}
    for (source_start, source_end), target_values in entries:
        source_values = list(range(source_start, source_end + 1))
        if not source_values:
            continue
        numeric_targets = _expand_numeric_targets(target_values)
        if not numeric_targets:
            continue

        if len(source_values) == 1:
            per_source[source_values[0]] = numeric_targets
            continue

        if len(numeric_targets) != len(source_values):
            continue

        for idx, source_value in enumerate(source_values):
            per_source[source_value] = [numeric_targets[idx]]

    if not per_source:
        return {}

    contiguous_sources = {
        source: values
        for source, values in per_source.items()
        if _is_contiguous(values)
    }
    non_contiguous_sources = {
        source: values
        for source, values in per_source.items()
        if not _is_contiguous(values)
    }

    mapping: dict[str, str] = {}
    for source_value, targets in non_contiguous_sources.items():
        mapping[str(source_value)] = ",".join(_compress_ranges(targets))

    if contiguous_sources:
        units = _build_units(contiguous_sources)
        segments = _merge_units(units)
        for segment in segments:
            source_start = segment[0]["source_start"]
            source_end = segment[-1]["source_end"]
            source_len = sum(unit["source_len"] for unit in segment)
            target_start = segment[0]["target_start"]
            target_end = segment[-1]["target_end"]
            target_len = target_end - target_start + 1
            ratio = _compute_ratio(source_len, target_len)

            source_key = _format_range(source_start, source_end)
            target_value = _format_target_with_ratio(target_start, target_end, ratio)
            mapping[source_key] = target_value

    return mapping


def _build_units(contiguous_sources: dict[int, list[int]]) -> list[dict[str, int]]:
    """Build contiguous source units grouped by identical targets."""
    sorted_sources = sorted(contiguous_sources)
    units: list[dict[str, int]] = []

    idx = 0
    while idx < len(sorted_sources):
        source = sorted_sources[idx]
        targets = contiguous_sources[source]
        j = idx + 1
        while (
            j < len(sorted_sources)
            and sorted_sources[j] == sorted_sources[j - 1] + 1
            and contiguous_sources[sorted_sources[j]] == targets
        ):
            j += 1

        unit_sources = sorted_sources[idx:j]
        source_len = len(unit_sources)
        target_len = len(targets)
        units.append(
            {
                "source_start": unit_sources[0],
                "source_end": unit_sources[-1],
                "source_len": source_len,
                "target_start": targets[0],
                "target_end": targets[-1],
                "target_len": target_len,
                "ratio_sign": 1 if target_len >= source_len else -1,
            }
        )
        idx = j

    return units


def _merge_units(units: list[dict[str, int]]) -> list[list[dict[str, int]]]:
    """Merge adjacent units into compatible segments."""
    merged: list[list[dict[str, int]]] = []
    idx = 0
    while idx < len(units):
        segment = [units[idx]]
        idx += 1
        while idx < len(units) and _can_merge_unit(segment[-1], units[idx]):
            segment.append(units[idx])
            idx += 1
        merged.append(segment)
    return merged


def _can_merge_unit(prev: dict[str, int], nxt: dict[str, int]) -> bool:
    """Return True if two units can be merged into one segment."""
    if nxt["source_start"] != prev["source_end"] + 1:
        return False
    if nxt["ratio_sign"] != prev["ratio_sign"]:
        return False

    if prev["ratio_sign"] == 1:
        return (
            nxt["target_len"] == prev["target_len"]
            and nxt["target_start"] == prev["target_end"] + 1
        )

    return (
        nxt["target_len"] == prev["target_len"]
        and nxt["source_len"] == prev["source_len"]
        and nxt["target_start"] == prev["target_start"] + prev["target_len"]
    )


def _compute_ratio(source_len: int, target_len: int) -> int | None:
    """Compute an integer ratio between source and target lengths."""
    if target_len == source_len:
        return None
    larger, smaller, sign = (
        (target_len, source_len, 1)
        if target_len > source_len
        else (source_len, target_len, -1)
    )
    if smaller == 0 or larger % smaller != 0:
        return None
    return sign * (larger // smaller)


def _format_special_targets(targets: set[str]) -> str:
    """Format target ranges for non-numeric source keys."""
    numeric = sorted({int(value) for value in targets if value.isdigit()})
    if numeric:
        return ",".join(_compress_ranges(numeric))

    return ",".join(sorted(targets))


def _compress_ranges(values: list[int]) -> list[str]:
    """Compress sorted integers into range strings."""
    if not values:
        return []
    ranges: list[str] = []
    start = prev = values[0]

    for current in values[1:]:
        if current == prev + 1:
            prev = current
            continue
        ranges.append(_format_range(start, prev))
        start = prev = current

    ranges.append(_format_range(start, prev))
    return ranges


def _format_range(start: int, end: int) -> str:
    """Format a numeric range label."""
    if start == end:
        return str(start)
    return f"{start}-{end}"


def _format_target_with_ratio(start: int, end: int, ratio: int | None) -> str:
    """Format a target range with an optional ratio suffix."""
    base = _format_range(start, end)
    if ratio is None or ratio == 1:
        return base
    return f"{base}|{ratio}"


def _is_contiguous(values: list[int]) -> bool:
    """Return True if a list of integers is strictly contiguous."""
    return all(values[idx] == values[idx - 1] + 1 for idx in range(1, len(values)))


def _expand_numeric_targets(values: set[str]) -> list[int]:
    """Expand numeric target range strings into integers."""
    numbers: set[int] = set()
    for value in values:
        bounds = parse_range_bounds(value)
        if bounds is not None and bounds[1] is not None:
            numbers.update(range(bounds[0], bounds[1] + 1))
        elif value.isdigit():
            numbers.add(int(value))
    return sorted(numbers)


def _parse_source_key(key: str) -> tuple[int, int] | None:
    """Return (start, end) for numeric source key or None for non-numeric."""
    if "|" in key or "," in key:
        return None
    bounds = parse_range_bounds(key)
    if bounds is None or bounds[1] is None:
        return None
    start, end = bounds
    if end is None:
        return None
    return (start, end)


def _format_source_key(start: int, end: int) -> str:
    """Format a source key from numeric bounds."""
    return str(start) if start == end else f"{start}-{end}"


def _merge_adjacent_numeric_keys(mapping: dict[str, str]) -> dict[str, str]:
    """Aggressively merge numeric source keys."""
    # Separate numeric and non-numeric entries
    numeric_items: list[tuple[int, int, str]] = []
    others: dict[str, str] = {}
    for k, v in mapping.items():
        bounds = _parse_source_key(k)
        if bounds is None:
            others[k] = v
            continue
        numeric_items.append((bounds[0], bounds[1], v))

    if not numeric_items:
        return {**mapping}

    # Expand numeric_items into per-episode rows
    episodes: list[tuple[int, str, set[int]]] = []
    for start, end, val in sorted(numeric_items, key=lambda t: t[0]):
        for ep in range(start, end + 1):
            pieces = {p.strip() for p in val.split(",") if p.strip()}
            expanded = set(_expand_numeric_targets(pieces))
            episodes.append((ep, val, expanded))

    n = len(episodes)
    idx = 0
    out_entries: list[tuple[int, int, str]] = []

    while idx < n:
        # Try to find the largest j >= idx such that episodes[idx..j] form a
        # contiguous source run and the union of their expanded targets is a
        # contiguous range with size == number of source episodes in the run.
        union_set: set[int] = set()
        found = False
        for j in range(idx, n):
            # ensure sources are contiguous numerically
            if j > idx and episodes[j][0] != episodes[j - 1][0] + 1:
                break
            union_set |= episodes[j][2]
            if not union_set:
                continue
            umin = min(union_set)
            umax = max(union_set)
            if (umax - umin + 1) == len(union_set) and len(union_set) == (j - idx + 1):
                # successful aggressive collapse for this run
                out_entries.append(
                    (episodes[idx][0], episodes[j][0], _format_source_key(umin, umax))
                )
                idx = j + 1
                found = True
                break

        if found:
            continue

        # Fallback: merge consecutive episodes that have identical formatted
        # target specs.
        cur_ep, cur_val, _ = episodes[idx]
        run_start = cur_ep
        run_end = cur_ep
        k = idx + 1
        while k < n and episodes[k][0] == run_end + 1 and episodes[k][1] == cur_val:
            run_end = episodes[k][0]
            k += 1
        out_entries.append((run_start, run_end, cur_val))
        idx = k

    # Build output mapping from out_entries
    out: dict[str, str] = {}
    for s, e, v in out_entries:
        out[_format_source_key(s, e)] = v

    # Append non-numeric keys afterwards
    out.update(others)
    return out


def _provider_scope_sort_key(k: str):
    """Return a sort key for provider-scoped mapping descriptors."""
    if k.startswith("$"):
        return (0, k, "")

    match = re.match(
        r"^(?P<provider>[a-zA-Z_][a-zA-Z0-9_]*):(?P<id>[^:]+)(?::(?P<scope>[^:]+))?$",
        k,
    )
    if not match:
        return (2, k, "")

    provider = match.group("provider")
    id_str = match.group("id")
    scope = match.group("scope")
    id_key = (0, int(id_str)) if id_str.isdigit() else (1, id_str)
    if scope is None:
        scope_key = (0, "")
    else:
        scope_match = re.match(r"^s([0-9]+)$", scope)
        scope_key = (1, int(scope_match.group(1))) if scope_match else (2, scope)
    return (1, provider, id_key, scope_key)


def ordered_payload(d: dict[str, Any]) -> dict[str, Any]:
    """Return a new dict with ordered keys for stable serialization.

    Args:
        d (dict[str, Any]): Mapping payload.

    Returns:
        dict[str, Any]: Ordered mapping payload.
    """
    items: list[tuple[str, Any]] = []
    if "$meta" in d:
        items.append(("$meta", d["$meta"]))

    top_keys = [k for k in d if k != "$meta"]
    for k in sorted(top_keys, key=_provider_scope_sort_key):
        v = d[k]
        if isinstance(v, dict):
            inner: dict[str, Any] = {}
            for tk in sorted(v.keys(), key=_provider_scope_sort_key):
                ranges = v[tk]
                if isinstance(ranges, dict):

                    def _range_key(rk: str):
                        bounds = parse_range_bounds(rk)
                        if bounds is not None:
                            return (0, bounds[0], rk)
                        return (1, rk)

                    sorted_ranges = dict(
                        sorted(ranges.items(), key=lambda it: _range_key(it[0]))
                    )
                    inner[tk] = sorted_ranges
                else:
                    inner[tk] = ranges
            items.append((k, inner))
        else:
            items.append((k, v))

    return dict(items)
