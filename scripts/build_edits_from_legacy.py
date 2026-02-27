"""Build `mappings.edits.yaml` from a legacy `mappings.json` payload.

This script migrates legacy AniBridge/PAB mapping data into the edit schema
used by this repository. It includes best-effort handling for:

- ambiguous/open-ended ranges,
- source-side comma ranges (unsupported in current schema),
- ID-only links (by inferring episode mappings from metadata),
- overlap cleanup across targets in the same provider namespace.
"""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap
from ruamel.yaml.scalarstring import DoubleQuotedScalarString

from anibridge_mappings.utils.mapping import provider_scope_sort_key

DESCRIPTOR_RE = re.compile(
    r"^(?P<provider>[a-zA-Z_][a-zA-Z0-9_]*):(?P<id>[^:]+)(?::(?P<scope>[^:]+))?$"
)
RANGE_SEGMENT_RE = re.compile(
    r"^(?P<start>\d+)(?:-(?P<end>\d*))?(?:\|(?P<ratio>-?\d+))?$"
)
LEGACY_EP_SEGMENT_RE = re.compile(
    r"^e(?P<start>\d+)(?:-e?(?P<end>\d*))?(?:\|(?P<ratio>-?\d+))?$",
    re.IGNORECASE,
)

SHOW_PROVIDERS = {"tvdb_show", "tmdb_show"}
MOVIE_PROVIDERS = {"tvdb_movie", "tmdb_movie"}
TVDB_ALIASES = {"tvdb", "thetvdb", "tvdb_show", "tvdb_movie"}
TMDB_ALIASES = {"tmdb", "themoviedb", "tmdb_show", "tmdb_movie"}


@dataclass(slots=True)
class Descriptor:
    """Parsed `provider:id[:scope]` descriptor."""

    provider: str
    entry_id: str
    scope: str | None

    def to_string(self) -> str:
        """Render normalized descriptor string."""
        if self.scope is None:
            return f"{self.provider}:{self.entry_id}"
        return f"{self.provider}:{self.entry_id}:{self.scope}"


@dataclass(slots=True)
class RangeSegment:
    """Parsed single range segment `x[-y][|ratio]`."""

    start: int
    end: int | None
    ratio: int | None

    @property
    def open_ended(self) -> bool:
        """Return True when the range has no explicit ending bound."""
        return self.end is None

    def length(self) -> int | None:
        """Return inclusive segment length, if closed."""
        if self.end is None:
            return None
        return self.end - self.start + 1

    def format(self) -> str:
        """Serialize segment to canonical string representation."""
        if self.end is None:
            base = f"{self.start}-"
        elif self.start == self.end:
            base = str(self.start)
        else:
            base = f"{self.start}-{self.end}"
        if self.ratio is None or self.ratio == 1:
            return base
        return f"{base}|{self.ratio}"


@dataclass(slots=True)
class MappingCandidate:
    """Normalized mapping row before final emission."""

    source: str
    target: str
    target_provider: str
    source_range: str
    target_range: str
    priority: int


@dataclass(slots=True)
class Stats:
    """Execution statistics for reporting."""

    parsed_explicit_pairs: int = 0
    parsed_id_only_links: int = 0
    inferred_id_only_pairs: int = 0
    unresolved_id_only_links: int = 0
    resolved_open_ranges: int = 0
    unresolved_open_ranges: int = 0
    overlap_dropped_pairs: int = 0
    invalid_pairs_dropped: int = 0


class MetaIndex:
    """Small metadata accessor over `data/meta/*.json` caches."""

    def __init__(self, meta_dir: Path) -> None:
        """Load metadata files used for migration heuristics."""
        self._anilist = self._load(meta_dir / "anilist_meta.json")
        self._tvdb_show = self._load(meta_dir / "tvdb_show.json")
        self._tvdb_movie = self._load(meta_dir / "tvdb_movie.json")
        self._tmdb_show = self._load(meta_dir / "tmdb_meta.json")

    @staticmethod
    def _load(path: Path) -> dict[str, Any]:
        """Load a source metadata JSON file or return an empty payload."""
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text())
        except json.JSONDecodeError:
            return {}
        entries = data.get("entries")
        return entries if isinstance(entries, dict) else {}

    def _episode_from_scopes(
        self,
        source: dict[str, Any],
        entry_id: str,
        scope: str | None,
        *,
        default_show_scope: bool = False,
    ) -> int | None:
        """Return episode count from a provider metadata cache."""
        payload = source.get(entry_id)
        if not isinstance(payload, dict):
            return None

        normalized_scope = _normalize_scope(scope)
        preferred_scopes: list[str] = []

        if normalized_scope is not None:
            preferred_scopes.append(normalized_scope)
        preferred_scopes.extend(["", "s1"])

        for preferred in preferred_scopes:
            candidate = payload.get(preferred)
            if isinstance(candidate, dict):
                episodes = candidate.get("episodes")
                if isinstance(episodes, int) and episodes > 0:
                    return episodes

        if default_show_scope and normalized_scope is None and len(payload) == 1:
            only = next(iter(payload.values()))
            if isinstance(only, dict):
                episodes = only.get("episodes")
                if isinstance(episodes, int) and episodes > 0:
                    return episodes
        return None

    def episodes(self, descriptor: Descriptor) -> int | None:
        """Return known episode count for a descriptor, when available."""
        provider = descriptor.provider
        if provider == "anilist":
            return self._episode_from_scopes(
                self._anilist,
                descriptor.entry_id,
                descriptor.scope,
            )
        if provider == "tvdb_show":
            return self._episode_from_scopes(
                self._tvdb_show,
                descriptor.entry_id,
                descriptor.scope,
                default_show_scope=True,
            )
        if provider == "tmdb_show":
            return self._episode_from_scopes(
                self._tmdb_show,
                descriptor.entry_id,
                descriptor.scope,
                default_show_scope=True,
            )
        if provider == "tvdb_movie":
            return self._episode_from_scopes(
                self._tvdb_movie,
                descriptor.entry_id,
                None,
            )
        if provider == "tmdb_movie":
            # TMDB movie metadata is not currently cached separately.
            return 1
        return None

    def has_show(self, provider: str, entry_id: str) -> bool:
        """Return whether a show-like metadata record exists."""
        if provider == "tvdb_show":
            return entry_id in self._tvdb_show
        if provider == "tmdb_show":
            return entry_id in self._tmdb_show
        return False

    def has_movie(self, provider: str, entry_id: str) -> bool:
        """Return whether a movie-like metadata record exists."""
        if provider == "tvdb_movie":
            return entry_id in self._tvdb_movie
        return False

    def show_scopes(self, provider: str, entry_id: str) -> dict[str, int]:
        """Return season scopes with episode counts for a show provider."""
        if provider == "tvdb_show":
            payload = self._tvdb_show.get(entry_id)
        elif provider == "tmdb_show":
            payload = self._tmdb_show.get(entry_id)
        else:
            payload = None
        if not isinstance(payload, dict):
            return {}

        scopes: dict[str, int] = {}
        for scope, meta in payload.items():
            normalized_scope = _normalize_scope(scope)
            if normalized_scope is None:
                continue
            if not isinstance(meta, dict):
                continue
            episodes = meta.get("episodes")
            if isinstance(episodes, int) and episodes > 0:
                scopes[normalized_scope] = episodes
        return scopes


def _normalize_scope(scope: str | None) -> str | None:
    """Normalize scope representations to canonical schema form."""
    if scope is None:
        return None
    value = str(scope).strip()
    if value in {"", "null", "None"}:
        return None

    season_match = re.match(r"^(?:s|season)?(\d+)$", value, re.IGNORECASE)
    if season_match:
        return f"s{int(season_match.group(1))}"

    return value


def _parse_descriptor(raw: str) -> Descriptor | None:
    """Parse descriptor string into provider, id and optional scope."""
    match = DESCRIPTOR_RE.match(raw.strip())
    if not match:
        return None
    provider = match.group("provider")
    entry_id = match.group("id")
    scope = _normalize_scope(match.group("scope"))
    return Descriptor(provider=provider, entry_id=entry_id, scope=scope)


def _normalize_source_descriptor(descriptor: Descriptor) -> Descriptor:
    """Normalize source descriptors to current schema conventions."""
    if descriptor.provider == "anilist":
        return Descriptor(provider="anilist", entry_id=descriptor.entry_id, scope=None)
    return descriptor


def _normalize_target_descriptor(
    descriptor: Descriptor,
    source: Descriptor,
    meta: MetaIndex,
) -> Descriptor:
    """Normalize legacy target descriptors to current schema conventions."""
    provider = descriptor.provider.lower()
    scope = descriptor.scope

    if provider in TVDB_ALIASES:
        if provider == "tvdb_movie":
            return Descriptor("tvdb_movie", descriptor.entry_id, None)
        if scope is not None:
            return Descriptor("tvdb_show", descriptor.entry_id, scope)
        if meta.has_movie("tvdb_movie", descriptor.entry_id) and not meta.has_show(
            "tvdb_show", descriptor.entry_id
        ):
            return Descriptor("tvdb_movie", descriptor.entry_id, None)
        return Descriptor("tvdb_show", descriptor.entry_id, "s1")

    if provider in TMDB_ALIASES:
        if provider == "tmdb_movie":
            return Descriptor("tmdb_movie", descriptor.entry_id, None)
        if scope is not None:
            return Descriptor("tmdb_show", descriptor.entry_id, scope)
        source_episodes = meta.episodes(source)
        if source_episodes == 1:
            return Descriptor("tmdb_movie", descriptor.entry_id, None)
        return Descriptor("tmdb_show", descriptor.entry_id, "s1")

    return Descriptor(descriptor.provider, descriptor.entry_id, scope)


def _parse_segment(raw: str) -> RangeSegment | None:
    """Parse a single range segment (`x[-y][|ratio]`)."""
    text = raw.strip()
    match = RANGE_SEGMENT_RE.match(text)
    if not match:
        return None
    start = int(match.group("start"))
    end_raw = match.group("end")
    ratio_raw = match.group("ratio")

    end: int | None
    if end_raw is None:
        end = start
    elif end_raw == "":
        end = None
    else:
        end = int(end_raw)
        if end < start:
            start, end = end, start

    ratio = int(ratio_raw) if ratio_raw is not None else None
    if ratio == 0:
        return None
    return RangeSegment(start=start, end=end, ratio=ratio)


def _parse_target_spec(target_spec: str) -> list[RangeSegment] | None:
    """Parse target range spec, including comma-separated segments."""
    segments: list[RangeSegment] = []
    for part in str(target_spec).split(","):
        part = part.strip()
        if not part:
            continue
        segment = _parse_segment(part)
        if segment is None:
            return None
        segments.append(segment)
    return segments if segments else None


def _segment_source_units(segment: RangeSegment) -> int | None:
    """Return source episode units represented by a target segment."""
    length = segment.length()
    if length is None:
        return None
    ratio = segment.ratio
    if ratio is None:
        return length
    if ratio > 0:
        if length % ratio != 0:
            return None
        return length // ratio
    return length * abs(ratio)


def _merge_target_specs(specs: set[str]) -> str | None:
    """Merge multiple target specs for the same source range."""
    all_segments: dict[tuple[int, int | None, int | None], RangeSegment] = {}
    for spec in specs:
        parsed = _parse_target_spec(spec)
        if parsed is None:
            return None
        for seg in parsed:
            all_segments[(seg.start, seg.end, seg.ratio)] = seg

    ordered = sorted(
        all_segments.values(),
        key=lambda s: (s.start, 10**9 if s.end is None else s.end, s.ratio or 1),
    )
    return ",".join(segment.format() for segment in ordered)


def _legacy_target_spec_to_current(
    raw: str | None,
    target: Descriptor,
    meta: MetaIndex,
) -> str | None:
    """Convert legacy `e...` target mapping syntax into current syntax."""
    if raw is None or str(raw).strip() == "":
        total = meta.episodes(target)
        return "1-" if total is None else RangeSegment(1, total, None).format()

    out_segments: list[str] = []
    for segment in str(raw).split(","):
        token = segment.strip()
        if not token:
            continue
        match = LEGACY_EP_SEGMENT_RE.match(token)
        if not match:
            return None
        start = int(match.group("start"))
        end_raw = match.group("end")
        ratio_raw = match.group("ratio")

        end: int | None
        if end_raw is None:
            end = start
        elif end_raw == "":
            end = None
        else:
            end = int(end_raw)
            if end < start:
                start, end = end, start

        # Legacy PAB semantics used the opposite ratio sign convention.
        ratio = -int(ratio_raw) if ratio_raw is not None else None
        seg = RangeSegment(start=start, end=end, ratio=ratio)
        out_segments.append(seg.format())

    merged = ",".join(out_segments)
    # Attempt to close open ranges with target metadata when available.
    parsed = _parse_target_spec(merged)
    if parsed is None:
        return None
    target_total = meta.episodes(target)
    if target_total is not None:
        for segment in parsed:
            if segment.end is None and target_total >= segment.start:
                segment.end = target_total
    return ",".join(segment.format() for segment in parsed)


def _season_sort_key(scope: str) -> tuple[int, str]:
    """Sort season-like keys (`s0`, `s1`, ...) numerically."""
    normalized = _normalize_scope(scope)
    if normalized is None:
        return (10**9, scope)
    match = re.match(r"^s(\d+)$", normalized)
    if match:
        return (int(match.group(1)), normalized)
    return (10**9, normalized)


def _best_show_scope(
    provider: str,
    entry_id: str,
    source_total: int | None,
    meta: MetaIndex,
) -> str | None:
    """Pick a best-effort season scope for show IDs lacking explicit mappings."""
    scopes = meta.show_scopes(provider, entry_id)
    if not scopes:
        return None
    if source_total is not None:
        for scope, episodes in scopes.items():
            if episodes == source_total:
                return scope
    ordered = sorted(scopes.keys(), key=_season_sort_key)
    return ordered[0] if ordered else None


def _extract_legacy_show_mappings(
    *,
    source_key: str,
    source_total: int | None,
    mappings: dict[str, Any],
    provider: str,
    target_id: str,
    meta: MetaIndex,
    explicit: dict[str, dict[str, dict[str, set[str]]]],
) -> int:
    """Extract explicit season mappings from a legacy TVDB/TMDB mapping object."""
    items: list[tuple[Descriptor, str, int | None]] = []
    for raw_scope in sorted(mappings.keys(), key=_season_sort_key):
        target_scope = _normalize_scope(raw_scope)
        target = Descriptor(provider, target_id, target_scope)
        spec = _legacy_target_spec_to_current(mappings.get(raw_scope), target, meta)
        if spec is None:
            continue
        parsed = _parse_target_spec(spec)
        if parsed is None:
            continue
        units: int | None = 0
        for segment in parsed:
            source_units = _segment_source_units(segment)
            if source_units is None:
                units = None
                break
            units += source_units
        items.append((target, ",".join(segment.format() for segment in parsed), units))

    if not items:
        return 0

    known_units = sum(units for _target, _spec, units in items if units is not None)
    unknown_indices = [
        idx for idx, (_target, _spec, units) in enumerate(items) if units is None
    ]
    if source_total is not None and len(unknown_indices) == 1:
        missing = source_total - known_units
        if missing > 0:
            idx = unknown_indices[0]
            target, spec, _units = items[idx]
            items[idx] = (target, spec, missing)

    generated = 0
    next_source = 1
    for idx, (target, spec, units) in enumerate(items):
        if units is None:
            source_range = f"{next_source}-"
            explicit[source_key][target.to_string()][source_range].add(spec)
            generated += 1
            if idx < len(items) - 1:
                break
            continue

        source_end = next_source + units - 1
        source_range = str(next_source)
        if next_source != source_end:
            source_range = f"{next_source}-{source_end}"
        explicit[source_key][target.to_string()][source_range].add(spec)
        generated += 1
        next_source = source_end + 1
    return generated


def _infer_id_only_pair(
    source: Descriptor,
    target: Descriptor,
    meta: MetaIndex,
) -> tuple[str, str] | None:
    """Infer an episode mapping for an ID-only relation."""
    src_total = meta.episodes(source)
    tgt_total = meta.episodes(target)

    if src_total is None and target.provider in MOVIE_PROVIDERS:
        src_total = 1
    if tgt_total is None and source.provider == "anilist" and src_total == 1:
        tgt_total = 1

    if src_total is None or tgt_total is None or src_total <= 0 or tgt_total <= 0:
        return None

    if src_total == tgt_total:
        src_seg = RangeSegment(1, src_total, None)
        tgt_seg = RangeSegment(1, tgt_total, None)
        return src_seg.format(), tgt_seg.format()

    if tgt_total % src_total == 0:
        ratio = tgt_total // src_total
        src_seg = RangeSegment(1, src_total, None)
        tgt_seg = RangeSegment(1, tgt_total, ratio)
        return src_seg.format(), tgt_seg.format()

    if src_total % tgt_total == 0:
        ratio = -(src_total // tgt_total)
        src_seg = RangeSegment(1, src_total, None)
        tgt_seg = RangeSegment(1, tgt_total, ratio)
        return src_seg.format(), tgt_seg.format()

    # Conservative fallback: only map the common prefix explicitly.
    common = min(src_total, tgt_total)
    if common <= 0:
        return None
    src_seg = RangeSegment(1, common, None)
    tgt_seg = RangeSegment(1, common, None)
    return src_seg.format(), tgt_seg.format()


def _resolve_ambiguous_pair(
    source_desc: Descriptor,
    target_desc: Descriptor,
    source_range: str,
    target_range: str,
    meta: MetaIndex,
    *,
    next_start_hint: int | None = None,
) -> tuple[str, str, bool]:
    """Resolve open-ended ranges with metadata-backed heuristics."""
    source_seg = _parse_segment(source_range)
    target_segments = _parse_target_spec(target_range)
    if source_seg is None or target_segments is None:
        return source_range, target_range, False

    was_ambiguous = source_seg.open_ended or any(
        segment.open_ended for segment in target_segments
    )
    if not was_ambiguous:
        return (
            source_seg.format(),
            ",".join(seg.format() for seg in target_segments),
            False,
        )

    source_total = meta.episodes(source_desc)
    target_total = meta.episodes(target_desc)

    if source_seg.end is None:
        if source_total is not None and source_total >= source_seg.start:
            source_seg.end = source_total
        elif next_start_hint is not None and next_start_hint > source_seg.start:
            source_seg.end = next_start_hint - 1
        elif len(target_segments) == 1:
            units = _segment_source_units(target_segments[0])
            if units is not None and units > 0:
                source_seg.end = source_seg.start + units - 1

    for segment in target_segments:
        if segment.end is not None:
            continue
        if target_total is not None and target_total >= segment.start:
            segment.end = target_total
            continue

        if source_seg.end is None or len(target_segments) != 1:
            continue

        source_length = source_seg.length()
        if source_length is None:
            continue
        ratio = segment.ratio
        if ratio is None:
            target_length = source_length
        elif ratio > 0:
            target_length = source_length * ratio
        else:
            divisor = abs(ratio)
            target_length = max(1, source_length // divisor)
        segment.end = segment.start + target_length - 1

    return source_seg.format(), ",".join(seg.format() for seg in target_segments), True


def _bounds_for_overlap(source_range: str) -> tuple[int, int | None] | None:
    """Return source range bounds for overlap checks."""
    segment = _parse_segment(source_range)
    if segment is None:
        return None
    return segment.start, segment.end


def _overlap(
    left: tuple[int, int | None],
    right: tuple[int, int | None],
) -> bool:
    """Return whether two inclusive ranges overlap."""
    l_start, l_end = left
    r_start, r_end = right
    return not (
        (l_end is not None and l_end < r_start)
        or (r_end is not None and r_end < l_start)
    )


def _sort_source_range_key(range_key: str) -> tuple[int, int, str]:
    """Sort ranges by numeric start/end for stable output."""
    bounds = _bounds_for_overlap(range_key)
    if bounds is None:
        return (10**9, 10**9, range_key)
    start, end = bounds
    return (start, 10**9 if end is None else end, range_key)


def _looks_like_pab_legacy(legacy: dict[str, Any]) -> bool:
    """Return whether payload matches legacy AniList-keyed PAB schema."""
    if not legacy:
        return False
    if not all(str(key).isdigit() for key in legacy):
        return False
    sample = next(iter(legacy.values()))
    if not isinstance(sample, dict):
        return False
    legacy_keys = {
        "anidb_id",
        "imdb_id",
        "mal_id",
        "tmdb_show_id",
        "tmdb_movie_id",
        "tvdb_id",
        "tvdb_mappings",
        "tmdb_mappings",
    }
    return any(key in sample for key in legacy_keys)


def _extract_pab_legacy_payload(
    legacy: dict[str, Any],
    meta: MetaIndex,
) -> tuple[dict[str, dict[str, dict[str, set[str]]]], list[tuple[str, str]]]:
    """Extract explicit pairs and ID-only links from old PAB schema."""
    explicit: dict[str, dict[str, dict[str, set[str]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(set))
    )
    id_only_links: list[tuple[str, str]] = []

    for raw_anilist_id, payload in legacy.items():
        if not isinstance(payload, dict):
            continue
        source = Descriptor("anilist", str(raw_anilist_id), None)
        source_key = source.to_string()
        source_total = meta.episodes(source)

        tvdb_mappings = payload.get("tvdb_mappings")
        tvdb_id = payload.get("tvdb_id")
        if isinstance(tvdb_mappings, dict) and tvdb_id not in {None, ""}:
            _extract_legacy_show_mappings(
                source_key=source_key,
                source_total=source_total,
                mappings=tvdb_mappings,
                provider="tvdb_show",
                target_id=str(tvdb_id),
                meta=meta,
                explicit=explicit,
            )

        tmdb_mappings = payload.get("tmdb_mappings")
        tmdb_show_id = payload.get("tmdb_show_id")
        if isinstance(tmdb_mappings, dict) and tmdb_show_id not in {None, ""}:
            _extract_legacy_show_mappings(
                source_key=source_key,
                source_total=source_total,
                mappings=tmdb_mappings,
                provider="tmdb_show",
                target_id=str(tmdb_show_id),
                meta=meta,
                explicit=explicit,
            )

        # ID mappings requiring inferred episode ranges.
        anidb_id = payload.get("anidb_id")
        if anidb_id not in {None, ""}:
            target = Descriptor("anidb", str(anidb_id), "R")
            id_only_links.append((source_key, target.to_string()))

        mal_values = payload.get("mal_id")
        if mal_values is None:
            mal_values = []
        elif not isinstance(mal_values, list):
            mal_values = [mal_values]
        for mal_id in mal_values:
            if mal_id in {None, ""}:
                continue
            target = Descriptor("mal", str(mal_id), None)
            id_only_links.append((source_key, target.to_string()))

        # Infer imdb namespace from source shape.
        imdb_provider = "imdb_movie" if source_total == 1 else "imdb_show"
        tmdb_movie_value = payload.get("tmdb_movie_id")
        tmdb_show_value = payload.get("tmdb_show_id")
        if source_total == 1 or (
            tmdb_movie_value is not None
            and tmdb_movie_value != ""
            and (tmdb_show_value is None or tmdb_show_value == "")
        ):
            imdb_provider = "imdb_movie"
        elif isinstance(payload.get("tvdb_mappings"), dict) or isinstance(
            payload.get("tmdb_mappings"), dict
        ):
            imdb_provider = "imdb_show"

        imdb_values = payload.get("imdb_id")
        if imdb_values is None:
            imdb_values = []
        elif not isinstance(imdb_values, list):
            imdb_values = [imdb_values]
        for imdb_id in imdb_values:
            if imdb_id in {None, ""}:
                continue
            target = Descriptor(imdb_provider, str(imdb_id), None)
            id_only_links.append((source_key, target.to_string()))

        # Show/movie ids when explicit mappings are missing.
        if tvdb_id not in {None, ""} and not isinstance(tvdb_mappings, dict):
            tvdb_id_str = str(tvdb_id)
            scope = _best_show_scope("tvdb_show", tvdb_id_str, source_total, meta)
            target = Descriptor("tvdb_show", tvdb_id_str, scope or "s1")
            id_only_links.append((source_key, target.to_string()))

        if tmdb_show_id not in {None, ""} and not isinstance(tmdb_mappings, dict):
            tmdb_show_id_str = str(tmdb_show_id)
            scope = _best_show_scope("tmdb_show", tmdb_show_id_str, source_total, meta)
            target = Descriptor("tmdb_show", tmdb_show_id_str, scope or "s1")
            id_only_links.append((source_key, target.to_string()))

        tmdb_movie_values = payload.get("tmdb_movie_id")
        if tmdb_movie_values is None:
            tmdb_movie_values = []
        elif not isinstance(tmdb_movie_values, list):
            tmdb_movie_values = [tmdb_movie_values]
        for tmdb_movie_id in tmdb_movie_values:
            if tmdb_movie_id in {None, ""}:
                continue
            target = Descriptor("tmdb_movie", str(tmdb_movie_id), None)
            id_only_links.append((source_key, target.to_string()))

    return explicit, id_only_links


def _extract_descriptor_payload(
    legacy: dict[str, Any],
) -> tuple[dict[str, dict[str, dict[str, set[str]]]], list[tuple[str, str]]]:
    """Extract source->target->range mappings from descriptor-shaped payload."""
    explicit: dict[str, dict[str, dict[str, set[str]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(set))
    )
    id_only_links: list[tuple[str, str]] = []

    top_items = (
        legacy.items()
        if any(DESCRIPTOR_RE.match(k) for k in legacy)
        else ((f"anilist:{k}", v) for k, v in legacy.items() if str(k).isdigit())
    )

    for raw_source, raw_targets in top_items:
        if not isinstance(raw_targets, dict):
            continue
        source_descriptor = _parse_descriptor(str(raw_source))
        if source_descriptor is None:
            continue
        source_descriptor = _normalize_source_descriptor(source_descriptor)
        source_key = source_descriptor.to_string()

        for raw_target, raw_ranges in raw_targets.items():
            if str(raw_target).startswith("$"):
                continue
            target_descriptor = _parse_descriptor(str(raw_target))
            if target_descriptor is None:
                continue
            target_key = target_descriptor.to_string()

            if not isinstance(raw_ranges, dict) or not raw_ranges:
                id_only_links.append((source_key, target_key))
                continue

            range_count = 0
            for raw_src_range, raw_tgt_range in raw_ranges.items():
                if str(raw_src_range).startswith("$"):
                    continue
                if raw_tgt_range is None:
                    continue
                source_spec = str(raw_src_range).strip()
                target_spec = str(raw_tgt_range).strip()
                if not source_spec or not target_spec:
                    continue

                # Source-side comma ranges are unsupported in the new schema.
                for part in [x.strip() for x in source_spec.split(",") if x.strip()]:
                    explicit[source_key][target_key][part].add(target_spec)
                    range_count += 1

            if range_count == 0:
                id_only_links.append((source_key, target_key))

    return explicit, id_only_links


def _migrate(
    legacy_payload: dict[str, Any],
    meta: MetaIndex,
    *,
    schema_version: str,
) -> tuple[CommentedMap, Stats]:
    """Convert legacy payload into edit-map YAML structure."""
    stats = Stats()
    if _looks_like_pab_legacy(legacy_payload):
        explicit, id_only_links = _extract_pab_legacy_payload(legacy_payload, meta)
    else:
        explicit, id_only_links = _extract_descriptor_payload(legacy_payload)

    candidates: list[MappingCandidate] = []

    # Resolve explicit range pairs.
    for source_key, targets in explicit.items():
        source_descriptor_raw = _parse_descriptor(source_key)
        if source_descriptor_raw is None:
            continue
        source_descriptor = _normalize_source_descriptor(source_descriptor_raw)
        normalized_source_key = source_descriptor.to_string()

        provider_start_points: dict[str, list[int]] = defaultdict(list)
        for target_key, source_ranges in targets.items():
            target_descriptor_raw = _parse_descriptor(target_key)
            if target_descriptor_raw is None:
                continue
            target_descriptor = _normalize_target_descriptor(
                target_descriptor_raw,
                source_descriptor,
                meta,
            )
            for source_range in source_ranges:
                bounds = _bounds_for_overlap(source_range)
                if bounds is None:
                    continue
                provider_start_points[target_descriptor.provider].append(bounds[0])

        next_start_cache: dict[tuple[str, str], int | None] = {}
        for provider, starts in provider_start_points.items():
            ordered = sorted(set(starts))
            for index, value in enumerate(ordered):
                next_start_cache[(provider, str(value))] = (
                    ordered[index + 1] if index + 1 < len(ordered) else None
                )

        for raw_target_key, source_ranges in targets.items():
            target_descriptor_raw = _parse_descriptor(raw_target_key)
            if target_descriptor_raw is None:
                continue
            target_descriptor = _normalize_target_descriptor(
                target_descriptor_raw,
                source_descriptor,
                meta,
            )
            normalized_target_key = target_descriptor.to_string()

            for source_range, target_specs in source_ranges.items():
                merged_target_spec = _merge_target_specs(target_specs)
                if merged_target_spec is None:
                    stats.invalid_pairs_dropped += 1
                    continue

                bounds = _bounds_for_overlap(source_range)
                if bounds is None:
                    stats.invalid_pairs_dropped += 1
                    continue
                next_start = next_start_cache.get(
                    (target_descriptor.provider, str(bounds[0]))
                )

                resolved_source, resolved_target, was_ambiguous = (
                    _resolve_ambiguous_pair(
                        source_descriptor,
                        target_descriptor,
                        source_range,
                        merged_target_spec,
                        meta,
                        next_start_hint=next_start,
                    )
                )
                resolved_source_seg = _parse_segment(resolved_source)
                resolved_target_segs = _parse_target_spec(resolved_target) or []

                if was_ambiguous:
                    if (
                        resolved_source_seg is None
                        or resolved_source_seg.open_ended
                        or any(seg.open_ended for seg in resolved_target_segs)
                    ):
                        stats.unresolved_open_ranges += 1
                    else:
                        stats.resolved_open_ranges += 1

                candidates.append(
                    MappingCandidate(
                        source=normalized_source_key,
                        target=normalized_target_key,
                        target_provider=target_descriptor.provider,
                        source_range=resolved_source,
                        target_range=resolved_target,
                        priority=1 if was_ambiguous else 0,
                    )
                )
                stats.parsed_explicit_pairs += 1

    # Infer mappings for ID-only relations.
    seen_id_only: set[tuple[str, str]] = set()
    for source_key, target_key in id_only_links:
        pair = (source_key, target_key)
        if pair in seen_id_only:
            continue
        seen_id_only.add(pair)
        stats.parsed_id_only_links += 1

        source_descriptor_raw = _parse_descriptor(source_key)
        target_descriptor_raw = _parse_descriptor(target_key)
        if source_descriptor_raw is None or target_descriptor_raw is None:
            stats.unresolved_id_only_links += 1
            continue

        source_descriptor = _normalize_source_descriptor(source_descriptor_raw)
        target_descriptor = _normalize_target_descriptor(
            target_descriptor_raw,
            source_descriptor,
            meta,
        )
        inferred = _infer_id_only_pair(source_descriptor, target_descriptor, meta)
        if inferred is None:
            stats.unresolved_id_only_links += 1
            continue

        source_range, target_range = inferred
        candidates.append(
            MappingCandidate(
                source=source_descriptor.to_string(),
                target=target_descriptor.to_string(),
                target_provider=target_descriptor.provider,
                source_range=source_range,
                target_range=target_range,
                priority=2,
            )
        )
        stats.inferred_id_only_pairs += 1

    # Remove overlaps within same source + target provider namespace.
    grouped: dict[tuple[str, str], list[MappingCandidate]] = defaultdict(list)
    for item in candidates:
        grouped[(item.source, item.target_provider)].append(item)

    accepted: list[MappingCandidate] = []
    for (_source, _provider), entries in grouped.items():
        entries_sorted = sorted(
            entries,
            key=lambda entry: (
                entry.priority,
                _sort_source_range_key(entry.source_range),
                provider_scope_sort_key(entry.target),
                entry.target_range,
            ),
        )
        accepted_bounds: list[tuple[int, int | None]] = []

        for entry in entries_sorted:
            bounds = _bounds_for_overlap(entry.source_range)
            if bounds is None:
                stats.invalid_pairs_dropped += 1
                continue
            if any(_overlap(bounds, existing) for existing in accepted_bounds):
                stats.overlap_dropped_pairs += 1
                continue
            accepted_bounds.append(bounds)
            accepted.append(entry)

    # Build deterministic nested payload.
    nested: dict[str, dict[str, dict[str, str]]] = defaultdict(
        lambda: defaultdict(dict)
    )
    for entry in accepted:
        nested[entry.source][entry.target][entry.source_range] = entry.target_range

    ordered_root = CommentedMap()
    ordered_root["$schema"] = CommentedMap(
        {"version": DoubleQuotedScalarString(schema_version)}
    )

    for source_key in sorted(nested.keys(), key=provider_scope_sort_key):
        ordered_targets = CommentedMap()
        target_keys = sorted(nested[source_key], key=provider_scope_sort_key)
        for target_key in target_keys:
            range_map = nested[source_key][target_key]
            ordered_ranges = CommentedMap()
            for src_range in sorted(range_map.keys(), key=_sort_source_range_key):
                ordered_ranges[DoubleQuotedScalarString(src_range)] = (
                    DoubleQuotedScalarString(range_map[src_range])
                )
            ordered_targets[target_key] = ordered_ranges
        ordered_root[source_key] = ordered_targets

    return ordered_root, stats


def _schema_version() -> str:
    """Return package version to include in `$schema.version`."""
    try:
        return importlib.metadata.version("anibridge-mappings")
    except importlib.metadata.PackageNotFoundError:
        return "3.0.0"


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Build mappings.edits.yaml from legacy mappings.json, resolving "
            "ambiguous ranges with local metadata."
        )
    )
    parser.add_argument(
        "--legacy",
        type=Path,
        required=True,
        help="Path to the legacy mappings.json file.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("mappings.edits.yaml"),
        help="Output YAML path (default: mappings.edits.yaml).",
    )
    parser.add_argument(
        "--meta-dir",
        type=Path,
        default=Path("data/meta"),
        help="Metadata cache directory (default: data/meta).",
    )
    return parser.parse_args()


def main() -> int:
    """Run legacy migration and emit `mappings.edits.yaml`."""
    args = _parse_args()

    if not args.legacy.exists():
        raise SystemExit(f"Legacy file not found: {args.legacy}")

    try:
        legacy_payload = json.loads(args.legacy.read_text())
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON in legacy file {args.legacy}: {exc}") from exc
    if not isinstance(legacy_payload, dict):
        raise SystemExit("Legacy mappings payload must be a JSON object.")

    # Remove known metadata-like envelope keys when present.
    cleaned = {
        k: v
        for k, v in legacy_payload.items()
        if k not in {"$meta", "$schema", "meta", "schema"}
    }
    if "mappings" in cleaned and isinstance(cleaned["mappings"], dict):
        cleaned = cleaned["mappings"]

    meta = MetaIndex(args.meta_dir)
    migrated, stats = _migrate(cleaned, meta, schema_version=_schema_version())

    yaml = YAML(typ="rt")
    yaml.preserve_quotes = True
    yaml.indent(mapping=2, sequence=4, offset=2)
    with args.out.open("w") as fh:
        yaml.dump(migrated, fh)

    print(f"Wrote: {args.out}")
    print(f"Parsed explicit pairs: {stats.parsed_explicit_pairs}")
    print(f"Parsed ID-only links: {stats.parsed_id_only_links}")
    print(f"Inferred ID-only pairs: {stats.inferred_id_only_pairs}")
    print(f"Unresolved ID-only links: {stats.unresolved_id_only_links}")
    print(f"Resolved open ranges: {stats.resolved_open_ranges}")
    print(f"Unresolved open ranges: {stats.unresolved_open_ranges}")
    print(f"Dropped overlaps: {stats.overlap_dropped_pairs}")
    print(f"Dropped invalid pairs: {stats.invalid_pairs_dropped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
