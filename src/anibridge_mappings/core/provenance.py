"""Compact provenance serialization helpers."""

import importlib.metadata
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import orjson

from anibridge_mappings.core.graph import EpisodeMappingGraph, ProvenanceEvent
from anibridge_mappings.utils.mapping import format_descriptor


def _normalize_timestamp(value: datetime | None) -> str:
    """Normalize a datetime into a UTC ISO-8601 string ending with Z."""
    moment = value or datetime.now(tz=UTC)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    iso = moment.astimezone(UTC).replace(microsecond=0).isoformat()
    return iso.replace("+00:00", "Z") if iso.endswith("+00:00") else iso


def _event_payload(
    event: ProvenanceEvent,
    *,
    source_range: str,
    target_range: str,
    include_details: bool,
) -> dict[str, Any]:
    """Serialize a single oriented event into a JSON-ready payload."""
    payload: dict[str, Any] = {
        "seq": event.seq,
        "action": event.action,
        "stage": event.stage,
        "effective": event.effective,
        "source_range": source_range,
        "target_range": target_range,
    }
    if event.actor is not None:
        payload["actor"] = event.actor
    if event.reason is not None:
        payload["reason"] = event.reason
    contributor = _event_contributor(event)
    if contributor is not None:
        payload["contributor"] = contributor
    if include_details and event.details:
        payload["details"] = event.details
    return payload


def _event_contributor(event: ProvenanceEvent) -> str | None:
    """Return a normalized contributor label for an event, if available."""
    if event.details:
        contributor = event.details.get("contributor")
        if isinstance(contributor, str) and contributor.strip():
            return contributor.strip()

    if event.actor and event.stage.startswith("Source ingestion"):
        marker = "Provider source:"
        if marker in event.actor:
            _, _, raw = event.actor.partition(marker)
            candidate = raw.strip()
            if candidate:
                return candidate
    return None


def _active_ranges(events: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Compute current active range pairs by replaying effective events."""
    active: set[tuple[str, str]] = set()
    for event in events:
        if not event.get("effective"):
            continue
        pair = (str(event["source_range"]), str(event["target_range"]))
        if event.get("action") == "add":
            active.add(pair)
        elif event.get("action") == "remove":
            active.discard(pair)

    return [
        {"source_range": source_range, "target_range": target_range}
        for source_range, target_range in sorted(active)
    ]


def _mapping_contributors(events: list[dict[str, Any]]) -> list[str]:
    """Return all contributors that emitted events for a mapping pair."""
    contributors = {
        contributor
        for event in events
        if isinstance((contributor := event.get("contributor")), str) and contributor
    }
    return sorted(contributors)


def _active_mapping_contributors(events: list[dict[str, Any]]) -> list[str]:
    """Return contributors with at least one currently active range pair."""
    active_pairs: dict[tuple[str, str], str] = {}
    for event in events:
        if not event.get("effective"):
            continue
        contributor = event.get("contributor")
        source_range = str(event["source_range"])
        target_range = str(event["target_range"])
        pair = (source_range, target_range)
        if event.get("action") == "add":
            if isinstance(contributor, str) and contributor:
                active_pairs[pair] = contributor
        elif event.get("action") == "remove":
            active_pairs.pop(pair, None)

    return sorted(set(active_pairs.values()))


class _StringInterner:
    """Intern repeated strings to compact integer indices."""

    __slots__ = ("_index", "values")

    def __init__(self) -> None:
        self.values: list[str] = []
        self._index: dict[str, int] = {}

    def intern(self, value: str | None) -> int:
        if value is None or value == "":
            return -1
        existing = self._index.get(value)
        if existing is not None:
            return existing
        index = len(self.values)
        self.values.append(value)
        self._index[value] = index
        return index

    def __len__(self) -> int:
        return len(self.values)


def build_provenance_payload(
    episode_graph: EpisodeMappingGraph,
    *,
    schema_version: str | None = None,
    generated_on: datetime | None = None,
    include_details: bool = False,
) -> dict[str, Any]:
    """Build a compact provenance payload for browser inspection."""
    if schema_version is None:
        schema_version = importlib.metadata.version("anibridge-mappings")

    pair_events: dict[tuple[str, str], list[dict[str, Any]]] = {}

    for left_node, right_node, events in episode_graph.provenance_items():
        left_provider, left_id, left_scope, left_range = left_node
        right_provider, right_id, right_scope, right_range = right_node
        left_desc = format_descriptor(left_provider, left_id, left_scope)
        right_desc = format_descriptor(right_provider, right_id, right_scope)
        a, b = sorted((left_desc, right_desc))
        canonical_key = (a, b)

        # Orient ranges so source_range belongs to descriptor `a`
        flip = left_desc != a

        for event in sorted(events, key=lambda item: item.seq):
            payload = _event_payload(
                event,
                source_range=right_range if flip else left_range,
                target_range=left_range if flip else right_range,
                include_details=include_details,
            )
            pair_events.setdefault(canonical_key, []).append(payload)

    descriptors = _StringInterner()
    actions = _StringInterner()
    stages = _StringInterner()
    actors = _StringInterner()
    reasons = _StringInterner()
    ranges: list[dict[str, str]] = []
    range_index_map: dict[tuple[str, str], int] = {}

    mappings: list[dict[str, Any]] = []
    present_count = 0
    event_count = 0

    for canonical_key in sorted(pair_events):
        left_desc, right_desc = canonical_key
        events = sorted(
            pair_events[canonical_key],
            key=lambda item: item["seq"],
        )
        active_ranges = _active_ranges(events)
        present = bool(active_ranges)
        if present:
            present_count += 2
        event_count += len(events) * 2

        # Forward direction (left to right)
        fwd_compact: list[dict[str, Any]] = []
        for event in events:
            range_key = (str(event["source_range"]), str(event["target_range"]))
            range_index = range_index_map.get(range_key)
            if range_index is None:
                range_index = len(ranges)
                ranges.append({"s": range_key[0], "t": range_key[1]})
                range_index_map[range_key] = range_index
            fwd_compact.append(
                {
                    "a": actions.intern(event.get("action")),
                    "s": stages.intern(event.get("stage")),
                    "ac": actors.intern(event.get("actor")),
                    "rs": reasons.intern(event.get("reason")),
                    "r": range_index,
                    "e": bool(event.get("effective")),
                }
            )

        # Reverse direction (ranges swapped)
        rev_compact: list[dict[str, Any]] = []
        for event in events:
            rev_key = (str(event["target_range"]), str(event["source_range"]))
            rev_index = range_index_map.get(rev_key)
            if rev_index is None:
                rev_index = len(ranges)
                ranges.append({"s": rev_key[0], "t": rev_key[1]})
                range_index_map[rev_key] = rev_index
            rev_compact.append(
                {
                    "a": actions.intern(event.get("action")),
                    "s": stages.intern(event.get("stage")),
                    "ac": actors.intern(event.get("actor")),
                    "rs": reasons.intern(event.get("reason")),
                    "r": rev_index,
                    "e": bool(event.get("effective")),
                }
            )

        left_idx = descriptors.intern(left_desc)
        right_idx = descriptors.intern(right_desc)
        mappings.append(
            {
                "s": left_idx,
                "t": right_idx,
                "p": present,
                "n": len(fwd_compact),
                "ev": fwd_compact,
            }
        )
        mappings.append(
            {
                "s": right_idx,
                "t": left_idx,
                "p": present,
                "n": len(rev_compact),
                "ev": rev_compact,
            }
        )

    mapping_count = len(mappings)
    summary = {
        "descriptors": len(descriptors),
        "mappings": mapping_count,
        "present_mappings": present_count,
        "missing_mappings": mapping_count - present_count,
        "events": event_count,
        "ranges": len(ranges),
        "files": 1,
    }

    return {
        "$meta": {
            "format": "anibridge.provenance.v2",
            "schema_version": schema_version,
            "generated_on": _normalize_timestamp(generated_on),
            "summary": summary,
        },
        "dict": {
            "descriptors": descriptors.values,
            "actions": actions.values,
            "stages": stages.values,
            "actors": actors.values,
            "reasons": reasons.values,
            "ranges": ranges,
        },
        "mappings": mappings,
    }


def validate_provenance_payload(payload: dict[str, Any]) -> None:
    """Validate payload integrity for compact provenance data."""

    def _require(value: Any, expected_type: type, label: str) -> Any:
        if not isinstance(value, expected_type):
            raise ValueError(
                f"Provenance payload {label} must be a {expected_type.__name__}."
            )
        return value

    meta = _require(payload.get("$meta"), dict, "$meta")
    payload_dict = _require(payload.get("dict"), dict, "dict")
    mappings = _require(payload.get("mappings"), list, "mappings")
    summary = _require(meta.get("summary"), dict, "summary")

    dict_lists = {}
    for key in ("descriptors", "actions", "stages", "actors", "reasons", "ranges"):
        dict_lists[key] = _require(payload_dict.get(key), list, f"dict.{key}")

    descriptors = dict_lists["descriptors"]
    actions = dict_lists["actions"]
    ranges = dict_lists["ranges"]

    expected = {
        k: int(summary.get(k, -1))
        for k in (
            "descriptors",
            "mappings",
            "present_mappings",
            "missing_mappings",
            "events",
            "ranges",
        )
    }

    actual_present = 0
    actual_events = 0

    for mapping in mappings:
        _require(mapping, dict, "mapping entry")
        source_index = int(mapping.get("s", -1))
        target_index = int(mapping.get("t", -1))
        if source_index < 0 or source_index >= len(descriptors):
            raise ValueError(f"Mapping source index out of bounds: {source_index}")
        if target_index < 0 or target_index >= len(descriptors):
            raise ValueError(f"Mapping target index out of bounds: {target_index}")

        events = _require(mapping.get("ev"), list, "mapping events")
        if int(mapping.get("n", -1)) != len(events):
            raise ValueError(
                f"Mapping event count mismatch: {mapping.get('n')} != {len(events)}"
            )

        actual_events += len(events)
        active_ranges: set[int] = set()

        for event in events:
            _require(event, dict, "event entry")
            for key, values, allow_missing in (
                ("a", dict_lists["actions"], False),
                ("s", dict_lists["stages"], False),
                ("ac", dict_lists["actors"], True),
                ("rs", dict_lists["reasons"], True),
            ):
                event_index = int(event.get(key, -1))
                if allow_missing and event_index == -1:
                    continue
                if event_index < 0 or event_index >= len(values):
                    raise ValueError(
                        f"Event index out of bounds for {key}: {event_index}"
                    )

            range_index = int(event.get("r", -1))
            if range_index < 0 or range_index >= len(ranges):
                raise ValueError(f"Event range index out of bounds: {range_index}")

            if bool(event.get("e")):
                action = actions[int(event["a"])]
                if action == "add":
                    active_ranges.add(range_index)
                elif action == "remove":
                    active_ranges.discard(range_index)

        present = bool(active_ranges)
        if bool(mapping.get("p")) != present:
            raise ValueError(
                f"Mapping presence mismatch: {mapping.get('p')} != {present}"
            )
        if present:
            actual_present += 1

    actual_mappings = len(mappings)
    checks = {
        "descriptors": (expected["descriptors"], len(descriptors)),
        "mappings": (expected["mappings"], actual_mappings),
        "present_mappings": (expected["present_mappings"], actual_present),
        "missing_mappings": (
            expected["missing_mappings"],
            actual_mappings - actual_present,
        ),
        "events": (expected["events"], actual_events),
        "ranges": (expected["ranges"], len(ranges)),
    }
    for label, (exp, actual) in checks.items():
        if exp != actual:
            raise ValueError(f"Manifest {label} mismatch: {exp} != {actual}")


def write_provenance_payload(path: Path, payload: dict[str, Any]) -> None:
    """Write a compact provenance payload into a zip file."""
    path.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(
        path,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=9,
    ) as archive:
        archive.writestr(
            "provenance.json",
            orjson.dumps(payload),
        )
