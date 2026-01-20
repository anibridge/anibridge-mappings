"""Provenance serialization helpers."""

import importlib.metadata
from datetime import UTC, datetime
from typing import Any

from anibridge_mappings.core.graph import EpisodeMappingGraph, ProvenanceEvent


def _normalize_timestamp(value: datetime | None) -> str:
    """Normalize a datetime to an ISO 8601 UTC string."""
    if value is None:
        value = datetime.now(tz=UTC)
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _descriptor(provider: str, entry_id: str, scope: str | None) -> str:
    """Build a unique descriptor string for a given ID with optional scope."""
    if scope is None or scope == "":
        return f"{provider}:{entry_id}"
    return f"{provider}:{entry_id}:{scope}"


def _event_payload(event: ProvenanceEvent) -> dict[str, Any]:
    """Serialize a single provenance event into a JSON-ready payload."""
    payload: dict[str, Any] = {
        "seq": event.seq,
        "action": event.action,
        "stage": event.stage,
        "effective": event.effective,
    }
    if event.actor is not None:
        payload["actor"] = event.actor
    if event.reason is not None:
        payload["reason"] = event.reason
    if event.details:
        payload["details"] = event.details
    return payload


def _range_state(ranges: set[tuple[str, str]]) -> dict[str, Any]:
    """Build a range state payload from a set of source/target range pairs."""
    return {
        "ranges": [
            {"source_range": src, "target_range": tgt} for src, tgt in sorted(ranges)
        ]
    }


def _index_value(value: str | None, index: dict[str, int], items: list[str]) -> int:
    """Get or assign an index for a given value in the provided index mapping."""
    if not value:
        return -1
    if value in index:
        return index[value]
    idx = len(items)
    items.append(value)
    index[value] = idx
    return idx


def build_provenance_payload(
    episode_graph: EpisodeMappingGraph,
    *,
    schema_version: str | None = None,
    generated_on: datetime | None = None,
    include_details: bool = False,
) -> dict[str, Any]:
    """Serialize mapping provenance into a JSON-ready payload.

    Args:
        episode_graph (EpisodeMappingGraph): Episode mapping graph with provenance.
        schema_version (str | None): Schema version string.
        generated_on (datetime | None): Timestamp for generation.
        include_details (bool): Whether to include event details in the output.

    Returns:
        dict[str, Any]: JSON-serializable provenance payload.
    """
    if schema_version is None:
        schema_version = importlib.metadata.version("anibridge-mappings")
    timestamp = _normalize_timestamp(generated_on)

    mapping_events: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for source, target, events in episode_graph.provenance_items():
        src_provider, src_id, src_scope, src_range = source
        tgt_provider, tgt_id, tgt_scope, tgt_range = target
        src_descriptor = _descriptor(src_provider, src_id, src_scope)
        tgt_descriptor = _descriptor(tgt_provider, tgt_id, tgt_scope)
        key = (src_descriptor, tgt_descriptor)
        for event in events:
            payload = _event_payload(event)
            payload["source_range"] = src_range
            payload["target_range"] = tgt_range
            mapping_events.setdefault(key, []).append(payload)

    descriptors: list[str] = []
    descriptor_index: dict[str, int] = {}
    actions: list[str] = []
    action_index: dict[str, int] = {}
    stages: list[str] = []
    stage_index: dict[str, int] = {}
    actors: list[str] = []
    actor_index: dict[str, int] = {}
    reasons: list[str] = []
    reason_index: dict[str, int] = {}
    ranges: list[tuple[str, str]] = []
    range_index: dict[tuple[str, str], int] = {}

    mappings: list[dict[str, Any]] = []
    present_count = 0

    for (src_descriptor, tgt_descriptor), events in sorted(
        mapping_events.items(), key=lambda item: (item[0][0], item[0][1])
    ):
        events.sort(key=lambda event: event["seq"])
        src_idx = _index_value(src_descriptor, descriptor_index, descriptors)
        tgt_idx = _index_value(tgt_descriptor, descriptor_index, descriptors)

        current_ranges: set[int] = set()
        compact_events: list[dict[str, Any]] = []
        for event in events:
            pair = (event["source_range"], event["target_range"])
            if pair in range_index:
                range_idx = range_index[pair]
            else:
                range_idx = len(ranges)
                ranges.append(pair)
                range_index[pair] = range_idx

            if event.get("effective"):
                if event.get("action") == "add":
                    current_ranges.add(range_idx)
                elif event.get("action") == "remove":
                    current_ranges.discard(range_idx)

            compact_event: dict[str, Any] = {
                "seq": event["seq"],
                "a": _index_value(str(event.get("action")), action_index, actions),
                "s": _index_value(str(event.get("stage")), stage_index, stages),
                "e": 1 if event.get("effective") else 0,
                "r": range_idx,
                "ac": _index_value(event.get("actor"), actor_index, actors),
                "rs": _index_value(event.get("reason"), reason_index, reasons),
            }
            if include_details and event.get("details"):
                compact_event["d"] = event.get("details")
            compact_events.append(compact_event)

        present = len(current_ranges) > 0
        if present:
            present_count += 1

        mapping_entry = {
            "s": src_idx,
            "t": tgt_idx,
            "p": 1 if present else 0,
            "n": len(compact_events),
            "ev": compact_events,
        }
        mappings.append(mapping_entry)

    payload: dict[str, Any] = {
        "$meta": {
            "schema_version": schema_version,
            "generated_on": timestamp,
            "mappings": len(mappings),
            "present_mappings": present_count,
        },
        "dict": {
            "descriptors": descriptors,
            "actions": actions,
            "stages": stages,
            "actors": actors,
            "reasons": reasons,
            "ranges": [{"s": src, "t": tgt} for src, tgt in ranges],
        },
        "mappings": mappings,
    }
    return payload
