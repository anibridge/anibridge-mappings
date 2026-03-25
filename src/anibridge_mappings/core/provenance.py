"""Descriptor-sharded provenance serialization helpers."""

import hashlib
import importlib.metadata
import json
import re
import zipfile
from datetime import UTC, datetime
from pathlib import Path
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


def _descriptor_filename(descriptor: str) -> str:
    """Create a stable, filesystem-safe filename for a descriptor document."""
    normalized = descriptor.lower()
    slug = re.sub(r"[^a-z0-9._-]+", "_", normalized).strip("_")
    if not slug:
        slug = "descriptor"
    digest = hashlib.sha1(descriptor.encode("utf-8")).hexdigest()[:10]
    return f"{slug[:48]}-{digest}.json"


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
    if include_details and event.details:
        payload["details"] = event.details
    return payload


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


def build_provenance_payload(
    episode_graph: EpisodeMappingGraph,
    *,
    schema_version: str | None = None,
    generated_on: datetime | None = None,
    include_details: bool = False,
) -> dict[str, Any]:
    """Build descriptor-sharded provenance payloads.

    Returns:
        dict[str, Any]: Provenance payload with manifest, index, and descriptor files.
    """
    if schema_version is None:
        schema_version = importlib.metadata.version("anibridge-mappings")

    descriptor_events: dict[str, dict[str, list[dict[str, Any]]]] = {}

    for source, target, events in episode_graph.provenance_items():
        src_provider, src_id, src_scope, src_range = source
        tgt_provider, tgt_id, tgt_scope, tgt_range = target
        src_descriptor = _descriptor(src_provider, src_id, src_scope)
        tgt_descriptor = _descriptor(tgt_provider, tgt_id, tgt_scope)

        for event in sorted(events, key=lambda item: item.seq):
            src_to_tgt = _event_payload(
                event,
                source_range=src_range,
                target_range=tgt_range,
                include_details=include_details,
            )
            tgt_to_src = _event_payload(
                event,
                source_range=tgt_range,
                target_range=src_range,
                include_details=include_details,
            )

            descriptor_events.setdefault(src_descriptor, {}).setdefault(
                tgt_descriptor, []
            ).append(src_to_tgt)
            descriptor_events.setdefault(tgt_descriptor, {}).setdefault(
                src_descriptor, []
            ).append(tgt_to_src)

    descriptor_files: dict[str, dict[str, Any]] = {}
    descriptor_index: list[dict[str, Any]] = []

    for descriptor in sorted(descriptor_events):
        target_map = descriptor_events[descriptor]
        mappings: list[dict[str, Any]] = []
        present_mappings = 0
        event_count = 0

        for target_descriptor in sorted(target_map):
            events = sorted(target_map[target_descriptor], key=lambda item: item["seq"])
            active_ranges = _active_ranges(events)
            present = bool(active_ranges)
            if present:
                present_mappings += 1
            event_count += len(events)

            mappings.append(
                {
                    "target_descriptor": target_descriptor,
                    "event_count": len(events),
                    "present": present,
                    "active_ranges": active_ranges,
                    "events": events,
                }
            )

        filename = _descriptor_filename(descriptor)
        file_path = f"descriptors/{filename}"
        missing_mappings = len(mappings) - present_mappings

        descriptor_files[file_path] = {
            "descriptor": descriptor,
            "mapping_count": len(mappings),
            "present_mappings": present_mappings,
            "missing_mappings": missing_mappings,
            "event_count": event_count,
            "mappings": mappings,
        }

        descriptor_index.append(
            {
                "descriptor": descriptor,
                "file": file_path,
                "mapping_count": len(mappings),
                "present_mappings": present_mappings,
                "missing_mappings": missing_mappings,
                "event_count": event_count,
            }
        )

    descriptor_count = len(descriptor_index)
    mapping_count = sum(item["mapping_count"] for item in descriptor_index)
    present_count = sum(item["present_mappings"] for item in descriptor_index)
    missing_count = sum(item["missing_mappings"] for item in descriptor_index)
    event_count = sum(item["event_count"] for item in descriptor_index)

    manifest = {
        "format": "anibridge.provenance.v1",
        "schema_version": schema_version,
        "generated_on": _normalize_timestamp(generated_on),
        "entrypoints": {
            "index": "descriptor-index.json",
            "descriptors_dir": "descriptors/",
        },
        "summary": {
            "descriptors": descriptor_count,
            "mappings": mapping_count,
            "present_mappings": present_count,
            "missing_mappings": missing_count,
            "events": event_count,
            "files": descriptor_count + 2,
        },
    }

    return {
        "manifest": manifest,
        "descriptor_index": descriptor_index,
        "descriptor_files": descriptor_files,
    }


def validate_provenance_payload(payload: dict[str, Any]) -> None:
    """Validate payload integrity for descriptor-sharded provenance data."""
    manifest = payload.get("manifest")
    descriptor_index = payload.get("descriptor_index")
    descriptor_files = payload.get("descriptor_files")

    if not isinstance(manifest, dict):
        raise ValueError("Provenance payload missing manifest object.")
    if not isinstance(descriptor_index, list):
        raise ValueError("Provenance payload missing descriptor_index list.")
    if not isinstance(descriptor_files, dict):
        raise ValueError("Provenance payload missing descriptor_files object.")

    summary = manifest.get("summary")
    if not isinstance(summary, dict):
        raise ValueError("Provenance payload manifest missing summary object.")

    expected_descriptors = int(summary.get("descriptors", -1))
    expected_mappings = int(summary.get("mappings", -1))
    expected_present = int(summary.get("present_mappings", -1))
    expected_missing = int(summary.get("missing_mappings", -1))
    expected_events = int(summary.get("events", -1))

    actual_descriptors = len(descriptor_index)
    actual_mappings = 0
    actual_present = 0
    actual_missing = 0
    actual_events = 0

    seen_descriptors: set[str] = set()
    seen_files: set[str] = set()

    for item in descriptor_index:
        if not isinstance(item, dict):
            raise ValueError("Descriptor index entries must be objects.")

        descriptor = item.get("descriptor")
        file_path = item.get("file")
        if not isinstance(descriptor, str) or not descriptor:
            raise ValueError("Descriptor index entry has invalid descriptor.")
        if not isinstance(file_path, str) or not file_path:
            raise ValueError("Descriptor index entry has invalid file path.")
        if descriptor in seen_descriptors:
            raise ValueError(f"Duplicate descriptor index entry: {descriptor}")
        if file_path in seen_files:
            raise ValueError(f"Duplicate descriptor file entry: {file_path}")

        seen_descriptors.add(descriptor)
        seen_files.add(file_path)

        descriptor_doc = descriptor_files.get(file_path)
        if not isinstance(descriptor_doc, dict):
            raise ValueError(f"Descriptor file missing for index path: {file_path}")
        if descriptor_doc.get("descriptor") != descriptor:
            raise ValueError(
                f"Descriptor mismatch for {file_path}: "
                f"{descriptor_doc.get('descriptor')}"
            )

        mapping_count = int(descriptor_doc.get("mapping_count", 0))
        present_count = int(descriptor_doc.get("present_mappings", 0))
        missing_count = int(descriptor_doc.get("missing_mappings", 0))
        event_count = int(descriptor_doc.get("event_count", 0))
        mappings = descriptor_doc.get("mappings")

        if not isinstance(mappings, list):
            raise ValueError(f"Descriptor mappings must be a list: {file_path}")
        if mapping_count != len(mappings):
            raise ValueError(
                f"mapping_count mismatch in {file_path}: {mapping_count} != "
                f"{len(mappings)}"
            )
        if mapping_count != present_count + missing_count:
            raise ValueError(
                f"present/missing mismatch in {file_path}: {mapping_count} != "
                f"{present_count} + {missing_count}"
            )

        actual_mappings += mapping_count
        actual_present += present_count
        actual_missing += missing_count
        actual_events += event_count

    extra_files = set(descriptor_files) - seen_files
    if extra_files:
        extra_preview = ", ".join(sorted(extra_files)[:5])
        raise ValueError(f"Descriptor files missing from index: {extra_preview}")

    if expected_descriptors != actual_descriptors:
        raise ValueError(
            f"Manifest descriptors mismatch: {expected_descriptors} != "
            f"{actual_descriptors}"
        )
    if expected_mappings != actual_mappings:
        raise ValueError(
            f"Manifest mappings mismatch: {expected_mappings} != {actual_mappings}"
        )
    if expected_present != actual_present:
        raise ValueError(
            f"Manifest present mismatch: {expected_present} != {actual_present}"
        )
    if expected_missing != actual_missing:
        raise ValueError(
            f"Manifest missing mismatch: {expected_missing} != {actual_missing}"
        )
    if expected_events != actual_events:
        raise ValueError(
            f"Manifest events mismatch: {expected_events} != {actual_events}"
        )


def write_provenance_payload(path: Path, payload: dict[str, Any]) -> None:
    """Write descriptor-sharded provenance payloads into a zip file."""
    path.parent.mkdir(parents=True, exist_ok=True)

    manifest = payload["manifest"]
    descriptor_index = payload["descriptor_index"]
    descriptor_files = payload["descriptor_files"]

    with zipfile.ZipFile(
        path,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=9,
    ) as archive:
        archive.writestr(
            "manifest.json",
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        )
        archive.writestr(
            "descriptor-index.json",
            json.dumps(descriptor_index, ensure_ascii=False, indent=2) + "\n",
        )
        for descriptor_path in sorted(descriptor_files):
            archive.writestr(
                descriptor_path,
                json.dumps(
                    descriptor_files[descriptor_path],
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
            )
