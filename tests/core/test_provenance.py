import zipfile
from datetime import UTC, datetime

import orjson
import pytest

from anibridge_mappings.core.graph import EpisodeMappingGraph, ProvenanceContext
from anibridge_mappings.core.provenance import (
    _active_mapping_contributors,
    _active_ranges,
    _event_contributor,
    _event_payload,
    _mapping_contributors,
    build_provenance_payload,
    validate_provenance_payload,
    write_provenance_payload,
)
from anibridge_mappings.utils.mapping import format_descriptor


def _sample_graph() -> EpisodeMappingGraph:
    graph = EpisodeMappingGraph()
    a = ("anidb", "1", "R", "1-2")
    b = ("mal", "2", None, "1-2")
    graph.add_edge(
        a,
        b,
        provenance=ProvenanceContext(
            stage="Source ingestion: episode mappings",
            actor="Provider source: AnimeListsSource",
            details={"contributor": "anime_lists:AnimeListsSource"},
        ),
    )
    return graph


def test_descriptor_helpers() -> None:
    assert format_descriptor("anidb", "1", None) == "anidb:1"
    assert format_descriptor("anidb", "1", "R") == "anidb:1:R"


def test_event_helpers_extract_contributors() -> None:
    graph = _sample_graph()
    event = graph.provenance_items()[0][2][0]

    assert _event_contributor(event) == "anime_lists:AnimeListsSource"
    payload = _event_payload(
        event, source_range="1", target_range="1", include_details=True
    )
    assert payload["contributor"] == "anime_lists:AnimeListsSource"


def test_build_validate_and_write_provenance_payload(tmp_path) -> None:
    graph = _sample_graph()
    payload = build_provenance_payload(
        graph,
        schema_version="1.2.3",
        generated_on=datetime(2024, 1, 1, tzinfo=UTC),
        include_details=True,
    )

    validate_provenance_payload(payload)
    assert payload["$meta"]["summary"]["events"] >= 1
    assert payload["$meta"]["format"] == "anibridge.provenance.v2"
    assert payload["dict"]["descriptors"] == ["anidb:1:R", "mal:2"]
    assert len(payload["mappings"]) == 2

    output = tmp_path / "prov.zip"
    write_provenance_payload(output, payload)

    with zipfile.ZipFile(output) as archive:
        assert archive.namelist() == ["provenance.json"]
        compact = orjson.loads(archive.read("provenance.json"))
        assert compact["$meta"]["schema_version"] == "1.2.3"
        assert compact["mappings"][0]["p"] is True


def test_validate_payload_catches_mismatched_manifest() -> None:
    graph = _sample_graph()
    payload = build_provenance_payload(graph, schema_version="1.0.0")
    payload["$meta"]["summary"]["descriptors"] = 999

    with pytest.raises(ValueError, match="descriptors mismatch"):
        validate_provenance_payload(payload)


def test_active_range_and_contributor_helpers() -> None:
    events = [
        {
            "action": "add",
            "effective": True,
            "source_range": "1",
            "target_range": "1",
            "contributor": "a",
        },
        {
            "action": "remove",
            "effective": True,
            "source_range": "1",
            "target_range": "1",
            "contributor": "b",
        },
        {
            "action": "add",
            "effective": True,
            "source_range": "2",
            "target_range": "2",
            "contributor": "c",
        },
    ]

    assert _active_ranges(events) == [{"source_range": "2", "target_range": "2"}]
    assert _mapping_contributors(events) == ["a", "b", "c"]
    assert _active_mapping_contributors(events) == ["c"]
