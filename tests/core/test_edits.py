from pathlib import Path

import pytest

from anibridge_mappings.core.edits import (
    EditError,
    _build_scope_index,
    _parse_descriptor,
    _parse_edit_descriptor,
    apply_edits,
    load_edits,
)
from anibridge_mappings.core.graph import EpisodeMappingGraph


def test_parse_descriptor_and_forced_marker() -> None:
    assert _parse_descriptor("anidb:1") == ("anidb", "1", "R")
    scope, forced, descriptor = _parse_edit_descriptor("^mal:2")
    assert scope == ("mal", "2", None)
    assert forced is True
    assert descriptor == "mal:2"


def test_apply_edits_replaces_edges() -> None:
    graph = EpisodeMappingGraph()
    src_old = ("anidb", "1", "R", "1")
    tgt_old = ("mal", "2", None, "1")
    graph.add_edge(src_old, tgt_old)

    edits = {
        "anidb:1": {
            "mal:2": {
                "1": "2",
                "2": "3",
            }
        }
    }

    edited_scopes, edited_pairs = apply_edits(graph, edits)

    assert ("anidb", "1", "R") in edited_scopes
    assert (("anidb", "1", "R"), ("mal", "2", None)) in edited_pairs
    assert not graph.has_edge(src_old, tgt_old)
    assert graph.has_edge(("anidb", "1", "R", "1"), ("mal", "2", None, "2"))
    assert graph.has_edge(("anidb", "1", "R", "2"), ("mal", "2", None, "3"))


def test_build_scope_index() -> None:
    graph = EpisodeMappingGraph()
    node = ("anidb", "1", "R", "1")
    graph.add_edge(node, ("mal", "2", None, "1"))

    index = _build_scope_index(graph)
    assert index[("anidb", "1", "R")] == {node}


def test_parse_descriptor_errors_and_forced_prefix_validation() -> None:
    with pytest.raises(EditError):
        _parse_descriptor("bad")

    with pytest.raises(EditError):
        _parse_edit_descriptor("^")


def test_load_edits_missing_file_returns_empty_dict(tmp_path: Path) -> None:
    assert load_edits(tmp_path / "missing.yaml") == {}


def test_apply_edits_ignores_invalid_target_payload() -> None:
    graph = EpisodeMappingGraph()
    graph.add_edge(("anidb", "1", "R", "1"), ("mal", "2", None, "1"))

    # Non-dict target section should be skipped, leaving graph unchanged.
    edited_scopes, edited_pairs = apply_edits(graph, {"anidb:1": "invalid"})

    assert edited_scopes == set()
    assert edited_pairs == set()
    assert graph.has_edge(("anidb", "1", "R", "1"), ("mal", "2", None, "1"))


def test_apply_edits_clears_edges_when_ranges_empty() -> None:
    graph = EpisodeMappingGraph()
    source = ("anidb", "1", "R", "1")
    target = ("mal", "2", None, "1")
    graph.add_edge(source, target)

    apply_edits(graph, {"anidb:1": {"mal:2": {}}})

    assert not graph.has_edge(source, target)


def test_load_edits_formats_commented_map_and_injects_schema(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr("importlib.metadata.version", lambda _name: "9.9.9")

    path = tmp_path / "mappings.edits.yaml"
    path.write_text(
        "mal:2:\n  anidb:1:R:\n    1: 1\n",
        encoding="utf-8",
    )

    payload = load_edits(path)

    assert "$schema" in payload
    assert payload["$schema"]["version"] == "9.9.9"
    text = path.read_text(encoding="utf-8")
    assert '"1"' in text


def test_load_edits_raises_edit_error_on_invalid_yaml(tmp_path: Path) -> None:
    path = tmp_path / "broken.yaml"
    path.write_text("foo: [", encoding="utf-8")

    with pytest.raises(EditError):
        load_edits(path)
