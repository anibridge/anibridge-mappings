"""Edit operations for overriding aggregated mappings."""

import importlib.metadata
import logging
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap
from ruamel.yaml.scalarstring import DoubleQuotedScalarString

from anibridge_mappings.core.graph import EpisodeMappingGraph, ProvenanceContext
from anibridge_mappings.utils.mapping import provider_scope_sort_key

log = logging.getLogger(__name__)

type Scope = tuple[str, str, str | None]  # (provider, id, scope)


class EditError(Exception):
    """Base exception for edit validation errors."""


class DuplicateTargetError(EditError):
    """Raised when a target is defined multiple times for same source."""


def load_edits(edits_file: Path | str) -> dict[str, Any]:
    """Load edits from a YAML file.

    Args:
        edits_file (Path | str): Path to the mappings.edits.yaml file.

    Returns:
        dict[str, Any]: Dictionary containing the edits configuration.

    Raises:
        EditError: If the file cannot be read or is invalid YAML.
    """
    path = Path(edits_file)
    if not path.exists():
        log.warning("Edits file not found: %s. Continuing without edits.", path)
        return {}

    try:
        yaml = YAML(typ="rt")
        yaml.preserve_quotes = True
        with path.open() as f:
            edits = yaml.load(f) or {}

        if isinstance(edits, CommentedMap):
            edits["$schema"] = {
                "version": DoubleQuotedScalarString(
                    importlib.metadata.version("anibridge-mappings")
                )
            }
            # Recursively normalize: sort keys, enforce quotes at depth 2, add spacers
            formatted = _normalize_node(edits, depth=0)
            with path.open("w") as f:
                yaml.dump(formatted, f)
            return formatted

        return edits
    except Exception as exc:
        raise EditError(f"Failed to load edits file '{path}': {exc}") from exc


def _normalize_node(node: CommentedMap, depth: int = 0) -> Any:
    """Recursively reconstructs CommentedMap to enforce sorting, quoting and spacing."""
    if not isinstance(node, (dict, CommentedMap)):
        # Only double-quote values if we are at depth 3 (Value of a Range)
        if depth == 3:
            return DoubleQuotedScalarString(str(node)) if node is not None else ""
        return str(node) if node is not None else ""

    new_map = CommentedMap()
    # Preserve comments from the original map
    if getattr(node, "ca", None):
        new_map.ca.comment = getattr(node.ca, "comment", None)
        new_map.ca.end = getattr(node.ca, "end", None)

    # Determine sort order
    keys = list(node.keys())
    if depth < 2:
        # Sort Source (depth 0) and Target (depth 1) descriptors
        keys.sort(key=provider_scope_sort_key)

    for k in keys:
        v = node[k]
        # Quoting Logic: Depth 2 (Range keys) gets quoted
        should_quote_key = depth == 2 and not (isinstance(k, str) and k.startswith("$"))
        new_key = DoubleQuotedScalarString(str(k)) if should_quote_key else str(k)

        # Recurse (Value will be depth + 1)
        new_val = _normalize_node(v, depth + 1)
        new_map[new_key] = new_val

        # Copy item-level comments
        if getattr(node, "ca", None) and k in node.ca.items:
            new_map.ca.items[new_key] = node.ca.items[k]

    return new_map


def apply_edits(
    episode_graph: EpisodeMappingGraph, edits: dict[str, Any]
) -> set[Scope]:
    """Applies edits directly to the episode graph.

    Args:
        episode_graph (EpisodeMappingGraph): The episode mapping graph to modify.
        edits (dict[str, Any]): Dictionary parsed from mappings.edits.yaml.

    Returns:
        set[Scope]: Set of source scopes that were modified.

    Raises:
        EditError: If any edit is invalid or conflicts.
    """
    edited_scopes: set[Scope] = set()
    scope_index = _build_scope_index(episode_graph)

    for src_desc, targets in edits.items():
        if src_desc.startswith("$"):
            continue
        source = _parse_descriptor(src_desc)
        processed_targets = set()

        for tgt_desc, config in targets.items():
            if tgt_desc.startswith("$"):
                continue
            if tgt_desc in processed_targets:
                raise DuplicateTargetError(
                    f"Duplicate target '{tgt_desc}' in '{src_desc}'"
                )
            processed_targets.add(tgt_desc)

            target = _parse_descriptor(tgt_desc)
            config = config or {}
            ranges = {k: v for k, v in config.items() if not k.startswith("$")}
            _apply_replace(
                episode_graph,
                source,
                target,
                ranges,
                scope_index,
                source_descriptor=src_desc,
                target_descriptor=tgt_desc,
            )

        edited_scopes.add(source)

    return edited_scopes


def _parse_descriptor(descriptor: str) -> Scope:
    """Parses 'provider:id[:scope]' string into a tuple."""
    parts = descriptor.split(":")
    if len(parts) == 2:
        provider, entry_id = parts
        if provider == "anidb":  # Special handling to normalize Anidb scope
            return provider, entry_id, "R"
        return provider, entry_id, None
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    raise EditError(
        "Invalid descriptor: "
        f"'{descriptor}'. Expected 'provider:id' or 'provider:id:scope'"
    )


def _build_scope_index(
    graph: EpisodeMappingGraph,
) -> dict[Scope, set[tuple[str, str, str | None, str]]]:
    """Index nodes by scope for faster edits application."""
    index: dict[Scope, set[tuple[str, str, str | None, str]]] = {}
    for node in graph.nodes():
        scope = (node[0], node[1], node[2])
        index.setdefault(scope, set()).add(node)
    return index


def _clear_source_target_ranges(
    graph: EpisodeMappingGraph,
    source_nodes: set[tuple[str, str, str | None, str]],
    target_nodes: set[tuple[str, str, str | None, str]],
    *,
    source_descriptor: str,
    target_descriptor: str,
) -> None:
    """Remove existing edges between source and target node sets."""
    if not source_nodes or not target_nodes:
        return

    for src_node in source_nodes:
        for neighbor in graph.neighbors(src_node):
            if neighbor in target_nodes:
                graph.remove_edge(
                    src_node,
                    neighbor,
                    provenance=ProvenanceContext(
                        stage="Manual overrides: clear prior links",
                        actor="Manual edit overrides (mappings.edits.yaml)",
                        reason="Cleared existing links before explicit replacement",
                        details={
                            "source_descriptor": source_descriptor,
                            "target_descriptor": target_descriptor,
                            "source_range": src_node[3],
                            "target_range": neighbor[3],
                        },
                    ),
                )


def _apply_replace(
    graph: EpisodeMappingGraph,
    source: Scope,
    target: Scope,
    ranges: dict[str, str],
    scope_index: dict[Scope, set[tuple[str, str, str | None, str]]],
    *,
    source_descriptor: str,
    target_descriptor: str,
) -> None:
    """Replaces mappings for a specific target scope."""
    source_nodes = scope_index.get(source, set())
    target_nodes = scope_index.get(target, set())
    _clear_source_target_ranges(
        graph,
        source_nodes,
        target_nodes,
        source_descriptor=source_descriptor,
        target_descriptor=target_descriptor,
    )

    if not ranges:
        return

    for src_rng, tgt_rng in ranges.items():
        src_node = (source[0], source[1], source[2], src_rng)
        tgt_node = (target[0], target[1], target[2], tgt_rng)
        graph.add_edge(
            src_node,
            tgt_node,
            bidirectional=True,
            provenance=ProvenanceContext(
                stage="Manual overrides: add replacement links",
                actor="Manual edit overrides (mappings.edits.yaml)",
                reason="Added explicit replacement mapping from the edits file",
                details={
                    "source_descriptor": source_descriptor,
                    "target_descriptor": target_descriptor,
                    "source_range": src_rng,
                    "target_range": tgt_rng,
                },
            ),
        )
        scope_index.setdefault(source, set()).add(src_node)
        scope_index.setdefault(target, set()).add(tgt_node)
