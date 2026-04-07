"""Find and remove no-op mapping edits from mappings.edits.yaml.

A no-op edit is one where the edges it specifies already exist identically in
the pre-edit episode graph (after source ingestion + inference, before
validation and transitive closure).

Usage:
    python scripts/find_noop_edits.py                    # report + auto-remove
    python scripts/find_noop_edits.py --dry-run          # report only
    python scripts/find_noop_edits.py --edits path.yaml  # custom edits file
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv
from ruamel.yaml import YAML

from anibridge_mappings.core.aggregator import default_aggregator
from anibridge_mappings.core.edits import _build_scope_index, _parse_edit_descriptor
from anibridge_mappings.core.graph import EpisodeMappingGraph, ProvenanceContext
from anibridge_mappings.core.inference import infer_episode_mappings

log = logging.getLogger(__name__)

type Scope = tuple[str, str, str | None]


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Find and remove no-op mapping edits.")
    parser.add_argument(
        "--edits",
        default="mappings.edits.yaml",
        help="Path to edits file (default: mappings.edits.yaml)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report no-op edits without modifying the file.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (default: INFO)",
    )
    return parser.parse_args()


async def build_pre_edit_graph() -> EpisodeMappingGraph:
    """Run the aggregation pipeline up to (and including) inference.

    Returns the episode graph in the state it would be in just before
    edits are applied. After source ingestion and inference, but before
    validation and transitive closure.
    """
    aggregator = default_aggregator()

    log.info("Preparing sources...")
    await aggregator._prepare_sources()

    log.info("Building ID graph...")
    id_graph = aggregator._build_id_graph()
    log.info("ID graph: %d nodes", id_graph.node_count())

    log.info("Collecting metadata...")
    meta_store = await aggregator._collect_metadata(id_graph)
    log.info("Metadata: %d entries", len(meta_store.items()))

    log.info("Building episode graph from sources...")
    episode_graph = aggregator._build_episode_graph(meta_store, id_graph)
    log.info("Episode graph: %d nodes (sources only)", len(episode_graph.nodes()))

    log.info("Running inference...")
    inferred_graph = infer_episode_mappings(meta_store, id_graph)
    if inferred_graph.node_count():
        episode_graph.add_graph(
            inferred_graph,
            provenance=ProvenanceContext(
                stage="Inference: metadata-driven episode alignment",
                actor="Inference engine: metadata alignment",
                reason="Inferred episode links via cross-source metadata alignment",
            ),
        )
        log.info(
            "Episode graph: %d nodes (after inference)", len(episode_graph.nodes())
        )

    return episode_graph


def _get_existing_edges(
    graph: EpisodeMappingGraph,
    source: Scope,
    target: Scope,
    scope_index: dict[Scope, set[tuple[str, str, str | None, str]]],
) -> set[tuple[str, str]]:
    """Collect all (source_range, target_range) edges between two scopes."""
    source_nodes = scope_index.get(source, set())
    target_nodes = scope_index.get(target, set())
    edges: set[tuple[str, str]] = set()
    for src_node in source_nodes:
        for neighbor in graph.neighbors(src_node):
            if neighbor in target_nodes:
                edges.add((src_node[3], neighbor[3]))
    return edges


def find_noop_edits(
    graph: EpisodeMappingGraph,
    edits_data: dict,
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """Identify no-op edits by comparing against the pre-edit graph.

    Returns:
        Tuple of (regular_noops, forced_noops) where each is a list of
        (source_descriptor, target_descriptor) pairs.
    """
    scope_index = _build_scope_index(graph)
    regular_noops: list[tuple[str, str]] = []
    forced_noops: list[tuple[str, str]] = []

    for src_desc, targets in edits_data.items():
        if isinstance(src_desc, str) and src_desc.startswith("$"):
            continue
        if not isinstance(targets, dict):
            continue

        source, source_forced, _source_norm = _parse_edit_descriptor(str(src_desc))

        for tgt_desc, config in targets.items():
            tgt_desc_str = str(tgt_desc)
            if tgt_desc_str.startswith("$"):
                continue

            target, target_forced, _target_norm = _parse_edit_descriptor(tgt_desc_str)
            forced = source_forced or target_forced

            config = config or {}
            edit_ranges: set[tuple[str, str]] = set()
            for k, v in config.items():
                k_str = str(k)
                if k_str.startswith("$"):
                    continue
                edit_ranges.add((k_str, str(v)))

            existing_edges = _get_existing_edges(graph, source, target, scope_index)

            if edit_ranges == existing_edges:
                if forced:
                    forced_noops.append((str(src_desc), tgt_desc_str))
                else:
                    regular_noops.append((str(src_desc), tgt_desc_str))

    return regular_noops, forced_noops


def remove_noop_edits(
    edits_data: dict,
    noops: list[tuple[str, str]],
) -> int:
    """Remove no-op target entries from the edits data structure in place."""
    # source_desc to set of target_descs to remove
    removals: dict[str, set[str]] = {}
    for src_desc, tgt_desc in noops:
        removals.setdefault(src_desc, set()).add(tgt_desc)

    removed = 0
    sources_to_delete: list[str] = []

    for src_desc, tgt_descs_to_remove in removals.items():
        # Find the matching key in edits_data (may differ in type due to YAML scalars)
        src_key = _find_key(edits_data, src_desc)
        if src_key is None:
            continue

        targets = edits_data[src_key]
        if not isinstance(targets, dict):
            continue

        for tgt_desc in tgt_descs_to_remove:
            tgt_key = _find_key(targets, tgt_desc)
            if tgt_key is not None:
                del targets[tgt_key]
                removed += 1

        # Check if all non-$ targets have been removed
        remaining = [k for k in targets if not str(k).startswith("$")]
        if not remaining:
            sources_to_delete.append(src_key)

    for src_key in sources_to_delete:
        del edits_data[src_key]

    return removed


def _find_key(mapping: dict, target: str) -> str | None:
    """Find a key in a dict matching by string value (handles YAML scalar types)."""
    for k in mapping:
        if str(k) == target:
            return k
    return None


def print_report(
    regular_noops: list[tuple[str, str]],
    forced_noops: list[tuple[str, str]],
    total_pairs: int,
    dry_run: bool,
) -> None:
    """Print a summary report of findings."""
    effective = total_pairs - len(regular_noops) - len(forced_noops)

    print(f"\n{'=' * 60}")
    print("No-Op Edit Detection Report")
    print(f"{'=' * 60}")
    print(f"Total source→target pairs scanned:  {total_pairs}")
    print(f"Effective edits (kept):              {effective}")
    print(f"Regular no-op edits:                 {len(regular_noops)}")
    print(f"Forced no-op edits (kept):           {len(forced_noops)}")
    print(f"{'=' * 60}")

    if regular_noops:
        action = "Would remove" if dry_run else "Removed"
        print(f"\n{action} {len(regular_noops)} regular no-op edit(s):")
        for src, tgt in sorted(regular_noops):
            print(f"  {src} → {tgt}")

    if forced_noops:
        print(f"\nKept {len(forced_noops)} forced no-op edit(s) (^ prefix):")
        for src, tgt in sorted(forced_noops):
            print(f"  {src} → {tgt}")

    if not regular_noops and not forced_noops:
        print("\nAll edits are effective — nothing to remove.")

    print()


def _count_pairs(edits_data: dict) -> int:
    """Count total source→target pairs in the edits data."""
    count = 0
    for src_desc, targets in edits_data.items():
        if isinstance(src_desc, str) and src_desc.startswith("$"):
            continue
        if not isinstance(targets, dict):
            continue
        for tgt_desc in targets:
            if not str(tgt_desc).startswith("$"):
                count += 1
    return count


async def main() -> None:
    """Entry point."""
    args = parse_args()
    load_dotenv()

    level = getattr(logging, args.log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    edits_path = Path(args.edits)
    if not edits_path.exists():
        print(f"error: edits file not found: {edits_path}", file=sys.stderr)
        sys.exit(1)

    yaml = YAML(typ="rt")
    yaml.preserve_quotes = True
    with edits_path.open() as f:
        edits_data = yaml.load(f) or {}

    total_pairs = _count_pairs(edits_data)
    if total_pairs == 0:
        print("No edits found in the file.")
        return

    log.info("Loaded %d source→target edit pairs from %s", total_pairs, edits_path)

    # Build the pre-edit episode graph (full pipeline minus edits/validation/transitive)
    episode_graph = await build_pre_edit_graph()

    # Detect no-ops
    log.info("Scanning edits for no-ops...")
    regular_noops, forced_noops = find_noop_edits(episode_graph, edits_data)

    # Report
    print_report(regular_noops, forced_noops, total_pairs, args.dry_run)

    # Remove + write back
    if regular_noops and not args.dry_run:
        removed = remove_noop_edits(edits_data, regular_noops)
        with edits_path.open("w") as f:
            yaml.dump(edits_data, f)
        log.info(
            "Removed %d no-op edit(s) and wrote cleaned file to %s",
            removed,
            edits_path,
        )
    elif regular_noops:
        log.info("Dry run — no changes written.")


if __name__ == "__main__":
    asyncio.run(main())
