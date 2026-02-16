"""Remove redundant manual edit mappings from `mappings.edits.yaml`.

A mapping edit is considered redundant when the final emitted mapping for the same
`source -> target` descriptor pair is identical in outputs generated:
"""

import argparse
import json
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Remove redundant entries from mappings.edits.yaml by comparing "
            "mappings generated with vs without manual edits."
        )
    )
    parser.add_argument(
        "--edits",
        type=Path,
        default=Path("mappings.edits.yaml"),
        help="Path to mappings.edits.yaml (default: mappings.edits.yaml)",
    )
    parser.add_argument(
        "--with",
        dest="with_mappings",
        type=Path,
        default=Path("data/out.with/mappings.json"),
        help=(
            "Path to mappings.json generated with edits "
            "(default: data/out.with/mappings.json)"
        ),
    )
    parser.add_argument(
        "--without",
        dest="without_mappings",
        type=Path,
        default=Path("data/out.without/mappings.json"),
        help=(
            "Path to mappings.json generated without edits "
            "(default: data/out.without/mappings.json)"
        ),
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write pruned results back to --edits. Without this, run as dry-run.",
    )
    return parser.parse_args()


def _load_json(path: Path) -> dict[str, Any]:
    """Load a JSON object from disk."""
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return payload


def _load_edits(path: Path) -> CommentedMap:
    """Load the edits YAML as a round-trip map."""
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    yaml = YAML(typ="rt")
    yaml.preserve_quotes = True
    loaded = yaml.load(path.read_text(encoding="utf-8"))

    if loaded is None:
        return CommentedMap()
    if not isinstance(loaded, CommentedMap):
        raise ValueError(f"Expected mapping at root of {path}")
    return loaded


def _normalize_mapping(value: Any) -> tuple[tuple[str, str], ...] | None:
    """Normalize a source->target range mapping for robust equality comparison."""
    if value is None:
        return None
    if not isinstance(value, dict):
        return None

    normalized: list[tuple[str, str]] = []
    for src_range, tgt_range in value.items():
        normalized.append((str(src_range), str(tgt_range)))
    normalized.sort()
    return tuple(normalized)


def _non_meta_keys(mapping: dict[str, Any] | CommentedMap) -> list[str]:
    """Return all keys except metadata keys prefixed with '$'."""
    return [str(key) for key in mapping if not str(key).startswith("$")]


def prune_redundant_targets(
    edits: CommentedMap,
    with_payload: dict[str, Any],
    without_payload: dict[str, Any],
) -> tuple[int, int, list[tuple[str, str]]]:
    """Prune redundant source->target entries in-place.

    Returns:
        tuple[int, int, list[tuple[str, str]]]:
            - removed source->target entries count
            - removed source entries count
            - list of removed (source, target) descriptor pairs
    """
    removed_targets = 0
    removed_sources = 0
    removed_pairs: list[tuple[str, str]] = []

    source_keys = _non_meta_keys(edits)
    for source in source_keys:
        target_map = edits.get(source)
        if not isinstance(target_map, dict):
            continue

        target_keys = _non_meta_keys(target_map)
        for target in target_keys:
            with_ranges = _normalize_mapping(
                with_payload.get(source, {}).get(target)
                if isinstance(with_payload.get(source), dict)
                else None
            )
            without_ranges = _normalize_mapping(
                without_payload.get(source, {}).get(target)
                if isinstance(without_payload.get(source), dict)
                else None
            )

            if with_ranges == without_ranges:
                del target_map[target]
                removed_targets += 1
                removed_pairs.append((source, target))

        if not _non_meta_keys(target_map):
            del edits[source]
            removed_sources += 1

    return removed_targets, removed_sources, removed_pairs


def write_edits(path: Path, edits: CommentedMap) -> None:
    """Write the pruned edits file while preserving YAML style."""
    yaml = YAML(typ="rt")
    yaml.preserve_quotes = True
    with path.open("w", encoding="utf-8") as handle:
        yaml.dump(edits, handle)


def main() -> None:
    """Run the redundancy pruning CLI."""
    args = parse_args()

    with_payload = _load_json(args.with_mappings)
    without_payload = _load_json(args.without_mappings)
    edits = _load_edits(args.edits)

    removed_targets, removed_sources, removed_pairs = prune_redundant_targets(
        edits,
        with_payload,
        without_payload,
    )

    action = "Would remove" if not args.write else "Removed"
    print(f"{action} {removed_targets} redundant source->target edit entries.")
    print(f"{action} {removed_sources} now-empty source entries.")

    if removed_pairs:
        preview_limit = 20
        print(f"Sample redundant entries ({min(len(removed_pairs), preview_limit)}):")
        for source, target in removed_pairs[:preview_limit]:
            print(f"  - {source} -> {target}")
        if len(removed_pairs) > preview_limit:
            print(f"  ... and {len(removed_pairs) - preview_limit} more")

    if args.write:
        write_edits(args.edits, edits)
        print(f"Updated edits file: {args.edits}")
    else:
        print("Dry-run only. Re-run with --write to persist changes.")


if __name__ == "__main__":
    main()
