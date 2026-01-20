"""CLI entrypoint for generating AniBridge mapping payloads."""

import argparse
import asyncio
import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from zstandard import ZstdCompressor

from anibridge_mappings.core.aggregator import (
    AggregationArtifacts,
    build_schema_payload,
    default_aggregator,
)
from anibridge_mappings.core.stats import build_stats

log = logging.getLogger("anibridge.cli")


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the mappings generator.

    Returns:
        argparse.Namespace: Parsed CLI arguments.
    """
    parser = argparse.ArgumentParser(
        description="Generate provider mappings in the v3 schema format."
    )
    parser.add_argument(
        "--out",
        default="data/out/",
        help=("Destination directory for the generated files (default: data/out/)"),
    )
    parser.add_argument(
        "--edits",
        default="mappings.edits.yaml",
        help="Path to edits file for overriding aggregated mappings "
        "(default: mappings.edits.yaml)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR). (default: INFO)",
    )
    parser.add_argument(
        "--compress",
        action="store_true",
        help=(
            "Also write minified and zstd-compressed variants alongside the main "
            "output (mappings.min.json and mappings.json.zst)."
        ),
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Write a stats.json file with aggregation summary metrics.",
    )
    return parser.parse_args()


def configure_logging(level: str) -> None:
    """Configure basic logging output using the desired severity level.

    Args:
        level (str): Logging level name.
    """
    value = getattr(logging, level.upper(), None)
    if not isinstance(value, int):
        raise ValueError(f"Invalid log level: {level}")
    logging.basicConfig(
        level=value,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


async def build_artifacts(
    edits_file: str,
) -> tuple[AggregationArtifacts, dict[str, Any]]:
    """Run the aggregation pipeline and return artifacts plus serialized payload.

    Args:
        schema_version (str): Version string for the schema metadata.
        edits_file (str): Path to the edits YAML file.

    Returns:
        tuple[AggregationArtifacts, dict[str, Any]]: Aggregation results and payload.
    """
    aggregator = default_aggregator()
    artifacts = await aggregator.run(edits_file=edits_file)
    payload = build_schema_payload(
        artifacts.episode_graph,
        generated_on=datetime.now(UTC),
    )
    return artifacts, payload


def write_payload(path: Path, payload: dict[str, Any], *, pretty: bool = True) -> None:
    """Persist the rendered payload to `path`.

    Args:
        path (Path): Destination path for the JSON payload.
        payload (dict[str, Any]): Serialized mapping payload.
        pretty (bool): Whether to pretty-print JSON with indentation.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if pretty:
        rendered = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    else:
        rendered = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    path.write_text(rendered, encoding="utf-8")


def write_zstd(path: Path, payload: dict[str, Any]) -> None:
    """Write a zstd-compressed JSON payload to `path`.

    Args:
        path (Path): Destination path for the compressed payload.
        payload (dict[str, Any]): Serialized mapping payload.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )
    compressor = ZstdCompressor()
    path.write_bytes(compressor.compress(data))


def main() -> None:
    """Entry point for the CLI application."""
    args = parse_args()
    try:
        configure_logging(args.log_level)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(2)

    try:
        artifacts, payload = asyncio.run(build_artifacts(args.edits))
    except KeyboardInterrupt:
        log.warning("Mapping generation interrupted")
        sys.exit(130)

    output_dir = Path(args.out)
    output_path = output_dir / "mappings.json"
    write_payload(output_path, payload, pretty=True)
    log.info(
        "Wrote %s with %d provider scopes",
        output_path,
        max(len(payload) - 1, 0),
    )

    if args.stats:
        stats_path = output_path.with_name("stats.json")
        stats_payload = build_stats(artifacts, payload)
        write_payload(stats_path, stats_payload, pretty=True)
        log.info("Wrote %s", stats_path)

    if args.compress:
        minified_path = output_path.with_name(
            f"{output_path.stem}.min{output_path.suffix}"
        )
        zstd_path = output_path.with_name(f"{output_path.name}.zst")
        write_payload(minified_path, payload, pretty=False)
        write_zstd(zstd_path, payload)
        log.info("Wrote %s", minified_path)
        log.info("Wrote %s", zstd_path)


if __name__ == "__main__":
    main()
