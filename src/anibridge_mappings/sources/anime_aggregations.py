"""ID and metadata source that ingests AnimeAggregations entries."""

import asyncio
import json
import subprocess
from collections import Counter
from logging import getLogger
from pathlib import Path
from typing import Any

from anibridge_mappings.core.graph import IdMappingGraph
from anibridge_mappings.core.meta import MetaStore, SourceType
from anibridge_mappings.sources.base import IdMappingSource, MetadataSource

log = getLogger(__name__)


class AnimeAggregationsSource(IdMappingSource, MetadataSource):
    """Emit ID links and metadata derived from the AnimeAggregations dataset."""

    REPO_URL = "https://github.com/notseteve/AnimeAggregations.git"
    ANIME_DIR = "anime"
    LOCAL_REPO_ROOT = Path("data/meta/AnimeAggregations")
    DEFAULT_SCOPE = "R"

    def __init__(self) -> None:
        """Initialize the local cache for fetched entries."""
        self._entries: list[dict[str, Any]] = []
        self._prepared = False

    async def prepare(self) -> None:
        """Ensure the repo is present and up to date, then cache entries."""
        if self._prepared:
            return

        repo_root = self.LOCAL_REPO_ROOT
        await asyncio.to_thread(self._ensure_repo, repo_root)
        self._entries = await asyncio.to_thread(self._load_entries, repo_root)
        self._prepared = True

    async def collect_metadata(self, id_graph: IdMappingGraph) -> MetaStore:
        """Populate and return a metadata store derived from the dataset.

        Args:
            id_graph (IdMappingGraph): ID graph (unused).

        Returns:
            MetaStore: Collected metadata.
        """
        del id_graph
        self._ensure_prepared()

        store = MetaStore()
        for entry in self._entries:
            anidb_id = self._normalize_numeric(entry.get("anime_id"))
            if anidb_id is None:
                continue

            resources = entry.get("resources")
            if not isinstance(resources, dict):
                resources = {}

            episodes = entry.get("episodes")
            if not isinstance(episodes, dict):
                episodes = {}
            main_episodes = episodes.get("REGULAR")
            if not isinstance(main_episodes, list):
                main_episodes = []
            special_episodes = episodes.get("SPECIAL")
            if not isinstance(special_episodes, list):
                special_episodes = []

            meta_type = self._parse_type(entry.get("type"), episodes=len(main_episodes))
            duration = self._extract_duration(entry.get("episodes"))
            start_year = self._extract_start_year(entry)

            if main_episodes:
                meta = store.get(
                    "anidb",
                    anidb_id,
                    scope=self.DEFAULT_SCOPE,
                )
                meta.episodes = len(main_episodes)
                if meta_type is not None:
                    meta.type = meta_type
                if duration is not None:
                    meta.duration = duration
                if start_year is not None:
                    meta.start_year = start_year

            if special_episodes:
                specials_meta = store.get("anidb", anidb_id, scope="S")
                specials_meta.episodes = len(special_episodes)
                if specials_meta.type is None:
                    specials_meta.type = SourceType.TV

        return store

    def build_id_graph(self) -> IdMappingGraph:
        """Produce AniDB to external ID equivalence classes.

        Returns:
            IdMappingGraph: ID mapping graph for the dataset.
        """
        self._ensure_prepared()

        graph = IdMappingGraph()
        for entry in self._entries:
            anidb_id = self._normalize_numeric(entry.get("anime_id"))
            if anidb_id is None:
                continue

            resources = entry.get("resources")
            if not isinstance(resources, dict):
                continue

            nodes: list[tuple[str, str, str | None]] = [
                ("anidb", anidb_id, AnimeAggregationsSource.DEFAULT_SCOPE)
            ]
            nodes.extend(
                ("mal", mal_id, None) for mal_id in self._collect_mal(resources)
            )

            imdb_ids = self._collect_imdb(resources)
            _, tmdb_movies = self._collect_tmdb(resources)
            nodes.extend(("tmdb_movie", movie_id, None) for movie_id in tmdb_movies)

            if imdb_ids and tmdb_movies:
                nodes.extend(("imdb_movie", imdb_id, None) for imdb_id in imdb_ids)

            deduped = list(dict.fromkeys(nodes))
            if len(deduped) >= 2:
                graph.add_equivalence_class(deduped)

        return graph

    def _ensure_prepared(self) -> None:
        """Raise if the source has not been prepared."""
        if not self._prepared:
            raise RuntimeError("Source not initialized.")

    @classmethod
    def _ensure_repo(cls, repo_root: Path) -> None:
        """Clone or update the AnimeAggregations repo (with sparse checkout)."""
        if not repo_root.exists():
            repo_root.parent.mkdir(parents=True, exist_ok=True)
            subprocess.run(
                [
                    "git",
                    "clone",
                    "--filter=blob:none",
                    "--sparse",
                    "--depth",
                    "1",
                    cls.REPO_URL,
                    str(repo_root),
                ],
                check=True,
            )
            # Enable sparse-checkout and set to only anime/
            subprocess.run(
                ["git", "-C", str(repo_root), "sparse-checkout", "init", "--cone"],
                check=True,
            )
            subprocess.run(
                ["git", "-C", str(repo_root), "sparse-checkout", "set", cls.ANIME_DIR],
                check=True,
            )
        else:
            # Pull latest changes for anime/ only
            subprocess.run(
                ["git", "-C", str(repo_root), "pull", "origin", "main"], check=True
            )

    @classmethod
    def _load_entries(cls, repo_root: Path) -> list[dict[str, Any]]:
        """Load entry payloads from the local repository."""
        anime_dir = repo_root / cls.ANIME_DIR
        if not anime_dir.is_dir():
            raise RuntimeError("AnimeAggregations repo missing anime/ directory")

        entries: list[dict[str, Any]] = []
        for path in sorted(anime_dir.glob("*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                log.warning("Skipping invalid JSON file %s", path.name)
                continue

            if not isinstance(payload, dict):
                continue

            if payload.get("anime_id") is None and path.stem.isdigit():
                payload["anime_id"] = int(path.stem)

            entries.append(payload)

        return entries

    @staticmethod
    def _normalize_numeric(value: Any) -> str | None:
        """Normalize numeric IDs into string values."""
        raw = str(value).strip()
        if not raw.isdigit():
            return None
        return raw

    @staticmethod
    def _collect_imdb(resources: dict[str, Any]) -> list[str]:
        """Collect IMDb IDs from the resources payload."""
        imdb_entries = resources.get("IMDB")
        if not isinstance(imdb_entries, list):
            return []
        normalized = {entry.strip() for entry in imdb_entries if isinstance(entry, str)}
        return sorted(filter(None, normalized))

    @staticmethod
    def _collect_mal(resources: dict[str, Any]) -> list[str]:
        """Collect MyAnimeList IDs from the resources payload."""
        mal_entries = resources.get("MAL")
        if not isinstance(mal_entries, list):
            return []
        normalized: set[str] = set()
        for entry in mal_entries:
            raw = str(entry).strip()
            if raw.isdigit():
                normalized.add(raw)
        return sorted(normalized)

    @staticmethod
    def _collect_tmdb(resources: dict[str, Any]) -> tuple[list[str], list[str]]:
        """Collect TMDB show and movie IDs from the resources payload."""
        tmdb_entries = resources.get("TMDB")
        if not isinstance(tmdb_entries, list):
            return ([], [])

        show_ids: set[str] = set()
        movie_ids: set[str] = set()
        for entry in tmdb_entries:
            raw = str(entry).strip()
            if not raw:
                continue
            if raw.startswith("tv/"):
                candidate = raw.split("/", 1)[1]
                if candidate.isdigit():
                    show_ids.add(candidate)
            elif raw.startswith("movie/"):
                candidate = raw.split("/", 1)[1]
                if candidate.isdigit():
                    movie_ids.add(candidate)

        return (sorted(show_ids), sorted(movie_ids))

    @staticmethod
    def _parse_type(raw_type: Any, episodes: int | None = None) -> SourceType | None:
        """Parse the AnimeAggregations type string into SourceType."""
        if not isinstance(raw_type, str):
            return None
        normalized = raw_type.strip().upper()
        if not normalized:
            return None
        if normalized in {"MOVIE"}:
            return SourceType.MOVIE
        if normalized in {"SERIES", "OVA", "SPECIAL"}:
            return SourceType.TV
        if normalized in {"OTHER", "UNKNOWN", "WEB"}:
            return SourceType.MOVIE if episodes == 1 else SourceType.TV
        return None

    @staticmethod
    def _extract_duration(episodes_payload: Any) -> int | None:
        """Return the most common episode duration (minutes) when available."""
        if not isinstance(episodes_payload, dict):
            return None

        episode_lists = []
        if isinstance(episodes_payload.get("REGULAR"), list):
            episode_lists.append(episodes_payload["REGULAR"])
        else:
            episode_lists.extend(
                value for value in episodes_payload.values() if isinstance(value, list)
            )

        lengths: list[int] = []
        for episodes in episode_lists:
            for entry in episodes:
                if not isinstance(entry, dict):
                    continue
                length = entry.get("length")
                if isinstance(length, int) and length > 0:
                    lengths.append(length)

        if not lengths:
            return None

        most_common, _count = Counter(lengths).most_common(1)[0]
        return most_common

    @staticmethod
    def _extract_start_year(entry: dict[str, Any]) -> int | None:
        """Extract the start year from known date fields."""
        for key in ("start_date", "end_date"):
            raw = entry.get(key)
            if isinstance(raw, str) and len(raw) >= 4 and raw[:4].isdigit():
                return int(raw[:4])

        episodes_payload = entry.get("episodes")
        if not isinstance(episodes_payload, dict):
            return None
        for value in episodes_payload.values():
            if not isinstance(value, list) or not value:
                continue
            for episode in value:
                if not isinstance(episode, dict):
                    continue
                air_date = episode.get("air_date")
                if (
                    isinstance(air_date, str)
                    and len(air_date) >= 4
                    and air_date[:4].isdigit()
                ):
                    return int(air_date[:4])
        return None
