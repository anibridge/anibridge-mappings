"""ID and metadata source that ingests AnimeAggregations entries."""

import asyncio
import subprocess
from collections import Counter
from logging import getLogger
from pathlib import Path
from typing import Any

import orjson

from anibridge_mappings.core.graph import IdMappingGraph
from anibridge_mappings.core.meta import MetaStore, SourceType, normalize_titles
from anibridge_mappings.sources.base import IdMappingSource, MetadataSource
from anibridge_mappings.utils.provider_ids import normalize_imdb_id

log = getLogger(__name__)


class AnimeAggregationsSource(IdMappingSource, MetadataSource):
    """Emit ID links and metadata derived from the AnimeAggregations dataset."""

    REPO_URL = "https://github.com/notseteve/AnimeAggregations.git"
    ANIME_DIR = "anime"
    LOCAL_REPO_ROOT = Path("data/meta/AnimeAggregations")
    DEFAULT_SCOPE = "R"

    _ALLOWED_TITLE_LANGUAGES = frozenset(
        {
            "ENGLISH",
            "JAPANESE",
            "JAPANESE_TRANSLITERATED",
            "CHINESE",
            "CHINESE_SIMPLIFIED",
            "CHINESE_TRADITIONAL",
            "CHINESE_TRANSLITERATED",
        }
    )

    def __init__(self) -> None:
        """Initialize the local cache for fetched entries."""
        self._entries: list[dict[str, Any]] = []

    async def prepare(self) -> None:
        """Ensure the repo is present and up to date, then cache entries."""
        repo_root = self.LOCAL_REPO_ROOT
        await asyncio.to_thread(self._ensure_repo, repo_root)
        self._entries = await asyncio.to_thread(self._load_entries, repo_root)

    async def collect_metadata(self, id_graph: IdMappingGraph) -> MetaStore:
        """Populate and return a metadata store derived from the dataset.

        Args:
            id_graph (IdMappingGraph): ID graph (unused).

        Returns:
            MetaStore: Collected metadata.
        """
        del id_graph

        store = MetaStore()
        for entry in self._entries:
            anidb_id = self._normalize_numeric(entry.get("anime_id"))
            if anidb_id is None:
                continue

            episodes = entry.get("episodes") or {}
            main_episodes = episodes.get("REGULAR") or []
            special_episodes = episodes.get("SPECIAL") or []

            meta_type = self._parse_type(entry.get("type"), episodes=len(main_episodes))
            duration = self._extract_duration(entry.get("episodes"))
            start_year = self._extract_start_year(entry)

            titles = self._extract_titles(entry.get("titles"))

            if main_episodes:
                meta = store.get(
                    "anidb",
                    anidb_id,
                    scope=self.DEFAULT_SCOPE,
                )
                if meta_type is not None:
                    meta.type = meta_type

                # AnimeAggregations often models movies as multiple internal parts
                # with per-part lengths, which is too granular for our schema.
                if meta_type == SourceType.MOVIE:
                    meta.episodes = 1
                else:
                    meta.episodes = len(main_episodes)

                if duration is not None and meta_type != SourceType.MOVIE:
                    meta.duration = duration
                if start_year is not None:
                    meta.start_year = start_year
                if titles:
                    meta.titles = titles

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
        graph = IdMappingGraph()
        for entry in self._entries:
            anidb_id = self._normalize_numeric(entry.get("anime_id"))
            if anidb_id is None:
                continue

            resources = entry.get("resources")
            if not resources:
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
                payload = orjson.loads(path.read_bytes())
            except orjson.JSONDecodeError:
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
        if not imdb_entries:
            return []
        return sorted(
            {
                nid
                for entry in imdb_entries
                if (nid := normalize_imdb_id(entry)) is not None
            }
        )

    @staticmethod
    def _collect_mal(resources: dict[str, Any]) -> list[str]:
        """Collect MyAnimeList IDs from the resources payload."""
        mal_entries = resources.get("MAL")
        if not mal_entries:
            return []
        return sorted(
            {raw for entry in mal_entries if (raw := str(entry).strip()).isdigit()}
        )

    @staticmethod
    def _collect_tmdb(resources: dict[str, Any]) -> tuple[list[str], list[str]]:
        """Collect TMDB show and movie IDs from the resources payload."""
        tmdb_entries = resources.get("TMDB")
        if not tmdb_entries:
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
    def _parse_type(
        raw_type: str | None, episodes: int | None = None
    ) -> SourceType | None:
        """Parse the AnimeAggregations type string into SourceType."""
        if not raw_type:
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
    def _extract_duration(episodes_payload: dict[str, Any] | None) -> int | None:
        """Return the most common episode duration in seconds."""
        if not episodes_payload:
            return None

        regular = episodes_payload.get("REGULAR")
        episode_lists = [regular] if regular else list(episodes_payload.values())

        lengths: list[int] = []
        for episodes in episode_lists:
            for entry in episodes:
                length = entry.get("length")
                if length and length > 0:
                    lengths.append(length)

        if not lengths:
            return None

        most_common, _count = Counter(lengths).most_common(1)[0]
        return most_common

    @staticmethod
    def _extract_titles(
        raw_titles: list[dict[str, Any]] | None,
    ) -> tuple[str, ...] | None:
        """Extract MAIN and OFFICIAL titles from the titles payload."""
        if not raw_titles:
            return None
        selected: list[str] = []
        for item in raw_titles:
            title_type = item.get("type", "")
            if title_type not in ("MAIN", "OFFICIAL"):
                continue
            language = item.get("language", "")
            if language not in AnimeAggregationsSource._ALLOWED_TITLE_LANGUAGES:
                continue
            title = item.get("title", "")
            if title:
                selected.append(title)
        return normalize_titles(selected) or None

    @staticmethod
    def _extract_start_year(entry: dict[str, Any]) -> int | None:
        """Extract the start year from known date fields."""
        for key in ("start_date", "end_date"):
            raw = entry.get(key)
            if raw and raw[:4].isdigit():
                return int(raw[:4])

        episodes_payload = entry.get("episodes")
        if not episodes_payload:
            return None
        for episode_list in episodes_payload.values():
            if not episode_list:
                continue
            for episode in episode_list:
                air_date = episode.get("air_date")
                if air_date and air_date[:4].isdigit():
                    return int(air_date[:4])
        return None
