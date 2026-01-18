"""ID source that ingests AnimeAggregations external references."""

from logging import getLogger
from typing import Any

import aiohttp

from anibridge_mappings.core.graph import IdMappingGraph
from anibridge_mappings.sources.base import IdMappingSource

log = getLogger(__name__)


class AnimeAggregationsSource(IdMappingSource):
    """Emit ID links derived from the AnimeAggregations dataset."""

    SOURCE_URL = (
        "https://raw.githubusercontent.com/notseteve/AnimeAggregations/main/"
        "aggregate/AnimeToExternal.json"
    )

    def __init__(self) -> None:
        """Initialize the local cache for fetched entries."""
        self._entries: dict[str, dict[str, Any]] = {}
        self._prepared = False

    async def prepare(self) -> None:
        """Download and cache the upstream dataset."""
        async with (
            aiohttp.ClientSession() as session,
            session.get(self.SOURCE_URL) as response,
        ):
            response.raise_for_status()
            payload: dict[str, Any] = await response.json(content_type=None)

        animes = payload.get("animes")
        if not isinstance(animes, dict):
            raise RuntimeError("AnimeAggregations payload missing 'animes' map")
        self._entries = animes
        self._prepared = True

    def build_id_graph(self) -> IdMappingGraph:
        """Produce AniDB to external ID equivalence classes.

        Returns:
            IdMappingGraph: ID mapping graph for the dataset.
        """
        self._ensure_prepared()

        graph = IdMappingGraph()
        for anidb_id_raw, entry in self._entries.items():
            anidb_id = self._normalize_numeric(anidb_id_raw)
            if anidb_id is None:
                continue

            resources = entry.get("resources")
            if not isinstance(resources, dict):
                continue

            nodes: list[tuple[str, str, str | None]] = [("anidb", anidb_id, None)]
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
