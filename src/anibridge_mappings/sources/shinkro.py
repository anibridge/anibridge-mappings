"""Episode mappings derived from varoOP/shinkro-mapping."""

import logging
from typing import Any

import aiohttp
from ruamel.yaml import YAML

from anibridge_mappings.core.graph import EpisodeMappingGraph, IdMappingGraph
from anibridge_mappings.core.meta import MetaStore
from anibridge_mappings.sources.base import (
    BaseSource,
    EpisodeMappingSource,
    IdMappingSource,
)

log = logging.getLogger(__name__)


class BaseShinkroMappingSource(BaseSource):
    """Shared loader for Shinkro mapping files."""

    SOURCE_URL: str = ""

    def __init__(self) -> None:
        """Initialize shared Shinkro loader state."""
        self._entries: list[dict[str, Any]] = []
        self._prepared = False

    async def prepare(self) -> None:
        """Fetch and parse the upstream mapping payload."""
        async with (
            aiohttp.ClientSession() as session,
            session.get(self.SOURCE_URL) as response,
        ):
            response.raise_for_status()
            payload = await response.text()

        yaml = YAML(typ="safe")
        parsed = yaml.load(payload)
        entries = self._extract_entries(parsed)

        if not isinstance(entries, list):
            raise RuntimeError("Unexpected Shinkro payload shape; expected list")

        self._entries = entries
        self._prepared = True

    def _extract_entries(self, parsed: Any) -> list[dict[str, Any]]:
        """Extract the list of mapping entries from the parsed payload."""
        raise NotImplementedError

    def _require_entries(self) -> list[dict[str, Any]]:
        """Return cached entries or raise if `prepare` was skipped."""
        if not self._prepared:
            raise RuntimeError("Source not initialized.")
        return self._entries

    @staticmethod
    def _normalize_id(value: Any) -> str | None:
        """Normalize an ID to a positive numeric string."""
        try:
            num = int(value)
        except (TypeError, ValueError):
            return None
        if num <= 0:
            return None
        return str(num)

    @staticmethod
    def _normalize_positive_int(value: Any) -> str | None:
        """Normalize an integer to a positive numeric string."""
        try:
            num = int(value)
        except (TypeError, ValueError):
            return None
        if num <= 0:
            return None
        return str(num)


class ShinkroTvdbMappingSource(
    BaseShinkroMappingSource, IdMappingSource, EpisodeMappingSource
):
    """Ingest TVDB↔MAL mappings maintained by the Shinkro community."""

    SOURCE_URL = (
        "https://raw.githubusercontent.com/varoOP/shinkro-mapping/main/"
        "tvdb-mal-master.yaml"
    )

    def _extract_entries(self, parsed: Any) -> list[dict[str, Any]]:
        """Extract TVDB mapping entries from the parsed payload."""
        if isinstance(parsed, dict) and "AnimeMap" in parsed:
            parsed = parsed.get("AnimeMap")
        if not isinstance(parsed, list):
            raise RuntimeError("Unexpected Shinkro TVDB payload shape; expected list")
        return parsed

    def build_id_graph(self) -> IdMappingGraph:
        """Link MAL entries to their referenced TVDB seasons.

        Returns:
            IdMappingGraph: ID mapping graph for TVDB↔MAL.
        """
        graph = IdMappingGraph()

        for entry in self._require_entries():
            mal_id = self._normalize_id(entry.get("malid"))
            tvdb_id = self._normalize_positive_int(entry.get("tvdbid"))
            if not mal_id or not tvdb_id:
                continue

            scopes = self._collect_scopes(entry)
            if not scopes:
                continue

            nodes = [("mal", mal_id, None)]
            nodes.extend(
                ("tvdb_show", tvdb_id, self._scope_label(scope)) for scope in scopes
            )
            graph.add_equivalence_class(nodes)

        return graph

    def build_episode_graph(
        self,
        store: MetaStore,
        id_graph: IdMappingGraph,
    ) -> EpisodeMappingGraph:
        """Create episode mappings between TVDB seasons and MAL.

        Args:
            store (MetaStore): Metadata store for TVDB episode totals.
            id_graph (IdMappingGraph): ID mapping graph (unused).

        Returns:
            EpisodeMappingGraph: Episode mapping graph for TVDB↔MAL.
        """
        del id_graph
        graph = EpisodeMappingGraph()

        for entry in self._require_entries():
            mal_id = self._normalize_id(entry.get("malid"))
            tvdb_id = self._normalize_positive_int(entry.get("tvdbid"))
            if not mal_id or not tvdb_id:
                continue

            mappings = entry.get("animeMapping") or []
            if bool(entry.get("useMapping")) and mappings:
                for mapping in mappings:
                    season = self._normalize_season(mapping.get("tvdbseason"))
                    if season is None:
                        continue

                    mapping_type = str(mapping.get("mappingType") or "range").lower()
                    if mapping_type == "explicit":
                        pairs = self._explicit_pairs(mapping)
                    else:
                        total = self._season_total(store, tvdb_id, season)
                        if total is None:
                            continue
                        start = self._normalize_start(mapping.get("start"))
                        skip = self._normalize_skip(mapping.get("skipMalEpisodes"))
                        pairs = self._range_pairs(start, total, skip)

                    self._add_pairs(graph, tvdb_id, season, mal_id, pairs)

            else:
                season = self._normalize_season(entry.get("tvdbseason"))
                if season is None:
                    continue

                total = self._season_total(store, tvdb_id, season)
                if total is None:
                    continue

                start = self._normalize_start(entry.get("start"))
                pairs = self._range_pairs(start, total, set())
                self._add_pairs(graph, tvdb_id, season, mal_id, pairs)

        return graph

    def _collect_scopes(self, entry: dict[str, Any]) -> set[int]:
        """Collect TVDB season scopes referenced by a mapping entry."""
        scopes: set[int] = set()
        if bool(entry.get("useMapping")):
            for mapping in entry.get("animeMapping") or []:
                season = self._normalize_season(mapping.get("tvdbseason"))
                if season is not None:
                    scopes.add(season)
            if not scopes:
                fallback = self._normalize_season(entry.get("tvdbseason"))
                if fallback is not None:
                    scopes.add(fallback)
        else:
            season = self._normalize_season(entry.get("tvdbseason"))
            if season is not None:
                scopes.add(season)
        return scopes

    def _season_total(self, store: MetaStore, tvdb_id: str, season: int) -> int | None:
        """Return the episode count for a TVDB season from metadata."""
        scope = self._scope_label(season)
        meta = store.peek("tvdb_show", tvdb_id, scope)
        if meta and isinstance(meta.episodes, int) and meta.episodes > 0:
            return meta.episodes
        log.debug("Missing TVDB metadata for %s season %s", tvdb_id, season)
        return None

    def _range_pairs(
        self, start: int, tvdb_total: int, skip_mal: set[int]
    ) -> list[tuple[int, int]]:
        """Generate TVDB→MAL episode pairs with optional skips."""
        if tvdb_total <= 0:
            return []

        pairs: list[tuple[int, int]] = []
        mal_episode = start if start > 0 else 1

        for tvdb_episode in range(1, tvdb_total + 1):
            while mal_episode in skip_mal:
                mal_episode += 1
            pairs.append((tvdb_episode, mal_episode))
            mal_episode += 1

        return pairs

    def _explicit_pairs(self, mapping: dict[str, Any]) -> list[tuple[int, int]]:
        """Extract explicit TVDB→MAL episode pairs from a mapping."""
        explicit = mapping.get("explicitEpisodes") or {}
        if not isinstance(explicit, dict):
            return []

        pairs: list[tuple[int, int]] = []
        for tvdb_ep_raw, mal_ep_raw in explicit.items():
            tvdb_ep = self._normalize_season(tvdb_ep_raw)
            mal_ep = self._normalize_season(mal_ep_raw)
            if tvdb_ep is None or mal_ep is None or mal_ep == 0:
                continue
            pairs.append((tvdb_ep, mal_ep))
        return pairs

    def _add_pairs(
        self,
        graph: EpisodeMappingGraph,
        tvdb_id: str,
        season: int,
        mal_id: str,
        pairs: list[tuple[int, int]],
    ) -> None:
        """Add episode mapping segments to the graph."""
        if not pairs:
            return

        segments = self._pairs_to_segments(sorted(pairs))
        scope = self._scope_label(season)

        for tvdb_start, tvdb_end, mal_start, mal_end in segments:
            source_node = (
                "tvdb_show",
                tvdb_id,
                scope,
                self._format_episode_label(tvdb_start, tvdb_end),
            )
            target_node = (
                "mal",
                mal_id,
                None,
                self._format_episode_label(mal_start, mal_end),
            )
            graph.add_edge(source_node, target_node)

    @staticmethod
    def _pairs_to_segments(
        pairs: list[tuple[int, int]],
    ) -> list[tuple[int, int, int, int]]:
        """Collapse consecutive pairs into contiguous segments."""
        if not pairs:
            return []

        segments: list[tuple[int, int, int, int]] = []
        start_source, start_target = pairs[0]
        prev_source, prev_target = start_source, start_target

        for source, target in pairs[1:]:
            if source == prev_source + 1 and target == prev_target + 1:
                prev_source = source
                prev_target = target
                continue

            segments.append((start_source, prev_source, start_target, prev_target))
            start_source = prev_source = source
            start_target = prev_target = target

        segments.append((start_source, prev_source, start_target, prev_target))
        return segments

    @staticmethod
    def _normalize_season(value: Any) -> int | None:
        """Normalize a season number into a non-negative integer."""
        try:
            num = int(value)
        except (TypeError, ValueError):
            return None
        if num < 0:
            return None
        return num

    @staticmethod
    def _normalize_start(value: Any) -> int:
        """Normalize a start value to a non-negative integer."""
        try:
            num = int(value)
        except (TypeError, ValueError):
            return 0
        return max(num, 0)

    @staticmethod
    def _normalize_skip(value: Any) -> set[int]:
        """Normalize skip entries to a set of positive integers."""
        if not isinstance(value, list):
            return set()
        skip: set[int] = set()
        for item in value:
            try:
                candidate = int(item)
            except (TypeError, ValueError):
                continue
            if candidate > 0:
                skip.add(candidate)
        return skip

    @staticmethod
    def _scope_label(season: int) -> str:
        """Format a season number into a scope label."""
        return f"s{season}"

    @staticmethod
    def _format_episode_label(start: int, end: int) -> str:
        """Format an episode range label."""
        if start == end:
            return f"{start}"
        return f"{start}-{end}"


class ShinkroTmdbMappingSource(
    BaseShinkroMappingSource, IdMappingSource, EpisodeMappingSource
):
    """Map TMDB movies to MAL using Shinkro data."""

    SOURCE_URL = (
        "https://raw.githubusercontent.com/varoOP/shinkro-mapping/main/"
        "tmdb-mal-master.yaml"
    )

    def _extract_entries(self, parsed: Any) -> list[dict[str, Any]]:
        """Extract TMDB mapping entries from the parsed payload."""
        if isinstance(parsed, dict) and "animeMovies" in parsed:
            parsed = parsed.get("animeMovies")
        if not isinstance(parsed, list):
            raise RuntimeError("Unexpected Shinkro TMDB payload shape; expected list")
        return parsed

    def build_id_graph(self) -> IdMappingGraph:
        """Link TMDB movie IDs to MAL movie identifiers.

        Returns:
            IdMappingGraph: ID mapping graph for TMDB↔MAL movies.
        """
        graph = IdMappingGraph()

        for entry in self._require_entries():
            mal_id = self._normalize_id(entry.get("malid"))
            tmdb_id = self._normalize_positive_int(entry.get("tmdbid"))
            if not mal_id or not tmdb_id:
                continue

            nodes = [
                ("tmdb_movie", tmdb_id, None),
                ("mal", mal_id, None),
            ]
            graph.add_equivalence_class(nodes)

        return graph

    def build_episode_graph(
        self, store: MetaStore, id_graph: IdMappingGraph
    ) -> EpisodeMappingGraph:
        """Produce movie-to-movie edges for TMDB↔MAL entries.

        Args:
            store (MetaStore): Metadata store (unused).
            id_graph (IdMappingGraph): ID mapping graph (unused).

        Returns:
            EpisodeMappingGraph: Episode mapping graph for movie mappings.
        """
        del store, id_graph
        graph = EpisodeMappingGraph()

        for entry in self._require_entries():
            mal_id = self._normalize_id(entry.get("malid"))
            tmdb_id = self._normalize_positive_int(entry.get("tmdbid"))
            if not mal_id or not tmdb_id:
                continue

            source_node = ("tmdb_movie", tmdb_id, None, "1")
            target_node = ("mal", mal_id, None, "1")
            graph.add_edge(source_node, target_node)

        return graph
