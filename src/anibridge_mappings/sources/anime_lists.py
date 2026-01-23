"""Module for the Anime-Lists/anime-lists source."""

from logging import getLogger

import aiohttp
from lxml import etree

from anibridge_mappings.core.graph import EpisodeMappingGraph, IdMappingGraph
from anibridge_mappings.core.meta import MetaStore
from anibridge_mappings.sources.base import EpisodeMappingSource, IdMappingSource

TargetSpec = tuple[str, str, str, int]

log = getLogger(__name__)


class AnimeListsSource(IdMappingSource, EpisodeMappingSource):
    """Source class for Anime-Lists/anime-lists."""

    SOURCE_URL = "https://github.com/Anime-Lists/anime-lists/raw/refs/heads/master/anime-list-master.xml"
    DEFAULT_ANIDB_SCOPE = "R"
    DEFAULT_SEASON_SCOPE = "s1"

    def __init__(self) -> None:
        """Initialize the source class."""
        self._data: etree._Element | None = None

    async def prepare(self) -> None:
        """Fetch and cache the upstream XML payload."""
        async with (
            aiohttp.ClientSession() as session,
            session.get(self.SOURCE_URL) as response,
        ):
            if response.status != 200:
                raise Exception(
                    f"Failed to fetch data from {self.SOURCE_URL}: {response.status}"
                )
            xml_bytes = await response.read()
            parser = etree.XMLParser(resolve_entities=False, no_network=True)
            self._data = etree.fromstring(xml_bytes, parser=parser)

    def build_id_graph(self) -> IdMappingGraph:
        """Graph AniDB IDs to other provider IDs.

        Returns:
            IdMappingGraph: ID mapping graph for the dataset.
        """
        if self._data is None:
            raise RuntimeError("Source not initialized.")

        graph = IdMappingGraph()
        for anime_el in self._data.findall("anime"):
            anidb_id = anime_el.get("anidbid")
            if not anidb_id:
                log.warning("Anime entry missing AniDB ID; skipping.")
                continue

            source_scopes = {
                self._anidb_scope_from_attr(mapping_el.get("anidbseason"))
                for mapping_el in anime_el.findall("mapping-list/mapping")
            }
            # TODO: is it actually ever possible for a mapping to NOT include R?
            source_scopes.add(AnimeListsSource.DEFAULT_ANIDB_SCOPE)

            nodes: list[tuple[str, str, str | None]] = [
                ("anidb", anidb_id, scope) for scope in source_scopes
            ]

            imdb_ids = self._split_ids(anime_el.get("imdbid"))
            tmdb_movie_ids = self._split_ids(anime_el.get("tmdbid"))

            tmdb_scope = (
                self._scope_from_attr(anime_el.get("tmdbseason"))
                or AnimeListsSource.DEFAULT_SEASON_SCOPE
            )
            tmdb_raw = (anime_el.get("tmdbtv") or "").strip()
            tmdb_show = tmdb_raw if tmdb_raw.isdigit() else None

            tvdb_scope = (
                self._scope_from_attr(anime_el.get("defaulttvdbseason"))
                or AnimeListsSource.DEFAULT_SEASON_SCOPE
            )
            tvdb_raw = (anime_el.get("tvdbid") or "").strip()
            tvdb = tvdb_raw if tvdb_raw.isdigit() else None

            if imdb_ids:  # Anime-Lists only supplies IMDB IDs for movies
                nodes.extend(("imdb_movie", imdb, None) for imdb in imdb_ids)

            for tmdb_movie in tmdb_movie_ids:
                nodes.append(("tmdb_movie", tmdb_movie, None))
            if tmdb_show:
                nodes.append(("tmdb_show", tmdb_show, tmdb_scope))
            if tvdb:
                nodes.append(("tvdb_show", tvdb, tvdb_scope))

            graph.add_equivalence_class(nodes)

        return graph

    def build_episode_graph(
        self,
        store: MetaStore,
        id_graph: IdMappingGraph,
    ) -> EpisodeMappingGraph:
        """Graph provider episode mappings with optional metadata fallbacks.

        Args:
            store (MetaStore): Metadata store used for fallbacks.
            id_graph (IdMappingGraph): ID mapping graph (unused).

        Returns:
            EpisodeMappingGraph: Episode mapping graph for the dataset.
        """
        del id_graph
        if self._data is None:
            raise RuntimeError("Source not initialized.")

        graph = EpisodeMappingGraph()
        for anime_el in self._data.findall("anime"):
            anidb_id = (anime_el.get("anidbid") or "").strip()
            if not anidb_id:
                log.warning("Anime entry missing AniDB ID; skipping episode mappings.")
                continue

            # Track which (src_scope, provider, target_scope) combinations are covered
            # by mapping-list edges (including explicit "mapped to 0" conflicts).
            coverage: set[tuple[str, str, str]] = set()

            mapping_list = anime_el.find("mapping-list")
            if mapping_list is not None:
                for mapping_el in mapping_list.findall("mapping"):
                    source_scope = self._anidb_scope_from_attr(
                        mapping_el.get("anidbseason")
                    )
                    if source_scope is None:
                        log.warning(
                            "Mapping missing AniDB season for AniDB ID %s; skipping.",
                            anidb_id,
                        )
                        continue

                    pairs = self._extract_mapping_pairs(mapping_el)
                    if not pairs:
                        continue

                    target_specs = self._extract_target_specs(
                        anime_el, mapping_el, apply_default_offsets=False
                    )
                    if not target_specs:
                        continue

                    for source_episode, target_episodes in pairs:
                        source_key = self._episode_key(source_episode)
                        if source_key is None:
                            continue

                        # Keep track of whether this mapping explicitly includes "0"
                        had_zero = any(ep.strip() == "0" for ep in target_episodes)

                        filtered_targets: list[str] = []
                        for ep in target_episodes:
                            trimmed = ep.strip()
                            if not trimmed:
                                continue
                            if trimmed == "0":
                                # Spec: "mapped to 0" is meaningful for conflicts;
                                # do not create an edge, but do count as coverage below.
                                continue
                            key = self._episode_key(trimmed)
                            if key is not None:
                                filtered_targets.append(key)

                        # Even if all targets are 0/blank, we should still record
                        # coverage so that default fallbacks don't override.
                        if had_zero and not filtered_targets:
                            for provider, entry_id, target_scope, _ in target_specs:
                                if entry_id:
                                    coverage.add((source_scope, provider, target_scope))
                            continue

                        if not filtered_targets:
                            continue

                        for provider, entry_id, target_scope, _unused in target_specs:
                            if not entry_id:
                                continue
                            source_node = (
                                "anidb",
                                anidb_id,
                                source_scope,
                                source_key,
                            )
                            for target_key in filtered_targets:
                                target_node = (
                                    provider,
                                    entry_id,
                                    target_scope,
                                    target_key,
                                )
                                graph.add_edge(source_node, target_node)
                            coverage.add((source_scope, provider, target_scope))

            self._add_movie_episode_mappings(anime_el, anidb_id, graph)

            # Default mappings use episodeoffset/tmdboffset ONLY when mapping-list
            # doesn't already cover that provider/season.
            self._add_default_episode_mappings(
                anime_el,
                anidb_id,
                coverage,
                graph,
                store,
            )

        return graph

    @staticmethod
    def _scope_from_attr(raw_scope: str | None) -> str | None:
        """Normalize a season value into a ProviderScope string."""
        if raw_scope is None:
            return None
        scope = raw_scope.strip()
        if not scope:
            return None
        value = "1" if scope.lower() == "a" else scope
        return f"s{value}"

    @classmethod
    def _anidb_scope_from_attr(cls, raw_scope: str | None) -> str | None:
        """Normalize AniDB scope values into episode-type scopes."""
        if raw_scope is None:
            return None
        scope = raw_scope.strip()
        if not scope:
            return None

        lowered = scope.lower()
        if lowered in {"a", "1", "r", "regular"}:
            return cls.DEFAULT_ANIDB_SCOPE
        if lowered in {"0", "s", "special", "specials"}:
            return "S"

        if len(scope) == 1 and scope.isalpha():
            return scope.upper()

        if lowered.startswith("s") and lowered[1:].isdigit():
            value = int(lowered[1:])
            if value == 0:
                return "S"
            if value == 1:
                return cls.DEFAULT_ANIDB_SCOPE
            log.debug(
                "Unexpected AniDB scope %s; treating as regular.",
                scope,
            )
            return cls.DEFAULT_ANIDB_SCOPE

        if lowered.isdigit():
            value = int(lowered)
            if value == 0:
                return "S"
            return cls.DEFAULT_ANIDB_SCOPE

        log.debug("Unrecognized AniDB scope %s; treating as regular.", scope)
        return cls.DEFAULT_ANIDB_SCOPE

    @staticmethod
    def _episode_key(value: str | None) -> str | None:
        """Convert a raw episode number into an EpisodeRangeKey."""
        if value is None:
            return None
        trimmed = value.strip()
        if not trimmed:
            return None
        if trimmed.lower() == "movie":
            return "1"
        return trimmed

    @classmethod
    def _extract_mapping_pairs(
        cls, mapping_el: etree._Element
    ) -> list[tuple[str, list[str]]]:
        """Extract explicit or offset-based mapping pairs for a mapping element."""
        pairs = cls._parse_explicit_pairs(mapping_el.text)
        if pairs:
            return pairs
        return cls._parse_offset_pairs(mapping_el)

    @staticmethod
    def _parse_explicit_pairs(text: str | None) -> list[tuple[str, list[str]]]:
        """Parse explicit mapping pairs from a mapping-list text value."""
        if not text:
            return []

        tokens = [segment.strip() for segment in text.split(";") if segment.strip()]
        pairs: list[tuple[str, list[str]]] = []
        for token in tokens:
            if "-" not in token:
                continue
            left, right = token.split("-", 1)
            left = left.strip()
            right = right.strip()
            if not left or not right:
                continue
            targets = [part.strip() for part in right.split("+") if part.strip()]
            if not targets:
                continue
            left_parts = [part.strip() for part in left.split(",") if part.strip()]
            if not left_parts:
                continue
            for part in left_parts:
                pairs.append((part, targets))

        return pairs

    @staticmethod
    def _parse_offset_pairs(mapping_el: etree._Element) -> list[tuple[str, list[str]]]:
        """Parse offset-based mapping pairs from a mapping element."""
        start_raw = (mapping_el.get("start") or "").strip()
        if not start_raw:
            return []

        end_raw = (mapping_el.get("end") or start_raw).strip()
        offset_raw = (mapping_el.get("offset") or "0").strip() or "0"

        try:
            start = int(start_raw)
            end = int(end_raw)
        except ValueError:
            log.warning(
                "Invalid start/end values in mapping %s-%s; skipping offset mapping.",
                start_raw,
                end_raw,
            )
            return []

        if start > end:
            start, end = end, start

        try:
            offset = int(offset_raw)
        except ValueError:
            log.warning("Invalid offset value %s; defaulting to 0.", offset_raw)
            offset = 0

        pairs: list[tuple[str, list[str]]] = []
        for episode in range(start, end + 1):
            target_value = episode + offset
            if target_value <= 0:
                continue
            pairs.append((str(episode), [str(target_value)]))

        return pairs

    def _extract_target_specs(
        self,
        anime_el: etree._Element,
        mapping_el: etree._Element,
        *,
        apply_default_offsets: bool,
    ) -> list[TargetSpec]:
        """Collect provider/id/scope tuples referenced by a mapping tag."""
        specs: list[TargetSpec] = []

        # --- TVDB show ---
        default_tvdb_scope = self._scope_from_attr(anime_el.get("defaulttvdbseason"))
        tvdb_scope = (
            self._scope_from_attr(mapping_el.get("tvdbseason")) or default_tvdb_scope
        )
        normalized_tvdb_scope = tvdb_scope or AnimeListsSource.DEFAULT_SEASON_SCOPE
        tvdb_id = (anime_el.get("tvdbid") or "").strip()

        tvdb_offset = 0
        # Only apply episodeoffset when we're mapping into the default TVDB season.
        if (
            apply_default_offsets
            and default_tvdb_scope
            and normalized_tvdb_scope == default_tvdb_scope
        ):
            tvdb_offset = self._safe_int(anime_el.get("episodeoffset"))

        if tvdb_id.isdigit():
            specs.append(("tvdb_show", tvdb_id, normalized_tvdb_scope, tvdb_offset))
        elif tvdb_id:
            log.debug(
                "Skipping TVDB mapping for AniDB ID %s because TVDB ID %s "
                "is non-numeric.",
                anime_el.get("anidbid"),
                tvdb_id,
            )

        # --- TMDB show ---
        tmdb_season_attr = mapping_el.get("tmdbseason")
        tmdb_scope = self._scope_from_attr(tmdb_season_attr) or self._scope_from_attr(
            anime_el.get("tmdbseason")
        )
        normalized_tmdb_scope = tmdb_scope or AnimeListsSource.DEFAULT_SEASON_SCOPE
        tmdb_id = (anime_el.get("tmdbtv") or "").strip()

        tmdb_offset = 0
        if apply_default_offsets:
            tmdb_offset = self._safe_int(anime_el.get("tmdboffset"))

        # For mapping-list entries, only include TMDB targets when the tag
        # explicitly names a TMDB season. Otherwise, let the default fallback
        # handle TMDB to avoid duplicating season-split TVDB mappings into TMDB.
        tmdb_allowed = apply_default_offsets or tmdb_season_attr is not None

        if tmdb_allowed and tmdb_id.isdigit():
            specs.append(("tmdb_show", tmdb_id, normalized_tmdb_scope, tmdb_offset))
        elif tmdb_id and tmdb_allowed:
            log.debug(
                "Skipping TMDB mapping for AniDB ID %s because TMDB ID %s "
                "is non-numeric.",
                anime_el.get("anidbid"),
                tmdb_id,
            )

        return specs

    def _add_movie_episode_mappings(
        self,
        anime_el: etree._Element,
        anidb_id: str,
        graph: EpisodeMappingGraph,
    ) -> None:
        """Link AniDB movie entries to other providers in the episode graph."""
        targets = self._collect_movie_targets(anime_el)
        if not targets:
            return

        source_node = ("anidb", anidb_id, AnimeListsSource.DEFAULT_ANIDB_SCOPE, "1")
        for provider, entry_id in targets:
            target_node = (provider, entry_id, None, "1")
            graph.add_edge(source_node, target_node)

    def _collect_movie_targets(self, anime_el: etree._Element) -> list[tuple[str, str]]:
        """Return provider/id tuples that should be linked via movie scope."""
        tmdb_movies = self._split_ids(anime_el.get("tmdbid"))
        imdb_ids = self._split_ids(anime_el.get("imdbid"))
        tvdb_id_raw = (anime_el.get("tvdbid") or "").strip().lower()

        if not tmdb_movies and tvdb_id_raw != "movie":
            return []

        entries: list[tuple[str, str]] = []
        entries.extend(("tmdb_movie", tmdb_id) for tmdb_id in tmdb_movies)
        entries.extend(("imdb_movie", imdb_id) for imdb_id in imdb_ids)

        deduped: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for entry in entries:
            if entry[1] and entry not in seen:
                seen.add(entry)
                deduped.append(entry)
        return deduped

    def _add_default_episode_mappings(
        self,
        anime_el: etree._Element,
        anidb_id: str,
        coverage: set[tuple[str, str, str]],
        graph: EpisodeMappingGraph,
        store: MetaStore,
    ) -> None:
        """Fill in default season mappings when no explicit mapping exists."""
        source_scope = (
            self._anidb_scope_from_attr("1") or AnimeListsSource.DEFAULT_ANIDB_SCOPE
        )
        default_specs = self._extract_target_specs(
            anime_el, etree.Element("mapping"), apply_default_offsets=True
        )
        if not default_specs:
            return

        meta = store.get("anidb", anidb_id, source_scope)
        total_episodes = meta.episodes or 0

        if total_episodes <= 0:
            # Fall back to target-side episode counts when AniDB metadata is missing.
            fallback_total = 0
            for provider, entry_id, target_scope, _ in default_specs:
                target_meta = store.peek(provider, entry_id, target_scope)
                if target_meta and target_meta.episodes:
                    fallback_total = max(fallback_total, target_meta.episodes)
            total_episodes = fallback_total

        if total_episodes <= 0:
            return

        for provider, entry_id, target_scope, episode_offset in default_specs:
            key = (source_scope, provider, target_scope)
            if key in coverage:
                continue
            effective_total = total_episodes
            target_meta = store.peek(provider, entry_id, target_scope)
            if target_meta and target_meta.episodes and target_meta.episodes > 0:
                max_source = target_meta.episodes - episode_offset
                if max_source <= 0:
                    log.debug(
                        "Skipping default mapping for AniDB ID %s to %s:%s:%s "
                        "because target episode limit %s is too small for offset %s.",
                        anidb_id,
                        provider,
                        entry_id,
                        target_scope,
                        target_meta.episodes,
                        episode_offset,
                    )
                    continue
                effective_total = min(effective_total, max_source)
                if effective_total < total_episodes:
                    log.debug(
                        "Capping default mapping for AniDB ID %s to %s:%s:%s at %s "
                        "episodes due to target limit %s and offset %s.",
                        anidb_id,
                        provider,
                        entry_id,
                        target_scope,
                        effective_total,
                        target_meta.episodes,
                        episode_offset,
                    )

            if effective_total <= 0:
                continue

            self._link_full_episode_range(
                anidb_id,
                source_scope,
                provider,
                entry_id,
                target_scope,
                episode_offset,
                effective_total,
                graph,
            )

    def _link_full_episode_range(
        self,
        anidb_id: str,
        source_scope: str,
        provider: str,
        entry_id: str,
        target_scope: str,
        episode_offset: int,
        total_episodes: int,
        graph: EpisodeMappingGraph,
    ) -> None:
        """Connect AniDB episodes 1..N to a provider season using an offset."""
        if not entry_id:
            return
        segments = self._build_offset_segments(total_episodes, episode_offset)
        if not segments:
            return

        for source_start, source_end, target_start, target_end in segments:
            source_node = (
                "anidb",
                anidb_id,
                source_scope,
                self._format_episode_label(source_start, source_end),
            )
            target_node = (
                provider,
                entry_id,
                target_scope,
                self._format_episode_label(target_start, target_end),
            )
            graph.add_edge(source_node, target_node)

    @staticmethod
    def _apply_episode_offset(key: str, offset: int) -> str | None:
        """Shift numeric episode keys by `offset`, dropping invalid results."""
        if offset == 0:
            return key
        try:
            value = int(key)
        except ValueError:
            return key
        shifted = value + offset
        if shifted <= 0:
            return None
        return str(shifted)

    def _build_offset_segments(
        self, total_episodes: int, offset: int
    ) -> list[tuple[int, int, int, int]]:
        """Build contiguous source/target segments for an offset mapping."""
        pairs: list[tuple[int, int]] = []
        for episode in range(1, total_episodes + 1):
            target_key = self._apply_episode_offset(str(episode), offset)
            if target_key is None:
                continue
            try:
                target_value = int(target_key)
            except ValueError:
                continue
            pairs.append((episode, target_value))

        if not pairs:
            return []

        segments: list[tuple[int, int, int, int]] = []
        start_source = prev_source = pairs[0][0]
        start_target = prev_target = pairs[0][1]

        for source_episode, target_episode in pairs[1:]:
            if source_episode == prev_source + 1 and target_episode == prev_target + 1:
                prev_source = source_episode
                prev_target = target_episode
                continue

            segments.append((start_source, prev_source, start_target, prev_target))
            start_source = prev_source = source_episode
            start_target = prev_target = target_episode

        segments.append((start_source, prev_source, start_target, prev_target))
        return segments

    @staticmethod
    def _format_episode_label(start: int, end: int) -> str:
        """Format an episode range label for a start/end pair."""
        if start == end:
            return f"{start}"
        return f"{start}-{end}"

    @staticmethod
    def _safe_int(raw_value: str | None) -> int:
        """Parse an integer attribute, returning 0 on empty or invalid input."""
        if not raw_value:
            return 0
        trimmed = raw_value.strip()
        if not trimmed:
            return 0
        try:
            return int(trimmed)
        except ValueError:
            log.warning("Invalid integer value %s; defaulting to 0.", raw_value)
            return 0

    @staticmethod
    def _split_ids(raw_value: str | None) -> list[str]:
        """Split comma-separated IDs while trimming whitespace."""
        if not raw_value:
            return []
        return [segment.strip() for segment in raw_value.split(",") if segment.strip()]
