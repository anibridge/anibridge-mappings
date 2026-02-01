"""ID source driven by the Wikidata SPARQL endpoint.

This source only collects Anime ID links for movies. Wikidata does not provide clear
mappings for seasons, so this source omits TV show IDs to avoid ambiguity.
"""

import importlib.metadata
import re
from logging import getLogger
from typing import Any, ClassVar

import aiohttp

from anibridge_mappings.core.graph import IdMappingGraph
from anibridge_mappings.core.meta import MetaStore, SourceMeta, SourceType
from anibridge_mappings.sources.base import IdMappingSource, MetadataSource

log = getLogger(__name__)


class BaseQleverImdbSource(MetadataSource):
    """IMDb metadata source backed by the QLever IMDb endpoint."""

    ENDPOINT_URL = "https://qlever.dev/api/imdb"
    BATCH_SIZE = 200

    provider_key: ClassVar[str]
    allowed_types: ClassVar[frozenset[str]]

    def __init__(self, batch_size: int = BATCH_SIZE) -> None:
        """Initialize the IMDb metadata source.

        Args:
            batch_size (int): Maximum IMDb IDs per SPARQL request.
        """
        self._prepared = False
        self._batch_size = max(1, batch_size)

    async def prepare(self) -> None:
        """Initialize the source state."""
        self._prepared = True

    def _session_kwargs(self) -> dict[str, Any]:
        """Return aiohttp session settings for QLever requests."""
        return {
            "headers": {
                "Accept": "application/sparql-results+json",
                "User-Agent": (
                    "anibridge-mappings/{} "
                    "(https://github.com/anibridge/anibridge-mappings)".format(
                        importlib.metadata.version("anibridge-mappings")
                    )
                ),
            }
        }

    async def collect_metadata(self, id_graph: IdMappingGraph) -> MetaStore:
        """Populate and return metadata for IMDb IDs in the graph."""
        self._ensure_prepared()
        entry_ids = self._eligible_ids(id_graph)
        store = MetaStore()
        if not entry_ids:
            return store

        results = await self._fetch_entries(entry_ids)
        for entry_id, scope_meta in results:
            if not scope_meta:
                continue
            for scope, meta in scope_meta.items():
                store.set(self.provider_key, entry_id, meta, scope)
        return store

    async def _fetch_entries(
        self,
        entry_ids: list[tuple[str, str | None]],
    ) -> list[tuple[str, dict[str | None, SourceMeta] | None]]:
        if not entry_ids:
            return []

        base_ids = sorted({entry_id for entry_id, _scope in entry_ids})
        batches = [
            base_ids[i : i + self._batch_size]
            for i in range(0, len(base_ids), self._batch_size)
        ]

        results: list[tuple[str, dict[str | None, SourceMeta] | None]] = []
        async with aiohttp.ClientSession(**self._session_kwargs()) as session:
            for batch in batches:
                query, normalized_map = self._build_query(batch)
                params = {"query": query, "format": "json"}
                async with session.get(self.ENDPOINT_URL, params=params) as response:
                    response.raise_for_status()
                    payload: dict[str, Any] = await response.json()

                bindings = payload.get("results", {}).get("bindings")
                if not isinstance(bindings, list):
                    raise RuntimeError("Unexpected IMDb payload structure")
                if not bindings:
                    log.debug(
                        "IMDb batch returned no bindings (size=%s, sample=%s)",
                        len(batch),
                        ",".join(batch[:5]),
                    )

                meta_by_entry = self._parse_bindings(bindings, normalized_map)
                log.debug(
                    "IMDb batch parsed %s meta entries (size=%s)",
                    len(meta_by_entry),
                    len(batch),
                )
                for entry_id in batch:
                    meta = meta_by_entry.get(entry_id)
                    if meta is None:
                        log.debug("IMDb metadata missing for %s", entry_id)
                        results.append((entry_id, None))
                    else:
                        results.append((entry_id, {None: meta}))

        return results

    def _eligible_ids(self, id_graph: IdMappingGraph) -> list[tuple[str, str | None]]:
        ids: set[tuple[str, str | None]] = set()
        for node in id_graph.nodes():
            if not isinstance(node, tuple) or len(node) < 2:
                continue
            provider = node[0]
            entry_id = node[1]
            scope = node[2] if len(node) > 2 else None
            if provider == self.provider_key:
                ids.add((entry_id, scope))
        return sorted(ids)

    def _build_query(self, entry_ids: list[str]) -> tuple[str, dict[str, list[str]]]:
        normalized_map: dict[str, list[str]] = {}
        for entry_id in entry_ids:
            normalized = self._normalize_imdb_id(entry_id)
            normalized_map.setdefault(normalized, []).append(entry_id)

        values = " ".join(f'"{entry_id}"' for entry_id in normalized_map)
        values_clause = f"VALUES ?id {{ {values} }}"

        query = f"""
        PREFIX imdb: <https://www.imdb.com/>

        SELECT ?id ?type ?startYear ?runtimeMinutes ?episodeCount WHERE {{
            {values_clause}
            ?title imdb:id ?id ;
                   imdb:type ?type .
            OPTIONAL {{ ?title imdb:startYear ?startYear . }}
            OPTIONAL {{ ?title imdb:runtimeMinutes ?runtimeMinutes . }}
            OPTIONAL {{
                SELECT ?id (COUNT(?episode) AS ?episodeCount) WHERE {{
                    {values_clause}
                    ?title imdb:id ?id .
                    ?episode imdb:parentTitle ?title .
                }} GROUP BY ?id
            }}
        }}
        """

        return query, normalized_map

    def _parse_bindings(
        self,
        bindings: list[dict[str, Any]],
        normalized_map: dict[str, list[str]],
    ) -> dict[str, SourceMeta]:
        meta_by_entry: dict[str, SourceMeta] = {}
        for binding in bindings:
            entry_id = self._extract_str(binding, "id")
            if not entry_id:
                continue

            title_type = self._extract_str(binding, "type")
            if not title_type:
                continue

            start_year = self._extract_int(binding, "startYear")
            runtime_minutes = self._extract_int(binding, "runtimeMinutes")
            episode_count = self._extract_int(binding, "episodeCount")

            duration = (
                runtime_minutes if runtime_minutes and runtime_minutes > 0 else None
            )

            if title_type not in self.allowed_types:
                continue

            if title_type in {"tvSeries", "tvMiniSeries", "tvShort"}:
                episodes = (
                    episode_count if episode_count and episode_count > 0 else None
                )
                meta = SourceMeta(
                    type=SourceType.TV,
                    episodes=episodes,
                    duration=duration,
                    start_year=start_year,
                )
            else:
                meta = SourceMeta(
                    type=SourceType.MOVIE,
                    episodes=1,
                    duration=duration,
                    start_year=start_year,
                )

            for original_id in normalized_map.get(entry_id, []):
                meta_by_entry[original_id] = meta

        return meta_by_entry

    @staticmethod
    def _normalize_imdb_id(entry_id: str) -> str:
        """Normalize IMDb IDs to tconst form (ttXXXXXXX)."""
        trimmed = entry_id.strip()
        if trimmed.startswith("tt"):
            suffix = trimmed[2:]
            if suffix.isdigit() and len(suffix) < 7:
                return f"tt{suffix.zfill(7)}"
            return trimmed
        if trimmed.isdigit():
            return f"tt{trimmed.zfill(7)}"
        return f"tt{trimmed}"

    @staticmethod
    def _extract_str(binding: dict[str, Any], key: str) -> str | None:
        slot = binding.get(key)
        if not isinstance(slot, dict):
            return None
        raw = slot.get("value")
        if raw is None:
            return None
        text = str(raw).strip()
        return text or None

    @staticmethod
    def _extract_int(binding: dict[str, Any], key: str) -> int | None:
        value = BaseQleverImdbSource._extract_str(binding, key)
        if value is None:
            return None
        if value.isdigit():
            return int(value)
        return None

    def _ensure_prepared(self) -> None:
        if not self._prepared:
            raise RuntimeError("Source not initialized.")


class QleverImdbMovieSource(BaseQleverImdbSource):
    """Collect IMDb movie metadata for IDs already present in the ID graph."""

    provider_key = "imdb_movie"
    allowed_types: ClassVar[frozenset[str]] = frozenset(
        {"movie", "tvMovie", "short", "video", "tvSpecial", "tvPilot"}
    )


class QleverImdbShowSource(BaseQleverImdbSource):
    """Collect IMDb show metadata for IDs already present in the ID graph."""

    provider_key = "imdb_show"
    allowed_types: ClassVar[frozenset[str]] = frozenset(
        {"tvSeries", "tvMiniSeries", "tvShort"}
    )


class QleverWikidataSource(IdMappingSource):
    """Emit AniList-centered ID links derived from Wikidata."""

    # https://query.wikidata.org/sparql (robots policy blocking usage)
    ENDPOINT_URL = "https://qlever.dev/api/wikidata"
    QUERY = """
    # PREFIX statements are only required for qlever.dev
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>

    SELECT DISTINCT ?item ?prop ?id WHERE {
        ?item wdt:P31/wdt:P279* wd:Q20650540. # instance/subclass of 'anime film'

        VALUES ?prop {
            wdt:P5646 # anidb id
            wdt:P8729 # anilist id
            wdt:P345 # imdb id - there's a chance this is a show
            wdt:P4086 # mal id
            wdt:P4947 # tmdb movie id
            wdt:P12196 # tvdb movie id
        }
        ?item ?prop ?id.
    }
    LIMIT 500000
    """

    def __init__(self) -> None:
        """Initialize the source cache.

        Returns:
            None: This function does not return a value.
        """
        self._bindings: list[dict[str, Any]] = []
        self._prepared = False

    async def prepare(self) -> None:
        """Execute the SPARQL query and cache the bindings.

        Returns:
            None: This coroutine does not return a value.
        """
        params = {"query": self.QUERY, "format": "json"}
        headers = {
            "Accept": "application/sparql-results+json",
            "User-Agent": (
                "anibridge-mappings/{} (https://github.com/anibridge/anibridge-mappings)".format(
                    importlib.metadata.version("anibridge-mappings")
                )
            ),
        }
        async with (
            aiohttp.ClientSession(headers=headers) as session,
            session.get(self.ENDPOINT_URL, params=params) as response,
        ):
            response.raise_for_status()
            payload: dict[str, Any] = await response.json()

        bindings = payload.get("results", {}).get("bindings")
        if not isinstance(bindings, list):
            raise RuntimeError("Unexpected Wikidata payload structure")
        self._bindings = bindings
        self._prepared = True

    def build_id_graph(self) -> IdMappingGraph:
        """Convert cached bindings into ID equivalence classes.

        Returns:
            IdMappingGraph: ID mapping graph for Wikidata links.
        """
        self._ensure_prepared()
        # Map Wikidata property codes to local provider names
        prop_map: dict[str, str] = {
            "P5646": "anidb",
            "P8729": "anilist",
            "P4086": "mal",
            "P345": "imdb_movie",
            "P4947": "tmdb_movie",
            "P12196": "tvdb_movie",
        }

        graph = IdMappingGraph()
        # Aggregate nodes by Wikidata item URI
        items: dict[str, list[tuple[str, str, str | None]]] = {}
        for binding in self._bindings:
            item_uri = self._extract_str(binding, "item")
            if not item_uri:
                continue

            prop_code = self._extract_prop_code(binding)
            if not prop_code or prop_code not in prop_map:
                continue

            provider = prop_map[prop_code]
            raw_id = self._extract_str(binding, "id")
            if raw_id is None:
                continue

            # For numeric providers, prefer the last run of digits in the value.
            if provider in {
                "anidb",
                "anilist",
                "imdb_movie",
                "mal",
                "tmdb_movie",
            }:
                m = re.search(r"(\d+)(?!.*\d)", raw_id)
                if not m:
                    continue
                entry_id = m.group(1)
            else:
                entry_id = raw_id

            items.setdefault(item_uri, []).append((provider, entry_id, None))

        for nodes in items.values():
            deduped = list(dict.fromkeys(node for node in nodes if node[1]))
            if len(deduped) >= 2:
                graph.add_equivalence_class(deduped)

        return graph

    def _ensure_prepared(self) -> None:
        """Raise if the source has not been prepared."""
        if not self._prepared:
            raise RuntimeError("Source not initialized.")

    @staticmethod
    def _extract_str(binding: dict[str, Any], key: str) -> str | None:
        """Extract a string value from a Wikidata binding."""
        slot = binding.get(key)
        if not isinstance(slot, dict):
            return None
        raw = slot.get("value")
        if raw is None:
            return None
        text = str(raw).strip()
        return text or None

    @staticmethod
    def _extract_prop_code(binding: dict[str, Any]) -> str | None:
        """Extract the property code from a Wikidata binding."""
        slot = binding.get("prop")
        if not isinstance(slot, dict):
            return None
        raw = slot.get("value")
        if raw is None:
            return None
        text = str(raw)
        # Expect something like https://www.wikidata.org/prop/direct/P8729
        import re

        m = re.search(r"P\d+", text)
        return m.group(0) if m else None
