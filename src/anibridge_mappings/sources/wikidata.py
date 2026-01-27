"""ID source driven by the Wikidata SPARQL endpoint.

This source only collects Anime ID links for movies. Wikidata does not provide clear
mappings for seasons, so this source omits TV show IDs to avoid ambiguity.
"""

import importlib.metadata
import re
from logging import getLogger
from typing import Any

import aiohttp

from anibridge_mappings.core.graph import IdMappingGraph
from anibridge_mappings.sources.base import IdMappingSource

log = getLogger(__name__)


class WikidataSource(IdMappingSource):
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
