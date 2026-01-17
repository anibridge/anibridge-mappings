"""ID source driven by the Wikidata SPARQL endpoint."""

import importlib.metadata
from logging import getLogger
from typing import Any

import aiohttp

from anibridge_mappings.core.graph import IdMappingGraph
from anibridge_mappings.sources.base import IdMappingSource

log = getLogger(__name__)


class WikidataSource(IdMappingSource):
    """Emit AniList-centered ID links derived from Wikidata."""

    ENDPOINT_URL = "https://query.wikidata.org/sparql"
    QUERY = """
    SELECT DISTINCT ?item ?prop ?id WHERE {
        ?item (p:P31/ps:P31/(wdt:P279*)) wd:Q1107.

        VALUES ?prop {
            wdt:P5646 wdt:P8729 wdt:P4086 wdt:P345
            wdt:P4947 wdt:P4983 wdt:P4835
        }
        ?item ?prop ?id.
    }
    LIMIT 50000
    """
    DEFAULT_SCOPE = "s1"

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
        # Map Wikidata property codes to local provider names and scope
        prop_map: dict[str, tuple[str, bool]] = {
            "P5646": ("anidb", False),
            "P8729": ("anilist", False),
            "P4086": ("mal", False),
            "P345": ("imdb", False),
            "P4947": ("tmdb_movie", False),
            "P4983": ("tmdb_show", True),
            "P4835": ("tvdb_show", True),
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

            provider, is_scoped = prop_map[prop_code]
            raw_id = self._extract_str(binding, "id")
            if raw_id is None:
                continue

            # For numeric providers, prefer the last run of digits in the value.
            if provider in {
                "anidb",
                "anilist",
                "mal",
                "tmdb_movie",
                "tmdb_show",
                "tvdb_show",
            }:
                import re

                m = re.search(r"(\d+)(?!.*\d)", raw_id)
                if not m:
                    continue
                entry_id = m.group(1)
            else:
                entry_id = raw_id

            scope = WikidataSource.DEFAULT_SCOPE if is_scoped else None
            items.setdefault(item_uri, []).append((provider, entry_id, scope))

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
