"""Metadata provider that fetches AniList episode counts."""

import asyncio
from logging import getLogger
from typing import Any

import aiohttp

from anibridge_mappings.core.meta import SourceMeta, SourceType
from anibridge_mappings.sources.base import CachedMetadataSource

log = getLogger(__name__)


class AnilistSource(CachedMetadataSource):
    """Collect AniList episode counts via batched GraphQL queries."""

    API_URL = "https://graphql.anilist.co"
    BATCH_SIZE = 50

    provider_key = "anilist"
    cache_filename = "anilist_meta.json"

    def __init__(self, batch_size: int = BATCH_SIZE) -> None:
        """Initialize the AnilistSource.

        Args:
            batch_size (int): Maximum IDs per AniList page query.
        """
        super().__init__(concurrency=1)
        self._batch_size = max(1, batch_size)

    def _session_kwargs(self) -> dict[str, Any]:
        """Return aiohttp session settings for AniList requests."""
        return {
            "headers": {
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        }

    async def _fetch_missing(
        self,
        entry_ids: list[tuple[str, str | None]],
    ) -> list[tuple[str, dict[str | None, SourceMeta] | None, bool]]:
        """Fetch AniList metadata for the given IDs using alias batching."""
        if not entry_ids:
            return []

        # Deduplicate by base ID; scope does not change AniList metadata fetch.
        base_ids = sorted({entry_id for entry_id, _scope in entry_ids})

        # Split IDs into simple batches that match AniList perPage constraints.
        simple_batches: list[list[str]] = [
            base_ids[i : i + self._batch_size]
            for i in range(0, len(base_ids), self._batch_size)
        ]

        # How many `Page` aliases to pack into a single HTTP request.
        # Each alias corresponds to one simple batch.
        pages_per_request = 70

        results: list[tuple[str, dict[str | None, SourceMeta] | None, bool]] = []

        async with aiohttp.ClientSession(**self._session_kwargs()) as session:
            # Process groups of simple_batches per HTTP request.
            for start in range(0, len(simple_batches), pages_per_request):
                multi_batches = simple_batches[start : start + pages_per_request]

                # Build variable definitions and values.
                var_defs = ["$perPage: Int!"]
                variables: dict[str, Any] = {"perPage": self._batch_size}
                query_sections: list[str] = []

                # For each batch of IDs, create an aliased Page block.
                for idx, batch in enumerate(multi_batches):
                    alias = f"batch{idx + 1}"
                    ids_var = f"ids_{idx + 1}"

                    var_defs.append(f"${ids_var}: [Int!]!")
                    variables[ids_var] = [int(eid) for eid in batch]

                    query_sections.append(
                        f"""
                        {alias}: Page(page: 1, perPage: $perPage) {{
                            media(id_in: ${ids_var}, type: ANIME) {{
                                id
                                episodes
                                format
                                seasonYear
                                duration
                            }}
                        }}
                        """
                    )

                query = f"""
                query ({", ".join(var_defs)}) {{
                    {" ".join(query_sections)}
                }}
                """

                payload = {
                    "query": query,
                    "variables": variables,
                }

                # Single HTTP request with basic rate-limit handling.
                while True:
                    async with session.post(self.API_URL, json=payload) as response:
                        if response.status == 429:
                            retry = int(response.headers.get("Retry-After", "60"))
                            log.warning(
                                "AniList rate limit hit; sleeping %s seconds", retry
                            )
                            await asyncio.sleep(retry + 1)
                            continue
                        response.raise_for_status()
                        raw: dict[str, Any] = await response.json()
                    break

                data = raw.get("data", {}) or {}

                # For each alias/batch, map IDs back to entries and build SourceMeta.
                for idx, batch in enumerate(multi_batches):
                    alias = f"batch{idx + 1}"
                    media_entries = data.get(alias, {}).get("media", []) or []

                    by_id = {
                        str(entry.get("id")): entry
                        for entry in media_entries
                        if isinstance(entry, dict) and entry.get("id") is not None
                    }

                    for entry_id in batch:
                        entry = by_id.get(entry_id)

                        if not entry:
                            results.append((entry_id, None, True))
                            continue

                        episodes = entry.get("episodes")
                        entry_format = entry.get("format")
                        media_type = (
                            SourceType.MOVIE
                            if entry_format in ("MOVIE", "MUSIC")
                            else SourceType.TV
                            if entry_format
                            in ("TV", "TV_SHORT", "OVA", "ONA", "SPECIAL")
                            else None
                        )

                        if isinstance(episodes, int) and episodes > 0:
                            scope_meta: dict[str | None, SourceMeta] | None = {
                                None: SourceMeta(
                                    type=media_type,
                                    episodes=episodes,
                                    start_year=entry.get("seasonYear"),
                                    duration=entry.get("duration"),
                                )
                            }
                        else:
                            # No valid episode count → treat as null mapping
                            scope_meta = None

                        results.append((entry_id, scope_meta, True))

        return results
