"""Metadata provider that fetches TVDB episode counts via Sonarr Skyhook."""

import asyncio
from logging import getLogger
from typing import Any

import aiohttp

from anibridge_mappings.core.meta import SourceMeta, SourceType
from anibridge_mappings.sources.base import CachedMetadataSource

log = getLogger(__name__)


class SkyhookSource(CachedMetadataSource):
    """Collect TVDB episode counts for IDs already present in the ID graph."""

    API_ROOT = "http://skyhook.sonarr.tv/v1/tvdb/shows/en"
    provider_key = "tvdb_show"
    cache_filename = "tvdb_meta.json"

    def __init__(self, concurrency: int = 6) -> None:
        """Initialize the SkyhookSource with a specific concurrency level.

        Args:
            concurrency (int): Maximum concurrent fetches.

        Returns:
            None: This function does not return a value.
        """
        super().__init__(concurrency=concurrency)
        self._show_cache: dict[str, dict[str | None, SourceMeta] | None] = {}

    async def _fetch_entry(
        self,
        session: aiohttp.ClientSession,
        entry_id: str,
        scope: str | None,
    ) -> tuple[str, dict[str | None, SourceMeta] | None, bool]:
        """Fetch TVDB metadata for a single entry."""
        log.debug("Fetching TVDB metadata for %s (season scope: %s)", entry_id, scope)
        scope_meta, cacheable = await self._get_or_fetch_show_meta(session, entry_id)
        return entry_id, scope_meta, cacheable

    async def _get_or_fetch_show_meta(
        self,
        session: aiohttp.ClientSession,
        base_id: str,
    ) -> tuple[dict[str | None, SourceMeta] | None, bool]:
        """Return cached TVDB metadata or fetch it on demand."""
        if base_id in self._show_cache:
            return self._show_cache[base_id], True

        payload, cacheable = await self._request_show_payload(session, base_id)
        if payload is None:
            self._show_cache[base_id] = None
            return None, cacheable

        scope_meta = self._build_scope_meta(payload)
        self._show_cache[base_id] = scope_meta
        return scope_meta, cacheable

    async def _request_show_payload(
        self,
        session: aiohttp.ClientSession,
        base_id: str,
    ) -> tuple[dict[str, Any] | None, bool]:
        """Request a TVDB show payload with rate-limit handling."""
        url = f"{self.API_ROOT}/{base_id}"
        while True:
            async with session.get(url) as response:
                if response.status == 429:
                    retry = int(response.headers.get("Retry-After", "2"))
                    log.warning(
                        "TVDB rate limit hit for %s; sleeping %s", base_id, retry
                    )
                    await asyncio.sleep(retry + 1)
                    continue

                if response.status == 404:
                    log.warning("TVDB show %s not found", base_id)
                    return None, True

                try:
                    response.raise_for_status()
                except aiohttp.ClientResponseError as exc:
                    log.error("TVDB request failed for %s: %s", base_id, exc)
                    # Keep failures as empty mapping
                    return None, False

                payload: dict[str, Any] = await response.json()
                return payload, True

    def _build_scope_meta(
        self, payload: dict[str, Any]
    ) -> dict[str | None, SourceMeta]:
        """Build per-season metadata from the show payload."""
        episodes = payload.get("episodes") or []
        counts: dict[int, int] = {}
        air_years: dict[int, int] = {}
        for episode in episodes:
            season_number = episode.get("seasonNumber")
            if season_number is None:
                continue
            counts[season_number] = counts.get(season_number, 0) + 1
            air_year = (episode.get("airDateUtc") or "")[:4]
            if air_year.isdigit():
                int_year = int(air_year)
                air_years[season_number] = min(
                    air_years.get(season_number, int_year), int_year
                )

        runtime = payload.get("runtime")
        normalized_runtime = int(runtime) if runtime is not None else None
        return {
            self._scope_from_season(number): SourceMeta(
                type=SourceType.TV,
                episodes=count,
                start_year=air_years.get(number),
                duration=normalized_runtime,
            )
            for number, count in counts.items()
            if count > 0
        }

    @staticmethod
    def _scope_from_season(season_number: int) -> str:
        """Format a season number into a scope label."""
        return f"s{season_number}"

    @staticmethod
    def _subset_scope_meta(
        scope_meta: dict[str | None, SourceMeta], scope: str | None
    ) -> dict[str | None, SourceMeta] | None:
        """Filter scope metadata to a single scope when requested."""
        if scope is None:
            return scope_meta
        meta = scope_meta.get(scope)
        if meta is None:
            return None
        return {scope: meta}
