"""Metadata provider that fetches TMDB episode counts."""

import asyncio
import os
from logging import getLogger
from typing import Any

import aiohttp

from anibridge_mappings.core.meta import SourceMeta, SourceType
from anibridge_mappings.sources.base import CachedMetadataSource

log = getLogger(__name__)


class TmdbSource(CachedMetadataSource):
    """Collect TMDB episode counts for IDs already present in the ID graph."""

    API_ROOT = "https://api.themoviedb.org/3"
    provider_key = "tmdb_show"
    cache_filename = "tmdb_meta.json"

    def __init__(self, concurrency: int = 6) -> None:
        """Initialize the TmdbSource with a specific concurrency level.

        Args:
            concurrency (int): Maximum concurrent fetches.

        Returns:
            None: This function does not return a value.
        """
        super().__init__(concurrency=concurrency)
        self._show_cache: dict[str, dict[str | None, SourceMeta] | None] = {}

    @staticmethod
    def _get_token() -> str:
        """Read the TMDB bearer token from `TMDB_API_KEY`."""
        token = os.environ.get("TMDB_API_KEY")
        if not token:
            raise RuntimeError("TMDB_API_KEY environment variable is required")
        return token

    def _session_kwargs(self) -> dict[str, Any]:
        """Return aiohttp session settings for TMDB requests."""
        return {
            "headers": {
                "Accept": "application/json",
                "Authorization": f"Bearer {self._get_token()}",
            }
        }

    async def _fetch_entry(
        self,
        session: aiohttp.ClientSession,
        entry_id: str,
        scope: str | None,
    ) -> tuple[str, dict[str | None, SourceMeta] | None, bool]:
        """Fetch TMDB metadata for a single entry."""
        log.debug("Fetching TMDB metadata for %s (season scope: %s)", entry_id, scope)
        scope_meta, cacheable = await self._get_or_fetch_show_meta(session, entry_id)
        if scope_meta is None:
            return entry_id, None, cacheable

        filtered = self._subset_scope_meta(scope_meta, scope)
        if not filtered:
            return entry_id, None, True
        return entry_id, filtered, True

    async def _get_or_fetch_show_meta(
        self,
        session: aiohttp.ClientSession,
        base_id: str,
    ) -> tuple[dict[str | None, SourceMeta] | None, bool]:
        """Return cached TMDB metadata or fetch it on demand."""
        if base_id in self._show_cache:
            return self._show_cache[base_id], True

        payload, cacheable = await self._request_show_payload(session, base_id)
        if payload is None:
            self._show_cache[base_id] = None
            return None, cacheable

        seasons = payload.get("seasons") or []
        scope_meta: dict[str | None, SourceMeta] = {}

        for season in seasons:
            season_number = season.get("season_number")
            if season_number is None:
                continue

            scope = self._scope_from_season(season_number)
            episode_count = season.get("episode_count") or 0
            if episode_count <= 0:
                continue

            first_air = season.get("air_date") or ""
            start_year = int(first_air[:4]) if first_air[:4].isdigit() else None

            scope_meta[scope] = SourceMeta(
                type=SourceType.TV,
                episodes=episode_count,
                start_year=start_year,
            )

        self._show_cache[base_id] = scope_meta
        return scope_meta, cacheable

    async def _request_show_payload(
        self,
        session: aiohttp.ClientSession,
        base_id: str,
    ) -> tuple[dict[str, Any] | None, bool]:
        """Request a TMDB show payload with rate-limit handling."""
        url = f"{self.API_ROOT}/tv/{base_id}"
        while True:
            async with session.get(url) as response:
                if response.status == 429:
                    retry = int(response.headers.get("Retry-After", "2"))
                    log.warning(
                        "TMDB rate limit hit for %s; sleeping %s", base_id, retry
                    )
                    await asyncio.sleep(retry + 1)
                    continue

                if response.status == 404:
                    log.warning("TMDB show %s not found", base_id)
                    return None, True

                try:
                    response.raise_for_status()
                except aiohttp.ClientResponseError as exc:
                    log.error("TMDB request failed for %s: %s", base_id, exc)
                    return None, False

                payload: dict[str, Any] = await response.json()
                return payload, True

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
