"""Metadata provider that fetches TMDB episode counts."""

import asyncio
import os
from logging import getLogger
from typing import Any

import aiohttp
from anibridge.utils.cache import cache

from anibridge_mappings.core.meta import SourceMeta, SourceType, normalize_titles
from anibridge_mappings.sources.base import CachedMetadataSource

log = getLogger(__name__)


class BaseTmdbSource(CachedMetadataSource):
    """Shared base for TMDB metadata sources."""

    API_ROOT = "https://api.themoviedb.org/3"

    async def prepare(self) -> None:
        """Load cache data and validate TMDB authentication configuration."""
        await super().prepare()
        self._require_token()

    @staticmethod
    @cache
    def _get_token() -> str | None:
        """Read the TMDB bearer token from `TMDB_API_KEY`."""
        return os.environ.get("TMDB_API_KEY") or None

    @classmethod
    def _require_token(cls) -> str:
        """Return the configured TMDB token or raise when missing."""
        token = cls._get_token()
        if not token:
            raise RuntimeError("TMDB_API_KEY is required for TMDB metadata fetches")
        return token

    def _session_kwargs(self) -> dict[str, Any]:
        """Return aiohttp session settings for TMDB requests."""
        token = self._require_token()
        return {
            "headers": {
                "Accept": "application/json",
                "Authorization": f"Bearer {token}",
            }
        }

    async def _request_json(
        self,
        session: aiohttp.ClientSession,
        url: str,
        label: str,
    ) -> tuple[dict[str, Any] | None, bool]:
        """Request a TMDB payload with rate-limit handling."""
        while True:
            async with session.get(url) as response:
                if response.status == 429:
                    retry = int(response.headers.get("Retry-After", "2"))
                    log.warning("TMDB rate limit hit for %s; sleeping %s", label, retry)
                    await asyncio.sleep(retry + 1)
                    continue

                if response.status == 404:
                    log.warning("TMDB resource %s not found", label)
                    return None, True

                try:
                    response.raise_for_status()
                except aiohttp.ClientResponseError as exc:
                    log.error("TMDB request failed for %s: %s", label, exc)
                    return None, False

                payload: dict[str, Any] = await response.json()
                return payload, True


class TmdbShowSource(BaseTmdbSource):
    """Collect TMDB episode counts for IDs already present in the ID graph."""

    CACHE_VERSION = 4
    provider_key = "tmdb_show"
    cache_filename = "tmdb_show.json"

    def __init__(self, concurrency: int = 6) -> None:
        """Initialize the TmdbSource with a specific concurrency level."""
        super().__init__(concurrency=concurrency)
        self._show_cache: dict[str, dict[str | None, SourceMeta] | None] = {}

    async def _fetch_entry(
        self,
        session: aiohttp.ClientSession,
        entry_id: str,
        scope: str | None,
    ) -> tuple[str, dict[str | None, SourceMeta] | None, bool]:
        """Fetch TMDB metadata for a single entry."""
        log.debug("Fetching TMDB metadata for %s (season scope: %s)", entry_id, scope)
        scope_meta, cacheable = await self._get_or_fetch_show_meta(session, entry_id)
        return entry_id, scope_meta, cacheable

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
        titles = normalize_titles(
            (
                payload.get("name"),
                payload.get("original_name"),
            )
        )
        scope_meta: dict[str | None, SourceMeta] = {}
        missing_years: list[tuple[str, int]] = []

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
                titles=titles,
            )

            if start_year is None:
                missing_years.append((scope, season_number))

        if missing_years:
            await self._fill_missing_start_years(
                session, base_id, missing_years, scope_meta
            )

        self._show_cache[base_id] = scope_meta
        return scope_meta, cacheable

    async def _request_show_payload(
        self,
        session: aiohttp.ClientSession,
        base_id: str,
    ) -> tuple[dict[str, Any] | None, bool]:
        """Request a TMDB show payload with rate-limit handling."""
        return await self._request_json(
            session, f"{self.API_ROOT}/tv/{base_id}", f"show {base_id}"
        )

    async def _fill_missing_start_years(
        self,
        session: aiohttp.ClientSession,
        base_id: str,
        missing: list[tuple[str, int]],
        scope_meta: dict[str | None, SourceMeta],
    ) -> None:
        """Fetch per-season details to fill missing start_year values."""
        for scope, season_number in missing:
            year = await self._fetch_season_start_year(session, base_id, season_number)
            if year is not None:
                scope_meta[scope].start_year = year

    async def _fetch_season_start_year(
        self,
        session: aiohttp.ClientSession,
        base_id: str,
        season_number: int,
    ) -> int | None:
        """Fetch a season's earliest episode air year from TMDB."""
        url = f"{self.API_ROOT}/tv/{base_id}/season/{season_number}"
        payload, _cacheable = await self._request_json(
            session, url, f"show {base_id}/season/{season_number}"
        )
        if payload is None:
            return None

        min_year: int | None = None
        for ep in payload.get("episodes") or []:
            air_date = ep.get("air_date") or ""
            if air_date[:4].isdigit():
                year = int(air_date[:4])
                if min_year is None or year < min_year:
                    min_year = year
        return min_year

    @staticmethod
    def _scope_from_season(season_number: int) -> str:
        """Format a season number into a scope label."""
        return f"s{season_number}"


class TmdbMovieSource(BaseTmdbSource):
    """Collect TMDB movie metadata for IDs already present in the ID graph."""

    CACHE_VERSION = 1
    provider_key = "tmdb_movie"
    cache_filename = "tmdb_movie.json"

    async def _fetch_entry(
        self,
        session: aiohttp.ClientSession,
        entry_id: str,
        scope: str | None,
    ) -> tuple[str, dict[str | None, SourceMeta] | None, bool]:
        """Fetch TMDB metadata for a single movie entry."""
        del scope
        url = f"{self.API_ROOT}/movie/{entry_id}"
        payload, cacheable = await self._request_json(session, url, f"movie {entry_id}")
        if payload is None:
            return entry_id, None, cacheable

        runtime = payload.get("runtime")
        release_date = payload.get("release_date")
        start_year = (
            int(release_date[:4])
            if release_date and release_date[:4].isdigit()
            else None
        )
        duration = runtime if runtime and runtime > 0 else None
        titles = normalize_titles(
            (
                payload.get("title"),
                payload.get("original_title"),
            )
        )
        return (
            entry_id,
            {
                None: SourceMeta(
                    type=SourceType.MOVIE,
                    episodes=1,
                    duration=duration,
                    start_year=start_year,
                    titles=titles,
                )
            },
            cacheable,
        )
