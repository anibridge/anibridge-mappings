"""Metadata providers that fetch TVDB metadata."""

import asyncio
import os
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from logging import getLogger
from typing import Any

import aiohttp
from anibridge.utils.cache import cache

from anibridge_mappings.core.meta import SourceMeta, SourceType, normalize_titles
from anibridge_mappings.sources.base import CachedMetadataSource

log = getLogger(__name__)


class BaseTvdbSource(CachedMetadataSource):
    """Base metadata source for TVDB requests."""

    API_ROOT = "https://api4.thetvdb.com/v4"
    LOGIN_ENDPOINT = f"{API_ROOT}/login"
    API_KEY_ENV = "TVDB_API_KEY"
    API_PIN_ENV = "TVDB_PIN"
    RECENT_AIR_DAYS = 180
    CACHE_VERSION = 4

    _ALLOWED_ALIAS_LANGUAGES = frozenset({"eng", "jpn", "zho"})

    def __init__(self, concurrency: int = 6) -> None:
        """Initialize the TVDB source with a specific concurrency level.

        Args:
            concurrency (int): Maximum concurrent fetches.

        Returns:
            None: This function does not return a value.
        """
        super().__init__(concurrency=concurrency)
        self._token: str | None = None

    async def prepare(self) -> None:
        """Load cache data and validate TVDB authentication."""
        await super().prepare()
        async with aiohttp.ClientSession() as session:
            await self._get_or_fetch_token(session)

    @classmethod
    @cache
    def _get_api_key(cls) -> str | None:
        """Read the TVDB API key from the environment."""
        return os.environ.get(cls.API_KEY_ENV)

    @classmethod
    @cache
    def _get_pin(cls) -> str | None:
        """Read the optional TVDB PIN from the environment."""
        return os.environ.get(cls.API_PIN_ENV)

    async def _fetch_missing(
        self,
        entry_ids: list[tuple[str, str | None]],
    ) -> list[tuple[str, dict[str | None, SourceMeta] | None, bool]]:
        async with aiohttp.ClientSession() as session:
            token = await self._get_or_fetch_token(session)

        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        }
        semaphore = asyncio.Semaphore(self._concurrency)
        async with aiohttp.ClientSession(headers=headers) as session:
            return await asyncio.gather(
                *(
                    self._fetch_with_semaphore(
                        session,
                        semaphore,
                        entry_id,
                        scope,
                    )
                    for entry_id, scope in entry_ids
                )
            )

    async def _get_or_fetch_token(self, session: aiohttp.ClientSession) -> str:
        """Login to TVDB and return a bearer token."""
        if self._token:
            return self._token

        api_key = self._get_api_key()
        if not api_key:
            raise RuntimeError("TVDB_API_KEY is required for TVDB metadata fetches")

        payload: dict[str, Any] = {"apikey": api_key}
        pin = self._get_pin()
        if pin:
            payload["pin"] = pin

        while True:
            async with session.post(self.LOGIN_ENDPOINT, json=payload) as response:
                if response.status == 429:
                    retry = int(response.headers.get("Retry-After", "2"))
                    log.warning("TVDB auth rate limit hit; sleeping %s", retry)
                    await asyncio.sleep(retry + 1)
                    continue

                try:
                    response.raise_for_status()
                except aiohttp.ClientResponseError as exc:
                    raise RuntimeError(f"TVDB login failed: {exc}") from exc

                payload_data: dict[str, Any] = await response.json()
                token = (payload_data.get("data") or {}).get("token")
                if not token:
                    raise RuntimeError("TVDB login response missing token")
                self._token = token
                return token

    async def _request_json(
        self,
        session: aiohttp.ClientSession,
        url: str,
    ) -> tuple[dict[str, Any] | None, bool]:
        """Request a TVDB payload with rate-limit handling."""
        while True:
            async with session.get(url) as response:
                if response.status == 429:
                    retry = int(response.headers.get("Retry-After", "2"))
                    log.warning("TVDB rate limit hit for %s; sleeping %s", url, retry)
                    await asyncio.sleep(retry + 1)
                    continue

                if response.status == 404:
                    log.warning("TVDB resource not found: %s", url)
                    return None, True

                try:
                    response.raise_for_status()
                except aiohttp.ClientResponseError as exc:
                    log.error("TVDB request failed for %s: %s", url, exc)
                    return None, False

                payload: dict[str, Any] = await response.json()
                return payload, True

    @staticmethod
    def _scope_from_season(season_number: int) -> str:
        """Format a season number into a scope label."""
        return f"s{season_number}"

    @staticmethod
    def _parse_runtime(value: int | None) -> int | None:
        """Return the runtime if positive, otherwise None."""
        return value if value and value > 0 else None

    @staticmethod
    def _parse_year(value: str | None) -> int | None:
        """Parse a year from a TVDB date string."""
        if value and value[:4].isdigit():
            return int(value[:4])
        return None

    def _build_show_scope_meta(
        self,
        episodes: Iterable[dict[str, Any]],
        runtime: int | None,
        titles: Iterable[object] = (),
    ) -> dict[str | None, SourceMeta]:
        """Build per-season metadata from TVDB episodes."""
        counts: dict[int, int] = {}
        air_years: dict[int, int] = {}
        last_air_dates: dict[int, datetime] = {}
        has_finale: dict[int, bool] = {}
        for episode in episodes:
            season_number = self._extract_season_number(episode)
            if season_number is None:
                continue
            counts[season_number] = counts.get(season_number, 0) + 1
            air_year = self._extract_air_year(episode)
            if air_year is not None:
                air_years[season_number] = min(
                    air_years.get(season_number, air_year),
                    air_year,
                )

            air_date = self._extract_air_date(episode)
            if air_date is not None:
                current_last = last_air_dates.get(season_number)
                if current_last is None or air_date > current_last:
                    last_air_dates[season_number] = air_date

            finale_type = self._extract_finale_type(episode)
            if finale_type:
                has_finale[season_number] = True

        normalized_runtime = self._parse_runtime(runtime)
        normalized_titles = normalize_titles(titles)
        recent_cutoff = datetime.now(UTC) - timedelta(days=self.RECENT_AIR_DAYS)

        scope_meta: dict[str | None, SourceMeta] = {}
        for number, count in counts.items():
            if count <= 0:
                continue

            last_air = last_air_dates.get(number)
            complete = bool(has_finale.get(number)) or (
                last_air is not None and last_air < recent_cutoff
            )
            episode_total = count if complete else None

            scope_meta[self._scope_from_season(number)] = SourceMeta(
                type=SourceType.TV,
                episodes=episode_total,
                start_year=air_years.get(number),
                duration=normalized_runtime,
                titles=normalized_titles,
            )

        return scope_meta

    @staticmethod
    def _extract_season_number(episode: dict[str, Any]) -> int | None:
        """Extract a season number from a TVDB episode entry."""
        return episode.get("seasonNumber")

    @staticmethod
    def _extract_air_year(episode: dict[str, Any]) -> int | None:
        """Extract a year from a TVDB episode entry."""
        aired = episode.get("aired")
        if aired and aired[:4].isdigit():
            return int(aired[:4])
        return None

    @staticmethod
    def _extract_air_date(episode: dict[str, Any]) -> datetime | None:
        """Extract a UTC datetime from a TVDB episode entry."""
        aired = episode.get("aired")
        if not aired:
            return None

        text = aired.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            try:
                parsed = datetime.strptime(text[:10], "%Y-%m-%d")
            except ValueError:
                return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    @staticmethod
    def _extract_finale_type(episode: dict[str, Any]) -> str | None:
        """Extract a finale type label from a TVDB episode entry."""
        raw = episode.get("finaleType")
        if not raw:
            return None
        text = raw.strip().lower()
        if text in {"season", "series"}:
            return text
        return None

    def _build_movie_meta(
        self,
        runtime: int | None,
        release_date: str | None,
        titles: Iterable[object] = (),
    ) -> SourceMeta:
        """Build a movie metadata payload from TVDB fields."""
        return SourceMeta(
            type=SourceType.MOVIE,
            episodes=1,
            duration=self._parse_runtime(runtime),
            start_year=self._parse_year(release_date),
            titles=normalize_titles(titles),
        )

    @staticmethod
    def _extract_titles(data: dict[str, Any]) -> tuple[str, ...]:
        """Extract a best-effort list of title variants from a TVDB payload."""
        candidates: list[str] = []
        name = data.get("name")
        if name:
            candidates.append(name)
        for alias in data.get("aliases") or []:
            language = alias.get("language")
            if language and language not in BaseTvdbSource._ALLOWED_ALIAS_LANGUAGES:
                continue
            alias_name = alias.get("name")
            if alias_name:
                candidates.append(alias_name)
        return normalize_titles(candidates)


class TvdbShowSource(BaseTvdbSource):
    """Collect TVDB episode counts for IDs already present in the ID graph."""

    provider_key = "tvdb_show"
    cache_filename = "tvdb_show.json"

    def __init__(self, concurrency: int = 6) -> None:
        """Initialize the TVDB show source with a specific concurrency level.

        Args:
            concurrency (int): Maximum concurrent fetches.
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

        data = payload.get("data")
        if not data:
            self._show_cache[base_id] = None
            return None, cacheable

        episodes = data.get("episodes")
        runtime = data.get("averageRuntime") or data.get("runtime")
        scope_meta = self._build_show_scope_meta(
            episodes or [],
            runtime,
            self._extract_titles(data),
        )
        self._show_cache[base_id] = scope_meta
        return scope_meta, cacheable

    async def _request_show_payload(
        self,
        session: aiohttp.ClientSession,
        base_id: str,
    ) -> tuple[dict[str, Any] | None, bool]:
        """Request TVDB show payload with embedded episodes."""
        url = f"{self.API_ROOT}/series/{base_id}/extended?meta=episodes&short=true"
        return await self._request_json(session, url)


class TvdbMovieSource(BaseTvdbSource):
    """Collect TVDB movie metadata for IDs already present in the ID graph."""

    provider_key = "tvdb_movie"
    cache_filename = "tvdb_movie.json"

    async def _fetch_entry(
        self,
        session: aiohttp.ClientSession,
        entry_id: str,
        scope: str | None,
    ) -> tuple[str, dict[str | None, SourceMeta] | None, bool]:
        """Fetch TVDB movie metadata for a single entry."""
        del scope
        log.debug("Fetching TVDB movie metadata for %s", entry_id)
        payload, cacheable = await self._request_movie_payload(session, entry_id)
        if payload is None:
            return entry_id, None, cacheable

        data = payload["data"]

        runtime = data.get("runtime")
        release_date = data.get("year")

        meta = self._build_movie_meta(
            runtime,
            release_date,
            self._extract_titles(data),
        )
        return entry_id, {None: meta}, cacheable

    async def _request_movie_payload(
        self,
        session: aiohttp.ClientSession,
        base_id: str,
    ) -> tuple[dict[str, Any] | None, bool]:
        """Request a TVDB movie payload with rate-limit handling."""
        url = f"{self.API_ROOT}/movies/{base_id}/extended"
        return await self._request_json(session, url)
