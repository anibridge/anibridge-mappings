"""Protocols for data source implementations."""

import asyncio
import json
from logging import getLogger
from pathlib import Path
from typing import Any, ClassVar, Protocol, runtime_checkable

import aiohttp

from anibridge_mappings.core.graph import EpisodeMappingGraph, IdMappingGraph
from anibridge_mappings.core.meta import MetaStore, SourceMeta

log = getLogger(__name__)


@runtime_checkable
class BaseSource(Protocol):
    """Base contract shared by all data sources."""

    async def prepare(self) -> None:
        """Fetch or compute any data needed before collection begins."""
        ...


@runtime_checkable
class IdMappingSource(BaseSource, Protocol):
    """Source capable of emitting ID relationships."""

    def build_id_graph(self) -> IdMappingGraph:
        """Return an `IdMappingGraph` constructed from source data.

        Returns:
            IdMappingGraph: ID mapping graph for the source.
        """
        ...


@runtime_checkable
class MetadataSource(BaseSource, Protocol):
    """Source capable of enriching the shared metadata store."""

    async def collect_metadata(self, id_graph: IdMappingGraph) -> MetaStore:
        """Return metadata derived from the source, using `id_graph` if needed.

        Args:
            id_graph (IdMappingGraph): ID graph that may guide lookups.

        Returns:
            MetaStore: Collected metadata.
        """
        ...


@runtime_checkable
class EpisodeMappingSource(BaseSource, Protocol):
    """Source capable of emitting episode mappings."""

    def build_episode_graph(
        self,
        store: MetaStore,
        id_graph: IdMappingGraph,
    ) -> EpisodeMappingGraph:
        """Return an `EpisodeMappingGraph` constructed from source data.

        Args:
            store (MetaStore): Metadata store used for lookups.
            id_graph (IdMappingGraph): ID mapping graph.

        Returns:
            EpisodeMappingGraph: Episode mapping graph for the source.
        """
        ...


class CachedMetadataSource(MetadataSource):
    """Shared cache for serialized metadata providers."""

    CACHE_VERSION: ClassVar[int] = 1
    DATA_DIR: ClassVar[Path] = Path("data/meta")

    provider_key: ClassVar[str]
    cache_filename: ClassVar[str]

    def __init__(self, *, concurrency: int = 1) -> None:
        """Initialize the cached metadata provider.

        Args:
            concurrency (int): Maximum concurrent fetches.
        """
        self._concurrency = max(1, concurrency)
        self._cache: dict[str, dict[str | None, SourceMeta] | None] = {}
        self._prepared = False

    async def prepare(self) -> None:
        """Load or initialize the metadata cache."""
        self._cache = self._load_cache()
        self._prepared = True

    async def collect_metadata(self, id_graph: IdMappingGraph) -> MetaStore:
        """Populate and return a metadata store using cached and fetched data.

        Args:
            id_graph (IdMappingGraph): ID graph used to find eligible IDs.

        Returns:
            MetaStore: Collected metadata.
        """
        self._ensure_prepared()
        entry_ids = self._eligible_ids(id_graph)
        store = MetaStore()
        if not entry_ids:
            return store

        missing: list[tuple[str, str | None]] = []
        for entry_id, scope in entry_ids:
            cache_key = entry_id if scope is None else f"{entry_id}|{scope}"
            base_key = entry_id

            if cache_key in self._cache:
                # Exact scoped cache hit
                self._ingest(store, entry_id, self._cache[cache_key])
                continue

            base_cached = self._cache.get(base_key, None)
            if base_key in self._cache:
                if base_cached is None:
                    # Previously confirmed missing (e.g. 404)
                    self._ingest(store, entry_id, None)
                    continue

                if isinstance(base_cached, dict):
                    if scope is None:
                        self._ingest(store, entry_id, base_cached)
                        continue

                    scoped_meta = base_cached.get(scope)
                    self._ingest(
                        store,
                        entry_id,
                        None if scoped_meta is None else {scope: scoped_meta},
                    )
                    continue

            missing.append((entry_id, scope))

        if missing:
            results = await self._fetch_missing(missing)
            for entry_id, scope_meta, cacheable in results:
                if cacheable:
                    self._cache[entry_id] = scope_meta
                self._ingest(store, entry_id, scope_meta)
            self._persist_cache()

        return store

    def _eligible_ids(self, id_graph: IdMappingGraph) -> list[tuple[str, str | None]]:
        """Return entry IDs in the graph that match the provider key."""
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

    async def _fetch_missing(
        self,
        entry_ids: list[tuple[str, str | None]],
    ) -> list[tuple[str, dict[str | None, SourceMeta] | None, bool]]:
        """Fetch metadata for missing entry IDs."""
        semaphore = asyncio.Semaphore(self._concurrency)
        async with aiohttp.ClientSession(**self._session_kwargs()) as session:
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

    async def _fetch_with_semaphore(
        self,
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        entry_id: str,
        scope: str | None,
    ) -> tuple[str, dict[str | None, SourceMeta] | None, bool]:
        """Fetch one entry while respecting the concurrency semaphore."""
        async with semaphore:
            try:
                return await self._fetch_entry(session, entry_id, scope)
            except Exception as exc:
                log.exception(
                    "Metadata fetch failed for %s (%s): %s", entry_id, scope, exc
                )
                return (entry_id, None, False)

    def _session_kwargs(self) -> dict[str, Any]:
        """Return keyword args for aiohttp session creation."""
        return {}

    def _load_cache(self) -> dict[str, dict[str | None, SourceMeta] | None]:
        """Load cached metadata from disk, if present."""
        path = self.cache_path
        CachedMetadataSource.DATA_DIR.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            return {}
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict) and payload.get("version") == self.CACHE_VERSION:
            entries = payload.get("entries") or {}
            return {
                str(entry_id): (
                    {
                        (None if scope == "" else scope): SourceMeta.from_dict(
                            meta_dict
                        )
                        for scope, meta_dict in scope_map.items()
                    }
                    if isinstance(scope_map, dict)
                    else None
                )
                for entry_id, scope_map in entries.items()
            }
        return self._convert_legacy_payload(payload)

    def _convert_legacy_payload(
        self, payload: object
    ) -> dict[str, dict[str | None, SourceMeta] | None]:
        """Convert legacy cache payloads into the current format."""
        return {}

    def _persist_cache(self) -> None:
        """Persist the in-memory cache to disk."""
        path = self.cache_path
        CachedMetadataSource.DATA_DIR.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "version": self.CACHE_VERSION,
                    "entries": {
                        entry_id: (
                            {
                                ("" if scope is None else scope): meta.to_dict()
                                for scope, meta in scopes.items()
                            }
                            if scopes is not None
                            else None
                        )
                        for entry_id, scopes in self._cache.items()
                    },
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

    @property
    def cache_path(self) -> Path:
        """Return the path to the cache file.

        Returns:
            Path: File path for the cache.
        """
        return CachedMetadataSource.DATA_DIR / self.cache_filename

    async def _fetch_entry(
        self,
        session: aiohttp.ClientSession,
        entry_id: str,
        scope: str | None,
    ) -> tuple[str, dict[str | None, SourceMeta] | None, bool]:
        """Fetch metadata for a single entry ID."""
        raise NotImplementedError

    def _ensure_prepared(self) -> None:
        """Raise if the `prepare` method was skipped."""
        if not self._prepared:
            raise RuntimeError("Source not initialized.")

    def _ingest(
        self,
        store: MetaStore,
        entry_id: str,
        scope_meta: dict[str | None, SourceMeta] | None,
    ) -> None:
        """Merge `scope_meta` into `store` if present."""
        if not scope_meta:
            return
        for scope, meta in scope_meta.items():
            store.set(self.provider_key, entry_id, meta, scope)
