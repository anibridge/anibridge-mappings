"""Module for the manami-project/anime-offline-database source."""

import io
import json
import re
from typing import ClassVar, Literal

import aiohttp
from pydantic import BaseModel, ConfigDict, Field
from zstandard import ZstdDecompressor

from anibridge_mappings.core.graph import IdMappingGraph
from anibridge_mappings.core.meta import MetaStore, SourceType
from anibridge_mappings.sources.base import IdMappingSource, MetadataSource


class AnimeOfflineDatabaseSeason(BaseModel):
    """Data model for a season in the Anime Offline Database."""

    year: int | None = None
    season: Literal["WINTER", "SPRING", "SUMMER", "FALL", "UNDEFINED"] | None = None


class AnimeOfflineDatabaseEntry(BaseModel):
    """Data model for an entry in the Anime Offline Database."""

    sources: list[str]
    title: str
    type: str
    episodes: int | None = None
    anime_season: AnimeOfflineDatabaseSeason | None = Field(
        default=None, alias="animeSeason"
    )

    model_config = ConfigDict(extra="ignore")


class AnimeOfflineDatabaseSource(MetadataSource, IdMappingSource):
    """Source handler for the Anime Offline Database."""

    SOURCE_URL = "https://github.com/manami-project/anime-offline-database/releases/download/latest/anime-offline-database-minified.json.zst"

    _SOURCE_PATTERNS: ClassVar[list[tuple[str, re.Pattern[str]]]] = [
        ("anidb", re.compile(r"^https?://anidb\.net/anime/(\d+)$", re.IGNORECASE)),
        ("anilist", re.compile(r"^https?://anilist\.co/anime/(\d+)$", re.IGNORECASE)),
        ("mal", re.compile(r"^https?://myanimelist\.net/anime/(\d+)$", re.IGNORECASE)),
    ]

    def __init__(self) -> None:
        """Initialize the source."""
        self._entries: list[AnimeOfflineDatabaseEntry] = []
        self._prepared = False

    async def prepare(self) -> None:
        """Download and decompress the upstream dataset."""
        async with (
            aiohttp.ClientSession() as session,
            session.get(self.SOURCE_URL) as response,
        ):
            response.raise_for_status()
            compressed_data = await response.read()

        dctx = ZstdDecompressor()
        with io.BytesIO(compressed_data) as bio, dctx.stream_reader(bio) as reader:
            decompressed_bytes = reader.read()

        payload = json.loads(decompressed_bytes)
        raw_entries = payload.get("data")
        if not isinstance(raw_entries, list):
            raise RuntimeError("Invalid data format.")

        self._entries = [
            AnimeOfflineDatabaseEntry.model_validate(entry) for entry in raw_entries
        ]
        self._prepared = True

    async def collect_metadata(self, id_graph: IdMappingGraph) -> MetaStore:
        """Populate and return a metadata store derived from the dataset.

        Args:
            id_graph (IdMappingGraph): ID graph (unused).

        Returns:
            MetaStore: Collected metadata.
        """
        del id_graph  # Metadata does not depend on graph structure yet.
        store = MetaStore()
        for entry in self._require_entries():
            providers = self._collect_provider_ids(entry)
            if not providers:
                continue

            parsed_type = self._parse_type_string(entry.type) if entry.type else None
            for provider, entry_id, scope in providers:
                meta = store.get(provider, entry_id, scope)
                if parsed_type is not None:
                    meta.type = parsed_type
                if entry.episodes is not None:
                    meta.episodes = entry.episodes
                if entry.anime_season is not None:
                    meta.start_year = entry.anime_season.year
        return store

    def build_id_graph(self) -> IdMappingGraph:
        """Build and return the ID mapping graph.

        Returns:
            IdMappingGraph: ID mapping graph for the dataset.
        """
        graph = IdMappingGraph()
        for entry in self._require_entries():
            providers = [
                (provider, entry_id, scope)
                for provider, entry_id, scope in self._collect_provider_ids(entry)
            ]
            if len(providers) >= 2:
                graph.add_equivalence_class(providers)
        return graph

    def _require_entries(self) -> list[AnimeOfflineDatabaseEntry]:
        """Return cached entries or raise if `prepare` was skipped."""
        if not self._prepared:
            raise RuntimeError("Source not initialized.")
        return self._entries

    def _collect_provider_ids(
        self, entry: AnimeOfflineDatabaseEntry
    ) -> list[tuple[str, str, str | None]]:
        """Return parsed provider/ID tuples for an entry."""
        parsed_sources = [self._parse_source_string(source) for source in entry.sources]
        return [source for source in parsed_sources if source is not None]

    @staticmethod
    def _parse_source_string(source: str) -> tuple[str, str, str | None] | None:
        """Parse a source string into provider and ID."""
        for provider, pattern in AnimeOfflineDatabaseSource._SOURCE_PATTERNS:
            match = pattern.match(source)
            # TODO: could be better - there's a chance of losing s0 anidb mappings here
            if match:
                return provider, match.group(1), None if provider != "anidb" else "s1"
        return None

    @staticmethod
    def _parse_type_string(type_str: str) -> SourceType | None:
        """Parse a type string into a SourceType enum."""
        type_str = type_str.lower()
        if type_str == "movie":
            return SourceType.MOVIE
        if type_str in ("tv", "ova", "ona", "special", "music"):
            return SourceType.TV
        if type_str == "unknown":
            return None
