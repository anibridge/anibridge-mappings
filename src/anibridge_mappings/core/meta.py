"""Metadata structures and store definitions."""

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class SourceType(StrEnum):
    """Enumeration of source types."""

    TV = "tv"
    MOVIE = "movie"


@dataclass(slots=True)
class SourceMeta:
    """Metadata about a source."""

    type: SourceType | None = None
    episodes: int | None = None
    duration: int | None = None  # in seconds
    start_year: int | None = None

    def to_dict(self, include_none: bool = True) -> dict[str, Any]:
        """Serialize the metadata into a JSON-friendly dictionary.

        Args:
            include_none (bool): Whether to keep keys with `None` values.

        Returns:
            dict[str, Any]: JSON-friendly metadata.
        """
        payload: dict[str, Any] = {
            "type": self.type.value if self.type else None,
            "episodes": self.episodes,
            "duration": self.duration,
            "start_year": self.start_year,
        }
        if include_none:
            return payload
        return {key: value for key, value in payload.items() if value is not None}

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> SourceMeta:
        """Deserialize a `SourceMeta` from a dictionary representation.

        Args:
            payload (Mapping[str, Any]): Serialized metadata dictionary.

        Returns:
            SourceMeta: Parsed metadata instance.
        """
        raw_type = payload.get("type")
        parsed_type = SourceType(raw_type) if raw_type else None
        return cls(
            type=parsed_type,
            episodes=payload.get("episodes"),
            duration=payload.get("duration"),
            start_year=payload.get("start_year"),
        )


class MetaStore:
    """In-memory store for per-entry metadata."""

    def __init__(self) -> None:
        """Initialize the metadata store."""
        self._store: dict[tuple[str, str, str | None], SourceMeta] = {}

    def _key(
        self, provider: str, entry_id: str, scope: str | None
    ) -> tuple[str, str, str | None]:
        """Return a normalized key tuple for metadata lookups."""
        return (provider, entry_id, scope)

    def get(
        self,
        provider: str,
        entry_id: str,
        scope: str | None = None,
    ) -> SourceMeta:
        """Return metadata for a provider/entry/scope creating it when absent.

        Args:
            provider (str): Provider namespace (e.g., "anidb").
            entry_id (str): Identifier within the provider namespace.
            scope (str | None): Optional season/scope identifier.

        Returns:
            SourceMeta: Mutable metadata instance for the entry.
        """
        return self._store.setdefault(
            self._key(provider, entry_id, scope), SourceMeta()
        )

    def peek(
        self,
        provider: str,
        entry_id: str,
        scope: str | None = None,
    ) -> SourceMeta | None:
        """Return metadata when present without creating placeholder entries.

        Args:
            provider (str): Provider namespace.
            entry_id (str): Identifier within the provider namespace.
            scope (str | None): Optional season/scope identifier.

        Returns:
            SourceMeta | None: Stored metadata or `None` when missing.
        """
        return self._store.get(self._key(provider, entry_id, scope))

    def set(
        self,
        provider: str,
        entry_id: str,
        meta: SourceMeta,
        scope: str | None = None,
    ) -> None:
        """Replace metadata for the given identifier.

        Args:
            provider (str): Provider namespace.
            entry_id (str): Identifier within the provider namespace.
            meta (SourceMeta): Metadata object to store verbatim.
            scope (str | None): Optional season/scope identifier.
        """
        self._store[self._key(provider, entry_id, scope)] = meta

    def update(
        self,
        provider: str,
        entry_id: str,
        scope: str | None = None,
        **values: int | SourceType | None,
    ) -> SourceMeta:
        """Update selected fields for an entry and return the stored object.

        Args:
            provider (str): Provider namespace.
            entry_id (str): Identifier within the provider namespace.
            scope (str | None): Optional season/scope identifier.
            **values (int | SourceType | None): Field overrides by attribute name.

        Returns:
            SourceMeta: The updated metadata instance.
        """
        meta = self.get(provider, entry_id, scope)
        for field_name, field_value in values.items():
            if hasattr(meta, field_name):
                setattr(meta, field_name, field_value)
        return meta

    def items(self) -> list[tuple[tuple[str, str, str | None], SourceMeta]]:
        """Return all stored metadata entries.

        Returns:
            list[tuple[tuple[str, str, str | None], SourceMeta]]: Stored entries.
        """
        return list(self._store.items())

    def merge(self, other: MetaStore) -> None:
        """Merge another `MetaStore` into this instance.

        Args:
            other (MetaStore): Store whose entries should merge into this one.
        """
        self._store.update(other._store)

    def __len__(self) -> int:
        """Return the number of stored metadata entries.

        Returns:
            int: Number of entries.
        """
        return len(self._store)
