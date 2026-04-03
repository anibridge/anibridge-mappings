import asyncio
import contextlib

import orjson

from anibridge_mappings.core.graph import IdMappingGraph
from anibridge_mappings.core.meta import SourceMeta, SourceType
from anibridge_mappings.sources.base import CachedMetadataSource


class DummyCachedSource(CachedMetadataSource):
    provider_key = "dummy"
    cache_filename = "dummy.json"

    async def _fetch_entry(self, session, entry_id, scope):
        raise AssertionError("_fetch_entry should not be called in this test")

    async def _fetch_missing(self, entry_ids):
        return [
            (
                entry_id,
                {scope: SourceMeta(type=SourceType.TV, episodes=12)},
                True,
            )
            for entry_id, scope in entry_ids
        ]


def test_cached_metadata_source_collect_metadata_roundtrip(tmp_path) -> None:
    CachedMetadataSource.DATA_DIR = tmp_path
    source = DummyCachedSource()

    id_graph = IdMappingGraph()
    id_graph.add_edge(("dummy", "1", None), ("other", "2", None))

    # Collecting before prepare should fail.
    with contextlib.suppress(RuntimeError):
        _ = source._ensure_prepared()

    asyncio.run(source.prepare())
    store = asyncio.run(source.collect_metadata(id_graph))

    meta = store.peek("dummy", "1", None)
    assert meta is not None
    assert meta.episodes == 12

    cache = source._load_cache()
    assert "1" in cache


def test_cached_metadata_legacy_payload_converter_default() -> None:
    source = DummyCachedSource()
    assert source._convert_legacy_payload({"v": 1}) == {}


def test_cached_metadata_eligible_ids_and_cache_path(tmp_path) -> None:
    CachedMetadataSource.DATA_DIR = tmp_path
    source = DummyCachedSource()

    id_graph = IdMappingGraph()
    id_graph.add_edge(("dummy", "1", None), ("other", "2", None))
    id_graph.add_edge(("dummy", "3", "s1"), ("other", "4", None))

    assert source._eligible_ids(id_graph) == [("1", None), ("3", "s1")]
    assert source.cache_path.name == "dummy.json"


def test_cached_metadata_loads_versioned_payload(tmp_path) -> None:
    CachedMetadataSource.DATA_DIR = tmp_path
    source = DummyCachedSource()

    payload = {
        "version": source.CACHE_VERSION,
        "entries": {
            "1": {
                "": {
                    "type": "tv",
                    "episodes": 2,
                    "duration": 24,
                    "start_year": 2020,
                    "titles": ["Cached Title"],
                }
            },
            "2": None,
        },
    }
    source.cache_path.write_bytes(orjson.dumps(payload))

    loaded = source._load_cache()
    loaded_1 = loaded["1"]
    assert loaded_1 is not None and loaded_1[None].episodes == 2
    assert loaded_1 is not None and loaded_1[None].titles == ("Cached Title",)
    assert loaded["2"] is None


def test_cached_metadata_fetch_with_semaphore_handles_exceptions() -> None:
    source = DummyCachedSource()

    async def _boom(session, entry_id, scope):
        del session, entry_id, scope
        raise RuntimeError("boom")

    source._fetch_entry = _boom  # type: ignore
    semaphore = asyncio.Semaphore(1)

    class _FakeSession:
        pass

    result = asyncio.run(
        source._fetch_with_semaphore(_FakeSession(), semaphore, "1", None)  # type: ignore
    )
    assert result == ("1", None, False)
