import asyncio

import pytest

from anibridge_mappings.core.meta import SourceMeta
from anibridge_mappings.sources.tmdb import TmdbSource


def test_tmdb_scope_helpers() -> None:
    assert TmdbSource._scope_from_season(4) == "s4"

    meta = SourceMeta(episodes=10)
    all_scopes: dict[str | None, SourceMeta] = {
        "s1": meta,
        "s2": SourceMeta(episodes=5),
    }
    assert TmdbSource._subset_scope_meta(all_scopes, None) == all_scopes
    assert TmdbSource._subset_scope_meta(all_scopes, "s1") == {"s1": meta}
    assert TmdbSource._subset_scope_meta(all_scopes, "missing") is None


def test_tmdb_session_kwargs_without_token_raises(monkeypatch) -> None:
    monkeypatch.delenv("TMDB_API_KEY", raising=False)
    source = TmdbSource()
    with pytest.raises(RuntimeError, match="TMDB_API_KEY is required"):
        source._session_kwargs()


def test_tmdb_fetch_missing_without_token_raises(monkeypatch) -> None:
    monkeypatch.delenv("TMDB_API_KEY", raising=False)
    source = TmdbSource()

    with pytest.raises(RuntimeError, match="TMDB_API_KEY is required"):
        asyncio.run(source._fetch_missing([("1", None), ("2", "s1")]))


def test_tmdb_prepare_requires_token(monkeypatch) -> None:
    monkeypatch.delenv("TMDB_API_KEY", raising=False)
    source = TmdbSource()

    with pytest.raises(RuntimeError, match="TMDB_API_KEY is required"):
        asyncio.run(source.prepare())


def test_tmdb_get_or_fetch_show_meta_uses_cache_and_parses_seasons(monkeypatch) -> None:
    source = TmdbSource()

    async def _fake_request(session, base_id):
        del session
        assert base_id == "55"
        return (
            {
                "name": "Show Name",
                "original_name": "Show Name Original",
                "seasons": [
                    {"season_number": 1, "episode_count": 12, "air_date": "2020-01-01"},
                    {"season_number": 2, "episode_count": 0, "air_date": "2021-01-01"},
                ],
            },
            True,
        )

    monkeypatch.setattr(source, "_request_show_payload", _fake_request)

    class _FakeSession:
        pass

    scope_meta, cacheable = asyncio.run(
        source._get_or_fetch_show_meta(_FakeSession(), "55")  # type: ignore
    )
    assert cacheable is True
    assert scope_meta is not None and scope_meta["s1"].episodes == 12
    assert scope_meta["s1"].start_year == 2020
    assert scope_meta["s1"].titles == ("Show Name", "Show Name Original")
    assert scope_meta is not None and "s2" not in scope_meta

    # second call should hit in-memory cache
    cached_scope_meta, cached_cacheable = asyncio.run(
        source._get_or_fetch_show_meta(_FakeSession(), "55")  # type: ignore
    )
    assert cached_cacheable is True
    assert cached_scope_meta == scope_meta


def test_tmdb_fetch_entry_returns_scope_meta_bundle(monkeypatch) -> None:
    source = TmdbSource()

    async def _fake_get_or_fetch(session, base_id):
        del session, base_id
        return ({"s1": SourceMeta(episodes=8), "s2": SourceMeta(episodes=10)}, True)

    monkeypatch.setattr(source, "_get_or_fetch_show_meta", _fake_get_or_fetch)

    class _FakeSessionFetch:
        pass

    entry_id, scoped, cacheable = asyncio.run(
        source._fetch_entry(_FakeSessionFetch(), "9", "s2")  # type: ignore
    )
    assert entry_id == "9"
    assert cacheable is True
    assert scoped == {"s1": SourceMeta(episodes=8), "s2": SourceMeta(episodes=10)}
