# Shinkro

## Data Sources

| Source   | URL                                                                                | Format |
| -------- | ---------------------------------------------------------------------------------- | ------ |
| TVDB-MAL | `https://raw.githubusercontent.com/Starter-Inc/shinkro-mapping/main/tvdb-mal.yaml` | YAML   |
| TMDB-MAL | `https://raw.githubusercontent.com/Starter-Inc/shinkro-mapping/main/tmdb-mal.yaml` | YAML   |

---

## TVDB-MAL Mapping

### Schema

```yaml
- malid: 21
  tvdbid: 81797
  tvdbseason: 1
  useMapping: true
  animeMapping:
    - start: 1
      mappingType: range
    - start: 517
      skipMalEpisodes: 1
      mappingType: range
    - start: 780
      explicitEpisodes:
        "3": 783
        "4": 784
      mappingType: explicit
```

### Top-Level Entry

| Field          | Type                 | Required | Description                                       |
| -------------- | -------------------- | -------- | ------------------------------------------------- |
| `malid`        | `int`                | Yes      | MyAnimeList anime ID                              |
| `tvdbid`       | `int`                | Yes      | TVDB series ID                                    |
| `tvdbseason`   | `int`                | No       | TVDB season number (default: `1`)                 |
| `useMapping`   | `bool`               | No       | If `true`, use `animeMapping` for episode mapping |
| `animeMapping` | `list[AnimeMapping]` | No       | Episode mapping segments                          |

### AnimeMapping Object

| Field              | Type             | Required | Description                                                                      |
| ------------------ | ---------------- | -------- | -------------------------------------------------------------------------------- |
| `start`            | `int`            | Yes      | Starting TVDB episode number for this segment                                    |
| `mappingType`      | `string`         | No       | `range` or `explicit` (default: `range`)                                         |
| `skipMalEpisodes`  | `int`            | No       | Number of MAL episodes already covered by previous segments (accumulated offset) |
| `explicitEpisodes` | `dict[str, int]` | No       | Explicit MAL→TVDB episode pairs (only when `mappingType: explicit`)              |

---

## TMDB-MAL Mapping

### Schema

```yaml
- malid: 36699
  tmdbid: 664399
- malid: 37451
  tmdbid: 784352
```

### Entry

| Field    | Type  | Required | Description          |
| -------- | ----- | -------- | -------------------- |
| `malid`  | `int` | Yes      | MyAnimeList anime ID |
| `tmdbid` | `int` | Yes      | TMDB movie ID        |
