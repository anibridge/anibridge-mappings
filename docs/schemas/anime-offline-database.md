# Anime Offline Database

## Data Source

**URL**: `https://github.com/manami-project/anime-offline-database/releases/latest/download/anime-offline-database-minified.json.zst`
**Format**: Zstandard-compressed JSON

## Schema

### Top-Level

```json
{
  "data": [
    { ... },
    { ... }
  ]
}
```

The `data` array contains ~40,000+ anime entries.

### Entry Object

```json
{
  "sources": [
    "https://anidb.net/anime/4563",
    "https://anilist.co/anime/1",
    "https://anime-planet.com/anime/cowboy-bebop",
    "https://anisearch.com/anime/22",
    "https://kitsu.app/anime/1",
    "https://livechart.me/anime/3418",
    "https://myanimelist.net/anime/1",
    "https://notify.moe/anime/T4HBppKig",
    "https://shikimori.one/animes/1"
  ],
  "title": "Cowboy Bebop",
  "type": "TV",
  "episodes": 26,
  "status": "FINISHED",
  "animeSeason": { "season": "SPRING", "year": 1998 },
  "picture": "https://cdn.myanimelist.net/images/anime/4/19644.jpg",
  "thumbnail": "https://cdn.myanimelist.net/images/anime/4/19644t.jpg",
  "duration": { "value": 1440, "unit": "SECONDS" },
  "synonyms": ["カウボーイビバップ", "Cowboy Bebop"],
  "relatedAnime": ["https://anidb.net/anime/5"],
  "tags": ["action", "adventure"]
}
```

### Fields

| Field          | Type           | Description                                                   |
| -------------- | -------------- | ------------------------------------------------------------- |
| `sources`      | `list[string]` | URLs to the anime on various provider sites                   |
| `title`        | `string`       | Primary title                                                 |
| `type`         | `string`       | Media type: `TV`, `MOVIE`, `ONA`, `OVA`, `SPECIAL`, `UNKNOWN` |
| `episodes`     | `int`          | Number of episodes                                            |
| `status`       | `string`       | Airing status: `FINISHED`, `ONGOING`, `UPCOMING`, `UNKNOWN`   |
| `animeSeason`  | `AnimeSeason`  | Season and year info                                          |
| `picture`      | `string`       | URL to main picture                                           |
| `thumbnail`    | `string`       | URL to thumbnail                                              |
| `duration`     | `Duration`     | Episode duration                                              |
| `synonyms`     | `list[string]` | Alternative titles                                            |
| `relatedAnime` | `list[string]` | URLs to related anime entries                                 |
| `tags`         | `list[string]` | Genre/tag list                                                |

### AnimeSeason Object

| Field    | Type     | Description                                       |
| -------- | -------- | ------------------------------------------------- |
| `season` | `string` | `SPRING`, `SUMMER`, `FALL`, `WINTER`, `UNDEFINED` |
| `year`   | `int`    | Year of airing                                    |

### Duration Object

| Field   | Type     | Description              |
| ------- | -------- | ------------------------ |
| `value` | `int`    | Duration value           |
| `unit`  | `string` | Duration unit: `SECONDS` |

### Source URL Patterns

IDs are extracted from `sources` URLs using regex:

| URL Pattern                             | Provider       |
| --------------------------------------- | -------------- |
| `https://anidb.net/anime/{id}`          | `anidb`        |
| `https://anilist.co/anime/{id}`         | `anilist`      |
| `https://myanimelist.net/anime/{id}`    | `mal`          |
| `https://anime-planet.com/anime/{slug}` | `anime-planet` |
| `https://kitsu.app/anime/{id}`          | `kitsu`        |
