# Anime Aggregations

## Data Source

**Repository**: `https://github.com/Ziedelth/anime-aggregations.git`
**Format**: Zstandard-compressed JSON files in `data/` directory (one per AniDB ID). Each file is named `{anidb_id}.json.zst`.

## Schema

### Top-Level Entry

```json
{
  "anime_id": 1,
  "anime_url": "https://anidb.net/anime/1",
  "episodes": [
    {
      "number": 1,
      "type": "REGULAR",
      "duration": 1520,
      "air_date": "1999-01-09"
    }
  ],
  "resources": [
    { "type": "MAL", "ids": [1] },
    { "type": "ANILIST", "ids": [1] }
  ]
}
```

### Fields

| Field       | Type             | Description                         |
| ----------- | ---------------- | ----------------------------------- |
| `anime_id`  | `int`            | AniDB anime ID                      |
| `anime_url` | `string`         | URL to AniDB page                   |
| `episodes`  | `list[Episode]`  | Episode list                        |
| `resources` | `list[Resource]` | Cross-references to other providers |

### Episode Object

| Field      | Type             | Description                          |
| ---------- | ---------------- | ------------------------------------ |
| `number`   | `int`            | Episode number within its type       |
| `type`     | `string`         | Episode type: `REGULAR` or `SPECIAL` |
| `duration` | `int`            | Duration in seconds                  |
| `air_date` | `string \| null` | Air date in `YYYY-MM-DD` format      |

### Resource Object

| Field  | Type        | Description                     |
| ------ | ----------- | ------------------------------- |
| `type` | `string`    | Provider identifier (see below) |
| `ids`  | `list[int]` | List of provider IDs            |

### Resource Types Used

| Type      | Provider Key                                                        |
| --------- | ------------------------------------------------------------------- |
| `MAL`     | `mal`                                                               |
| `ANILIST` | `anilist`                                                           |
| `IMDB`    | `imdb_show` or `imdb_movie` (based on episode count)                |
| `TMDB`    | `tmdb_show` or `tmdb_movie` (based on episode count, uses first ID) |
| `TVDB`    | `tvdb_show` (uses first ID)                                         |

### Scope Mapping

Episode types map to scope identifiers:

- `REGULAR` → `R` (regular episodes)
- `SPECIAL` → `S` (special episodes)
