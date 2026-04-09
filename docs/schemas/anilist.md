# AniList

## Endpoint

```
POST https://graphql.anilist.co
```

**Headers**:

- `Content-Type: application/json`
- `Accept: application/json`

No authentication required.

## Request

Uses batched GraphQL multi-alias queries. Each HTTP request contains up to 40 page aliases, each page requesting up to 50 IDs.

```graphql
query ($perPage: Int!, $ids_1: [Int!]!, $ids_2: [Int!]!, ...) {
  batch1: Page(page: 1, perPage: $perPage) {
    media(id_in: $ids_1, type: ANIME) {
      id
      episodes
      format
      seasonYear
      duration
      title {
        romaji
        english
        native
      }
      synonyms
    }
  }
  batch2: Page(page: 1, perPage: $perPage) {
    media(id_in: $ids_2, type: ANIME) { ... }
  }
  ...
}
```

**Variables**:

- `perPage`: always `50` (constant `BATCH_SIZE`)
- `ids_N`: array of integer AniList IDs per page

## Response

```json
{
  "data": {
    "batch1": {
      "media": [
        {
          "id": 1,
          "episodes": 26,
          "format": "TV",
          "seasonYear": 1998,
          "duration": 24,
          "title": {
            "romaji": "Cowboy Bebop",
            "english": "Cowboy Bebop",
            "native": "カウボーイビバップ"
          },
          "synonyms": ["카우보이 비밥", "Kowboj Bebop"]
        }
      ]
    }
  }
}
```

## Fields Used

| Field           | Type             | Description                                                         |
| --------------- | ---------------- | ------------------------------------------------------------------- |
| `id`            | `int`            | AniList media ID                                                    |
| `episodes`      | `int \| null`    | Total episode count; `null` if airing/unknown                       |
| `format`        | `string`         | One of: `TV`, `MOVIE`, `TV_SHORT`, `OVA`, `ONA`, `SPECIAL`, `MUSIC` |
| `seasonYear`    | `int \| null`    | Year the anime started airing                                       |
| `duration`      | `int \| null`    | Average episode duration in minutes                                 |
| `title.romaji`  | `string \| null` | Romanized title                                                     |
| `title.english` | `string \| null` | English title                                                       |
| `title.native`  | `string \| null` | Native language title                                               |
| `synonyms`      | `string[]`       | Alternative titles                                                  |

## Rate Limiting

HTTP 429 responses are handled by reading the `Retry-After` header and sleeping `Retry-After + 1` seconds.

## How Data Is Used

- `format` determines `SourceType`: `MOVIE` → `SourceType.MOVIE`, everything else → `SourceType.TV`
- `episodes` becomes the episode count metadata
- Titles are collected from `romaji`, `english`, `native`, and `synonyms`
- `seasonYear` is stored as start year metadata
- `duration` is stored as runtime in minutes
- Scope is always `None` (no per-season breakdown)
