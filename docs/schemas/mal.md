# MAL

## Authentication

### OAuth Token Endpoint

```
POST https://myanimelist.net/v1/oauth2/token
```

**Form fields**:

- `grant_type`: always `refresh_token`
- `refresh_token`: from `MAL_API_KEY`
- `client_id`: from `MAL_CLIENT_ID`

**Environment variables**:

- `MAL_API_KEY`: required refresh token
- `MAL_CLIENT_ID`: MAL client ID; defaults to the public client ID baked into the source when unset

**Response**:

```json
{
  "token_type": "Bearer",
  "expires_in": 2678400,
  "access_token": "<access-token>",
  "refresh_token": "<refresh-token>"
}
```

The `access_token` is used as `Authorization: Bearer {token}` in subsequent ranking requests.

---

## Anime Ranking Endpoint

### Endpoint

```
GET https://api.myanimelist.net/v2/anime/ranking
```

**Headers**:

- `Accept: application/json`
- `Authorization: Bearer {access_token}`

### Query Parameters

| Parameter      | Value                                                                            | Description                                     |
| -------------- | -------------------------------------------------------------------------------- | ----------------------------------------------- |
| `ranking_type` | `all`                                                                            | Requests the full anime ranking listing         |
| `limit`        | `500`                                                                            | Maximum page size used by the source            |
| `offset`       | `0`, `500`, `1000`, ...                                                          | Pagination offset                               |
| `fields`       | `alternative_titles,start_date,media_type,num_episodes,average_episode_duration` | Additional metadata fields needed by the mapper |

### Response

```json
{
  "data": [
    {
      "node": {
        "id": 1,
        "title": "Cowboy Bebop",
        "alternative_titles": {
          "synonyms": ["קאובוי ביבופ"],
          "en": "Cowboy Bebop",
          "ja": "カウボーイビバップ"
        },
        "start_date": "1998-04-03",
        "media_type": "tv",
        "num_episodes": 26,
        "average_episode_duration": 1440
      }
    }
  ],
  "paging": {
    "next": "https://api.myanimelist.net/v2/anime/ranking?ranking_type=all&limit=500&offset=500"
  }
}
```

### Fields Used

| Field                              | Type             | Description                                               |
| ---------------------------------- | ---------------- | --------------------------------------------------------- |
| `node.id`                          | `int`            | MAL anime ID                                              |
| `node.title`                       | `string`         | Primary title                                             |
| `node.alternative_titles.synonyms` | `string[]`       | Alternate titles                                          |
| `node.alternative_titles.en`       | `string \| null` | English title                                             |
| `node.alternative_titles.ja`       | `string \| null` | Japanese title                                            |
| `node.start_date`                  | `string \| null` | Start date; year is extracted from the first 4 characters |
| `node.media_type`                  | `string`         | MAL media type                                            |
| `node.num_episodes`                | `int \| null`    | Episode count                                             |
| `node.average_episode_duration`    | `int \| null`    | Average runtime in seconds                                |
| `paging.next`                      | `string \| null` | Next page URL used to continue crawling                   |

## Pagination

The source crawls the ranking endpoint until `paging.next` is absent. The next page is determined by extracting `offset` from the returned `paging.next` URL.

## Rate Limiting

HTTP 429 responses are handled by reading the `Retry-After` header and sleeping `Retry-After + 1` seconds. This retry behavior is used for both token refresh and ranking requests.
