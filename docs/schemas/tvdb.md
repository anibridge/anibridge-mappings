# TVDB

## Authentication

### Login Endpoint

```
POST https://api4.thetvdb.com/v4/login
```

**Request body**:

```json
{ "apikey": "<TVDB_API_KEY>", "pin": "<TVDB_PIN>" }
```

- `TVDB_API_KEY` (required, from env var)
- `TVDB_PIN` (optional, from env var)

**Response**:

```json
{ "data": { "token": "eyJhbGciOi..." } }
```

The token is used as `Authorization: Bearer {token}` in all subsequent requests.

---

## TV Show Extended Details

### Endpoint

```
GET https://api4.thetvdb.com/v4/series/{id}/extended?meta=episodes&short=true
```

**Headers**:

- `Authorization: Bearer {token}`
- `Accept: application/json`

### Response (SeriesExtendedRecord)

```json
{
  "data": {
    "id": 78857,
    "name": "Naruto",
    "aliases": [
      { "language": "jpn", "name": "ナルト" },
      { "language": "kor", "name": "나루토" }
    ],
    "averageRuntime": 25,
    "firstAired": "2002-10-03",
    "lastAired": "2007-02-08",
    "episodes": [
      {
        "id": 127491,
        "seasonNumber": 1,
        "number": 1,
        "name": "Enter: Naruto Uzumaki!",
        "aired": "2002-10-03",
        "runtime": 25,
        "finaleType": null,
        "seriesId": 78857,
        "absoluteNumber": 1,
        "isMovie": 0
      }
    ],
    "year": "2002"
  }
}
```

### EpisodeBaseRecord Fields

| Field            | Type             | Description                                        |
| ---------------- | ---------------- | -------------------------------------------------- |
| `seasonNumber`   | `int`            | Season number the episode belongs to               |
| `number`         | `int`            | Episode number within the season                   |
| `aired`          | `string \| null` | Air date (YYYY-MM-DD)                              |
| `finaleType`     | `string \| null` | `"season"`, `"midseason"`, or `"series"` if finale |
| `runtime`        | `int \| null`    | Episode runtime in minutes                         |
| `absoluteNumber` | `int \| null`    | Absolute episode number                            |
| `isMovie`        | `int`            | 0 for regular episode, 1 for movie                 |

### SeriesExtendedRecord Fields Used

| Field            | Type          | Description                                   |
| ---------------- | ------------- | --------------------------------------------- |
| `name`           | `string`      | Show title                                    |
| `aliases`        | `array`       | Array of `{language, name}` objects           |
| `averageRuntime` | `int \| null` | Average episode runtime in minutes            |
| `episodes`       | `array`       | All episodes with `meta=episodes` query param |
| `year`           | `string`      | Year the series started                       |

---

## Movie Extended Details

### Endpoint

```
GET https://api4.thetvdb.com/v4/movies/{id}/extended
```

**Headers**: same as TV shows.

### Response (MovieExtendedRecord)

```json
{
  "data": {
    "id": 12345,
    "name": "My Movie",
    "aliases": [{ "language": "jpn", "name": "マイムービー" }],
    "runtime": 120,
    "year": "2020",
    "first_release": { "date": "2020-05-15", "country": "JP" }
  }
}
```

### Fields Used

| Field     | Type          | Description                         |
| --------- | ------------- | ----------------------------------- |
| `name`    | `string`      | Movie title                         |
| `aliases` | `array`       | Array of `{language, name}` objects |
| `runtime` | `int \| null` | Runtime in minutes                  |
| `year`    | `string`      | Release year                        |
