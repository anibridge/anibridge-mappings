# TMDB

## Authentication

- **Environment variable**: `TMDB_API_KEY`
- **Header**: `Authorization: Bearer {TMDB_API_KEY}`
- **Accept**: `application/json`

---

## TV Show Details

### Endpoint

```
GET https://api.themoviedb.org/3/tv/{id}
```

### Response

```json
{
  "name": "Game of Thrones",
  "original_name": "Game of Thrones",
  "seasons": [
    {
      "air_date": "2010-12-05",
      "episode_count": 64,
      "id": 3627,
      "name": "Specials",
      "overview": "...",
      "poster_path": "/...",
      "season_number": 0,
      "vote_average": 0.0
    },
    {
      "air_date": "2011-04-17",
      "episode_count": 10,
      "id": 3624,
      "name": "Season 1",
      "overview": "...",
      "poster_path": "/...",
      "season_number": 1,
      "vote_average": 8.3
    }
  ],
  "number_of_episodes": 73,
  "number_of_seasons": 8,
  "first_air_date": "2011-04-17",
  "status": "Ended",
  "type": "Scripted",
  "episode_run_time": [60],
  "original_language": "en",
  "id": 1399
}
```

### Fields Used

| Field                     | Type             | Description                      |
| ------------------------- | ---------------- | -------------------------------- |
| `name`                    | `string`         | Show title                       |
| `original_name`           | `string`         | Original language title          |
| `seasons`                 | `array`          | Array of season objects          |
| `seasons[].season_number` | `int`            | Season number (0 = specials)     |
| `seasons[].episode_count` | `int`            | Number of episodes in the season |
| `seasons[].air_date`      | `string \| null` | First air date (YYYY-MM-DD)      |

---

## Movie Details

### Endpoint

```
GET https://api.themoviedb.org/3/movie/{id}
```

### Response

```json
{
  "id": 11,
  "title": "Star Wars",
  "original_title": "Star Wars",
  "runtime": 121,
  "release_date": "1977-05-25",
  "budget": 11000000,
  "revenue": 775398007,
  "status": "Released",
  "adult": false,
  "imdb_id": "tt0076759"
}
```

### Fields Used

| Field            | Type          | Description               |
| ---------------- | ------------- | ------------------------- |
| `title`          | `string`      | Movie title               |
| `original_title` | `string`      | Original language title   |
| `runtime`        | `int \| null` | Runtime in minutes        |
| `release_date`   | `string`      | Release date (YYYY-MM-DD) |
