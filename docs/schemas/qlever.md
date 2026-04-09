# QLever

## IMDb Endpoint

**Provider keys**: `imdb_movie`, `imdb_show`

### Endpoint

```
GET https://qlever.dev/api/imdb?query={sparql}&format=json
```

### SPARQL Query

```sparql
PREFIX imdb: <https://www.imdb.com/>

SELECT ?id ?type ?startYear ?runtimeMinutes ?episodeCount
       ?primaryTitle ?originalTitle
WHERE {
    VALUES ?id { "tt0092067" "tt0114746" ... }
    ?title imdb:id ?id ;
           imdb:type ?type .
    OPTIONAL { ?title imdb:startYear ?startYear . }
    OPTIONAL { ?title imdb:runtimeMinutes ?runtimeMinutes . }
    OPTIONAL { ?title imdb:primaryTitle ?primaryTitle . }
    OPTIONAL { ?title imdb:originalTitle ?originalTitle . }
    OPTIONAL {
        SELECT ?id (COUNT(?episode) AS ?episodeCount) WHERE {
            VALUES ?id { "tt0092067" "tt0114746" ... }
            ?title imdb:id ?id .
            ?episode imdb:parentTitle ?title .
        } GROUP BY ?id
    }
}
```

**Batching**: 200 IMDb IDs per request.

### Response

```json
{
  "head": {
    "vars": [
      "id",
      "type",
      "startYear",
      "runtimeMinutes",
      "episodeCount",
      "primaryTitle",
      "originalTitle"
    ]
  },
  "results": {
    "bindings": [
      {
        "id": { "type": "literal", "value": "tt0092067" },
        "type": { "type": "literal", "value": "movie" },
        "startYear": {
          "datatype": "http://www.w3.org/2001/XMLSchema#int",
          "type": "literal",
          "value": "1986"
        },
        "runtimeMinutes": {
          "datatype": "http://www.w3.org/2001/XMLSchema#int",
          "type": "literal",
          "value": "124"
        },
        "primaryTitle": { "type": "literal", "value": "Castle in the Sky" },
        "originalTitle": {
          "type": "literal",
          "value": "Tenkū no shiro Rapyuta"
        }
      }
    ]
  }
}
```

### Fields Used

| Field            | Type               | Description                           |
| ---------------- | ------------------ | ------------------------------------- |
| `id`             | `literal (string)` | IMDb ID in `ttNNNNNNN` format         |
| `type`           | `literal (string)` | IMDb title type (see below)           |
| `startYear`      | `literal (int)`    | Year of first release                 |
| `runtimeMinutes` | `literal (int)`    | Runtime in minutes                    |
| `episodeCount`   | `literal (int)`    | Number of child episodes (shows only) |
| `primaryTitle`   | `literal (string)` | Primary English title                 |
| `originalTitle`  | `literal (string)` | Original language title               |

### IMDb Title Types

**Movie types** (→ `SourceType.MOVIE`): `movie`, `tvMovie`, `short`, `video`, `tvSpecial`, `tvPilot`

**Show types** (→ `SourceType.TV`): `tvSeries`, `tvMiniSeries`, `tvShort`

### IMDb ID Normalization

Raw IDs are normalized to `tt{suffix}` format with the numeric portion zero-padded to 7 digits (up to 9 digits allowed).

---

## Wikidata Endpoint

### Endpoint

```
GET https://qlever.dev/api/wikidata?query={sparql}&format=json
```

### SPARQL Query

```sparql
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>

SELECT DISTINCT ?item ?prop ?id WHERE {
    ?item wdt:P31/wdt:P279* wd:Q20650540.

    VALUES ?prop {
        wdt:P5646   -- anidb
        wdt:P8729   -- anilist
        wdt:P345    -- imdb
        wdt:P4086   -- mal
        wdt:P4947   -- tmdb_movie
        wdt:P12196  -- tvdb_movie
    }
    ?item ?prop ?id.
}
LIMIT 500000
```

This is a single static query (no batching).

### Response

```json
{
  "head": { "vars": ["item", "prop", "id"] },
  "results": {
    "bindings": [
      {
        "item": {
          "type": "uri",
          "value": "http://www.wikidata.org/entity/Q100146660"
        },
        "prop": {
          "type": "uri",
          "value": "http://www.wikidata.org/prop/direct/P5646"
        },
        "id": { "type": "literal", "value": "1877" }
      }
    ]
  }
}
```

### Property Code to Provider Mapping

| Wikidata Property | Provider     | Notes                                |
| ----------------- | ------------ | ------------------------------------ |
| `P5646`           | `anidb`      |                                      |
| `P8729`           | `anilist`    |                                      |
| `P345`            | `imdb_movie` | Normalized via `normalize_imdb_id()` |
| `P4086`           | `mal`        |                                      |
| `P4947`           | `tmdb_movie` |                                      |
| `P12196`          | `tvdb_movie` |                                      |
