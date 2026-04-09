# Anime Lists

## Data Source

**URL**: `https://raw.githubusercontent.com/Anime-Lists/anime-lists/master/anime-list.xml`
**Format**: XML

## Schema

### Root Element

```xml
<?xml version="1.0" encoding="UTF-8"?>
<anime-list>
    <anime anidbid="1" tvdbid="76885" defaulttvdbseason="1"
           episodeoffset="0" tmdbid="" imdbid="">
        <name>Cowboy Bebop</name>
        <mapping-list>
            <mapping anidbseason="1" tvdbseason="0" start="1" end="1" offset="24">
                ;1-25;
            </mapping>
        </mapping-list>
    </anime>
</anime-list>
```

### `<anime>` Element Attributes

| Attribute           | Type     | Description                                                    |
| ------------------- | -------- | -------------------------------------------------------------- |
| `anidbid`           | `string` | AniDB anime ID                                                 |
| `tvdbid`            | `string` | TVDB series ID (may be `"unknown"`)                            |
| `defaulttvdbseason` | `string` | Default TVDB season number for this anime                      |
| `episodeoffset`     | `string` | Default episode offset applied to all episodes                 |
| `tmdbid`            | `string` | TMDB ID (optional, may be empty)                               |
| `imdbid`            | `string` | IMDb ID (optional, may be empty)                               |
| `tmdbseason`        | `string` | TMDB season number (optional; defaults to `defaulttvdbseason`) |
| `tmdboffset`        | `string` | TMDB episode offset (optional; defaults to `episodeoffset`)    |

### `<mapping>` Element

Specifies non-default episode mappings between AniDB and TVDB seasons.

#### Attributes

| Attribute     | Type     | Description                                         |
| ------------- | -------- | --------------------------------------------------- |
| `anidbseason` | `string` | AniDB season number (`1` = regular, `0` = specials) |
| `tvdbseason`  | `string` | TVDB season number                                  |
| `start`       | `string` | Start episode number (in AniDB numbering)           |
| `end`         | `string` | End episode number (inclusive)                      |
| `offset`      | `string` | Episode number offset                               |

#### Text Content

Explicit episode mappings in the format `;anidb_ep-tvdb_ep;`:

```
;1-25;2-26;3-27;
```

### Special Values

- `tvdbid="unknown"`: Entry is skipped
- `defaulttvdbseason="a"`: Absolute ordering mode, maps with scope `a` (absolute)
- Empty `tmdbid`/`imdbid`: No mapping available for that provider
